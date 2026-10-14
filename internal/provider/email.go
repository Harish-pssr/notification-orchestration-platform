package provider

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/events"
)

// Real email sending (D21). Off unless the SMTP account is configured AND an operator switches it on, and it
// starts off again after every restart. Every email goes to one fixed recipient.
//
// A real email cannot be sent inside a database transaction, so the provider uses a small state machine
// instead of the mock's single transaction:
//
//	1. claim the key as IN_PROGRESS (a unique insert: only one request owns it)
//	2. send the email
//	3. in one transaction, record the effect and mark the key DONE
//
// A crash between 2 and 3 leaves the key IN_PROGRESS with the email already sent. That window cannot be closed
// (it is the "called an external system, then failed to record it" problem): after StuckAfter the next retry
// sends again, so the guarantee for real email is "at least once, with a rare duplicate in that window".

// EmailConfig is the SMTP account and the safety limits. It comes from the environment; the credentials are
// never stored in the database and never shown in the dashboard.
type EmailConfig struct {
	Host, Port, User, Pass string
	From                   string        // sender address; defaults to User
	InitialTo              string        // recipient used until the operator saves one in the dashboard
	MaxPerHour             int           // hard cap on real emails per hour (the safety net)
	StuckAfter             time.Duration // an IN_PROGRESS key older than this is resent
	Timeout                time.Duration // one SMTP conversation
}

// EmailConfigFromEnv reads EMAIL_SMTP_HOST, EMAIL_SMTP_PORT (587), EMAIL_SMTP_USER, EMAIL_SMTP_PASS, EMAIL_FROM,
// EMAIL_TO, EMAIL_MAX_PER_HOUR (20), EMAIL_STUCK_AFTER (60s) and EMAIL_TIMEOUT (15s).
func EmailConfigFromEnv() EmailConfig {
	c := EmailConfig{
		Host:       strings.TrimSpace(config.Getenv("EMAIL_SMTP_HOST", "")),
		Port:       strings.TrimSpace(config.Getenv("EMAIL_SMTP_PORT", "587")),
		User:       strings.TrimSpace(config.Getenv("EMAIL_SMTP_USER", "")),
		Pass:       config.Getenv("EMAIL_SMTP_PASS", ""),
		From:       strings.TrimSpace(config.Getenv("EMAIL_FROM", "")),
		InitialTo:  strings.TrimSpace(config.Getenv("EMAIL_TO", "")),
		MaxPerHour: config.GetenvInt("EMAIL_MAX_PER_HOUR", 20),
		StuckAfter: config.GetenvDuration("EMAIL_STUCK_AFTER", 60*time.Second),
		Timeout:    config.GetenvDuration("EMAIL_TIMEOUT", 15*time.Second),
	}
	if c.From == "" {
		c.From = c.User
	}
	return c
}

// Configured reports whether there is an SMTP account to send through.
func (c EmailConfig) Configured() bool {
	return c.Host != "" && c.User != "" && c.Pass != "" && c.From != ""
}

// Mail is one message to send.
type Mail struct {
	From, To, Subject, Body string
	MessageID               string
}

// Sender sends one email and returns the server's acceptance reply (for example "250 2.0.0 OK ...").
type Sender interface {
	Send(ctx context.Context, m Mail) (reply string, err error)
}

// SendError is a failed send. Uncertain means the server may nevertheless have accepted the message (the
// connection failed after the message was handed over), so retrying could send it twice.
type SendError struct {
	Err       error
	Uncertain bool
}

func (e *SendError) Error() string { return e.Err.Error() }
func (e *SendError) Unwrap() error { return e.Err }

// statusError is an answer the send handler returns as an HTTP status (so the consumer's retry rules apply):
// 503 and 429 are retried with backoff, 502 too.
type statusError struct {
	code       int
	retryAfter int // seconds, 0 for none
	msg        string
}

func (e *statusError) Error() string { return e.msg }

// ---------- SMTP ----------

type smtpSender struct{ cfg EmailConfig }

// NewSMTPSender returns a Sender that talks SMTP to cfg.Host. Port 465 uses implicit TLS; any other port uses
// STARTTLS when the server offers it.
func NewSMTPSender(cfg EmailConfig) Sender { return &smtpSender{cfg: cfg} }

func (s *smtpSender) Send(ctx context.Context, m Mail) (string, error) {
	addr := net.JoinHostPort(s.cfg.Host, s.cfg.Port)
	dialer := net.Dialer{Timeout: s.cfg.Timeout}
	var conn net.Conn
	var err error
	if s.cfg.Port == "465" {
		conn, err = tls.DialWithDialer(&dialer, "tcp", addr, &tls.Config{ServerName: s.cfg.Host})
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return "", &SendError{Err: fmt.Errorf("connect to %s: %w", addr, err)}
	}
	_ = conn.SetDeadline(time.Now().Add(s.cfg.Timeout))
	c, err := smtp.NewClient(conn, s.cfg.Host)
	if err != nil {
		_ = conn.Close()
		return "", &SendError{Err: fmt.Errorf("smtp greeting: %w", err)}
	}
	defer c.Close()

	if ok, _ := c.Extension("STARTTLS"); ok && s.cfg.Port != "465" {
		if err := c.StartTLS(&tls.Config{ServerName: s.cfg.Host}); err != nil {
			return "", &SendError{Err: fmt.Errorf("starttls: %w", err)}
		}
	}
	if ok, _ := c.Extension("AUTH"); ok && s.cfg.User != "" {
		if err := c.Auth(smtp.PlainAuth("", s.cfg.User, s.cfg.Pass, s.cfg.Host)); err != nil {
			return "", &SendError{Err: fmt.Errorf("smtp login: %w", err)}
		}
	}
	if err := c.Mail(addressOnly(m.From)); err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp MAIL FROM: %w", err)}
	}
	if err := c.Rcpt(addressOnly(m.To)); err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp RCPT TO: %w", err)}
	}
	reply, err := dataWithReply(c, composeMessage(m))
	if err != nil {
		return "", err
	}
	_ = c.Quit()
	return reply, nil
}

// dataWithReply sends the message body and returns the server's reply to it. net/smtp's own Data() discards
// that reply, and we want to show it (it usually carries the server's queue id).
func dataWithReply(c *smtp.Client, msg []byte) (string, error) {
	id, err := c.Text.Cmd("DATA")
	if err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp DATA: %w", err)}
	}
	c.Text.StartResponse(id)
	_, _, err = c.Text.ReadResponse(354)
	c.Text.EndResponse(id)
	if err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp DATA: %w", err)}
	}
	w := c.Text.DotWriter()
	if _, err := w.Write(msg); err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp message: %w", err)} // the terminator was never sent: not accepted
	}
	if err := w.Close(); err != nil {
		return "", &SendError{Err: fmt.Errorf("smtp message end: %w", err), Uncertain: true}
	}
	code, text, err := c.Text.ReadResponse(250)
	if err != nil {
		var te *textproto.Error
		// An explicit refusal from the server means the message was not accepted. Anything else (a timeout, a
		// dropped connection) means we do not know.
		return "", &SendError{Err: fmt.Errorf("smtp reply to the message: %w", err), Uncertain: !errors.As(err, &te)}
	}
	return fmt.Sprintf("%d %s", code, text), nil
}

func addressOnly(s string) string {
	if a, err := mail.ParseAddress(s); err == nil {
		return a.Address
	}
	return s
}

// composeMessage builds a plain-text RFC 5322 message. Header values must not contain line breaks.
func composeMessage(m Mail) []byte {
	clean := func(s string) string { return strings.NewReplacer("\r", " ", "\n", " ").Replace(s) }
	var b strings.Builder
	b.WriteString("From: " + clean(m.From) + "\r\n")
	b.WriteString("To: " + clean(m.To) + "\r\n")
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", clean(m.Subject)) + "\r\n")
	b.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")
	b.WriteString("Message-ID: " + clean(m.MessageID) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	b.WriteString("\r\n")
	b.WriteString(strings.ReplaceAll(strings.ReplaceAll(m.Body, "\r\n", "\n"), "\n", "\r\n"))
	b.WriteString("\r\n")
	return []byte(b.String())
}

// newMessageID returns a Message-ID that is unique to this send ATTEMPT, not to the notification: some mailbox
// providers hide a second message that repeats a Message-ID, which would make a real duplicate invisible.
func newMessageID(from string) string {
	domain := "localhost"
	if i := strings.LastIndex(addressOnly(from), "@"); i >= 0 {
		domain = addressOnly(from)[i+1:]
	}
	return "<" + uuid.NewString() + "@" + domain + ">"
}

// buildEmail turns the consumer's request body into a readable subject and text, so each email in an inbox can
// be matched to an event.
func buildEmail(key string, body []byte) (subject, text string) {
	var in struct {
		EventID   string         `json:"event_id"`
		EventType string         `json:"event_type"`
		Payload   map[string]any `json:"payload"`
	}
	_ = json.Unmarshal(body, &in)
	short := in.EventID
	if len(short) > 8 {
		short = short[len(short)-8:]
	}
	var what string
	switch events.Type(in.EventType) {
	case events.PaymentSucceeded:
		what = "Payment succeeded"
		if cents, ok := in.Payload["amount_cents"].(float64); ok {
			cur, _ := in.Payload["currency"].(string)
			what += fmt.Sprintf(": %.2f %s", cents/100, cur)
		}
	case events.BookingConfirmed:
		what = "Booking confirmed"
		if item, ok := in.Payload["item"].(string); ok {
			what += ": " + item
		}
	case events.PasswordChanged:
		what = "Password changed"
		if u, ok := in.Payload["user_id"].(string); ok {
			what += " for " + u
		}
	default:
		what = "Notification"
	}
	subject = fmt.Sprintf("%s (event ...%s)", what, short)
	pretty, _ := json.MarshalIndent(in.Payload, "", "  ")
	text = what + "\n\nEvent: " + in.EventID + " (" + in.EventType + ")\nIdempotency key: " + key +
		"\n\nDetails:\n" + string(pretty) +
		"\n\nSent by the Notification Platform demo. The provider sends once per idempotency key, so this email should reach you once however many times the broker delivers the message."
	return subject, text
}

// ---------- safety cap ----------

// hourlyCap allows at most max sends in any rolling hour.
type hourlyCap struct {
	mu    sync.Mutex
	max   int
	times []time.Time
}

func (h *hourlyCap) prune(now time.Time) {
	cut := now.Add(-time.Hour)
	i := 0
	for i < len(h.times) && h.times[i].Before(cut) {
		i++
	}
	h.times = h.times[i:]
}

// Allow records a send and reports whether it is within the cap.
func (h *hourlyCap) Allow() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	h.prune(now)
	if len(h.times) >= h.max {
		return false
	}
	h.times = append(h.times, now)
	return true
}

// Refund gives back the slot of a send that definitely did not happen, so failing attempts (a wrong password, an
// unreachable server) cannot use up the cap without sending a single email.
func (h *hourlyCap) Refund() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if n := len(h.times); n > 0 {
		h.times = h.times[:n-1]
	}
}

// Used is how many sends were recorded in the last hour.
func (h *hourlyCap) Used() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.prune(time.Now())
	return len(h.times)
}

// ---------- the emailer ----------

type emailer struct {
	cfg       EmailConfig
	sender    Sender
	pool      *pgxpool.Pool
	log       *slog.Logger
	real      atomic.Bool // the operator's switch; always starts off
	recipient atomic.Value
	cap       hourlyCap
	stuck     atomic.Int64 // sends repeated because an earlier attempt never finished
	afterSend func() error // tests only: simulate a crash between the send and the DONE write
}

// SetEmail configures real email sending. A nil sender means "SMTP with cfg" (used only when cfg is
// complete). The real-send switch starts off.
func (p *Provider) SetEmail(ctx context.Context, cfg EmailConfig, sender Sender) {
	if cfg.MaxPerHour <= 0 {
		cfg.MaxPerHour = 20
	}
	if cfg.StuckAfter <= 0 {
		cfg.StuckAfter = 60 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if sender == nil && cfg.Configured() {
		sender = NewSMTPSender(cfg)
	}
	e := &emailer{cfg: cfg, sender: sender, pool: p.pool, log: p.log}
	e.cap.max = cfg.MaxPerHour
	e.recipient.Store(cfg.InitialTo)
	var saved string
	if err := p.pool.QueryRow(ctx, `SELECT value FROM provider_settings WHERE key = 'email_to'`).Scan(&saved); err == nil && saved != "" {
		e.recipient.Store(saved)
	}
	p.email = e
}

// SetEmailAfterSend installs a test hook that runs right after a real send and before it is recorded.
func (p *Provider) SetEmailAfterSend(fn func() error) {
	if p.email != nil {
		p.email.afterSend = fn
	}
}

func (e *emailer) to() string {
	s, _ := e.recipient.Load().(string)
	return s
}

// active reports whether the email channel currently sends real emails.
func (e *emailer) active() bool {
	return e != nil && e.sender != nil && e.real.Load() && e.to() != ""
}

func maskAddress(a string) string {
	i := strings.LastIndex(a, "@")
	if i < 1 {
		return ""
	}
	return a[:1] + "***" + a[i:]
}

// emailStatus is what the dashboard shows. It never includes the credentials or the full recipient.
type emailStatus struct {
	Configured     bool   `json:"configured"`
	Real           bool   `json:"real"`
	Recipient      string `json:"recipient"` // masked
	HasRecipient   bool   `json:"has_recipient"`
	Host           string `json:"host,omitempty"`
	SentThisHour   int    `json:"sent_this_hour"`
	MaxPerHour     int    `json:"max_per_hour"`
	StuckResends   int64  `json:"stuck_resends"`
	StuckAfterSecs int    `json:"stuck_after_s"`
}

func (e *emailer) status() emailStatus {
	if e == nil {
		return emailStatus{}
	}
	return emailStatus{
		Configured: e.sender != nil, Real: e.active(), Recipient: maskAddress(e.to()),
		HasRecipient: e.to() != "", Host: e.cfg.Host, SentThisHour: e.cap.Used(), MaxPerHour: e.cfg.MaxPerHour,
		StuckResends: e.stuck.Load(), StuckAfterSecs: int(e.cfg.StuckAfter.Seconds()),
	}
}

// claim takes ownership of a key for a real send. It reports whether the caller owns it; when it does not and
// there is no error, the stored response of the finished earlier attempt is returned.
func (e *emailer) claim(ctx context.Context, key string, ch events.Channel) (owned bool, stored []byte, err error) {
	for attempt := 0; attempt < 3; attempt++ {
		tag, err := e.pool.Exec(ctx,
			`INSERT INTO idempotency_keys (key, channel, response, state, started_at) VALUES ($1, $2, '{}', 'IN_PROGRESS', now()) ON CONFLICT (key) DO NOTHING`,
			key, string(ch))
		if err != nil {
			return false, nil, err
		}
		if tag.RowsAffected() == 1 {
			return true, nil, nil
		}
		var state string
		var response []byte
		var age float64
		err = e.pool.QueryRow(ctx,
			`SELECT state, response, extract(epoch FROM (now() - started_at)) FROM idempotency_keys WHERE key = $1`, key).Scan(&state, &response, &age)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // released between the two statements: try to claim it again
		}
		if err != nil {
			return false, nil, err
		}
		if state == "DONE" {
			return false, response, nil
		}
		if age >= e.cfg.StuckAfter.Seconds() {
			// The earlier attempt never finished, and we cannot know whether it sent the email. Take it over
			// (only one request wins this update) and send again: at least once.
			tag, err := e.pool.Exec(ctx,
				`UPDATE idempotency_keys SET started_at = now() WHERE key = $1 AND state = 'IN_PROGRESS' AND started_at <= now() - make_interval(secs => $2)`,
				key, e.cfg.StuckAfter.Seconds())
			if err != nil {
				return false, nil, err
			}
			if tag.RowsAffected() == 1 {
				e.stuck.Add(1)
				e.log.Warn("sending again: an earlier attempt for this key never finished, so the email may be a duplicate", "key", key, "status", "email_resend_after_stuck")
				return true, nil, nil
			}
		}
		return false, nil, &statusError{code: 503, retryAfter: 2, msg: "an earlier attempt for this idempotency key is still in progress"}
	}
	return false, nil, &statusError{code: 503, retryAfter: 1, msg: "could not claim the idempotency key"}
}

// release gives a claimed key back after a send that definitely did not happen.
func (e *emailer) release(ctx context.Context, key string) {
	if _, err := e.pool.Exec(context.WithoutCancel(ctx), `DELETE FROM idempotency_keys WHERE key = $1 AND state = 'IN_PROGRESS'`, key); err != nil {
		e.log.Warn("could not release the idempotency key", "key", key, "error", err)
	}
}

// executeEmail is the real-send path (see the state machine at the top of this file).
func (p *Provider) executeEmail(reqCtx context.Context, key string, ch events.Channel, body []byte) (resp []byte, replayed bool, err error) {
	if err := reqCtx.Err(); err != nil {
		return nil, false, err // the caller is already gone: do not start a send nobody is waiting for
	}
	// From here the work finishes even if the caller gives up. A real send takes seconds, so the consumer's call
	// timeout can fire while the email is already on its way; if the record after the send were cancelled with
	// the request, the key would stay IN_PROGRESS and the email would be sent again after the stuck timeout, which
	// is a duplicate with no crash at all. The SMTP timeout bounds how long this can run.
	ctx := context.WithoutCancel(reqCtx)
	e := p.email
	dedupe := p.dedupe.Load()
	if dedupe {
		owned, stored, err := e.claim(ctx, key, ch)
		if err != nil {
			return nil, false, err
		}
		if !owned {
			return stored, true, nil
		}
	}
	if !e.cap.Allow() {
		if dedupe {
			e.release(ctx, key)
		}
		return nil, false, &statusError{code: 429, retryAfter: 30, msg: fmt.Sprintf("the hourly limit of %d real emails was reached", e.cfg.MaxPerHour)}
	}

	subject, text := buildEmail(key, body)
	sendStart := time.Now()
	reply, sendErr := e.sender.Send(ctx, Mail{From: e.cfg.From, To: e.to(), Subject: subject, Body: text, MessageID: newMessageID(e.cfg.From)})
	if sendErr != nil {
		var se *SendError
		uncertain := errors.As(sendErr, &se) && se.Uncertain
		if !uncertain {
			e.cap.Refund()
		}
		if uncertain {
			// Leave the key IN_PROGRESS: the email may have been accepted, so a retry must wait for the stuck
			// timeout before sending again.
			e.log.Warn("email send outcome unknown", "key", key, "error", sendErr, "status", "email_send_uncertain")
		} else if dedupe {
			e.release(ctx, key)
		}
		return nil, false, &statusError{code: 502, retryAfter: 1, msg: "email send failed: " + sendErr.Error()}
	}
	e.log.Info("email accepted by the mail server", "key", key, "reply", reply, "took_ms", time.Since(sendStart).Milliseconds(), "status", "email_sent")

	if e.afterSend != nil {
		if err := e.afterSend(); err != nil {
			return nil, false, err
		}
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var effectID int64
	if err := tx.QueryRow(ctx, `INSERT INTO effects (key, channel, payload) VALUES ($1, $2, $3) RETURNING id`, key, string(ch), body).Scan(&effectID); err != nil {
		return nil, false, err
	}
	resp, _ = json.Marshal(map[string]any{"status": "sent", "channel": ch, "effect_id": effectID, "transport": "smtp", "accepted": reply})
	if dedupe {
		if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET state = 'DONE', response = $2 WHERE key = $1`, key, resp); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return resp, false, nil
}

// ---------- operator endpoints ----------

func writeEmailJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeEmailError(w http.ResponseWriter, code int, msg string) {
	writeEmailJSON(w, code, map[string]string{"error": msg})
}

const notConfiguredMsg = "SMTP is not configured on the server (set EMAIL_SMTP_HOST, EMAIL_SMTP_USER and EMAIL_SMTP_PASS)"

type emailSettings struct {
	Real      *bool   `json:"real"`
	Recipient *string `json:"recipient"`
}

// emailInfo handles GET /email: the public, read-only status (the recipient is masked).
func (p *Provider) emailInfo(w http.ResponseWriter, r *http.Request) {
	writeEmailJSON(w, http.StatusOK, p.email.status())
}

// setEmail handles POST /admin/email: save the recipient and/or switch real sending on or off.
func (p *Provider) setEmail(w http.ResponseWriter, r *http.Request) {
	e := p.email
	var in emailSettings
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in); err != nil {
		writeEmailError(w, http.StatusBadRequest, `body must be {"real": true|false, "recipient": "address"}`)
		return
	}
	if e == nil || e.sender == nil {
		writeEmailError(w, http.StatusBadRequest, notConfiguredMsg)
		return
	}
	if in.Recipient != nil {
		addr, err := mail.ParseAddress(strings.TrimSpace(*in.Recipient))
		if err != nil {
			writeEmailError(w, http.StatusBadRequest, "recipient is not a valid email address")
			return
		}
		if _, err := p.pool.Exec(r.Context(), `INSERT INTO provider_settings (key, value) VALUES ('email_to', $1) ON CONFLICT (key) DO UPDATE SET value = $1`, addr.Address); err != nil {
			writeEmailError(w, http.StatusInternalServerError, "could not save the recipient")
			return
		}
		e.recipient.Store(addr.Address)
	}
	if in.Real != nil {
		if *in.Real && e.to() == "" {
			writeEmailError(w, http.StatusBadRequest, "set a recipient before switching real sending on")
			return
		}
		e.real.Store(*in.Real)
		p.log.Warn("real email sending switched", "enabled", *in.Real, "status", "email_real_switched")
	}
	writeEmailJSON(w, http.StatusOK, e.status())
}

// sendTestEmail handles POST /admin/email/test: one real email, to check the SMTP settings work.
func (p *Provider) sendTestEmail(w http.ResponseWriter, r *http.Request) {
	e := p.email
	if e == nil || e.sender == nil {
		writeEmailError(w, http.StatusBadRequest, notConfiguredMsg)
		return
	}
	if e.to() == "" {
		writeEmailError(w, http.StatusBadRequest, "set a recipient first")
		return
	}
	if !e.cap.Allow() {
		writeEmailError(w, http.StatusTooManyRequests, fmt.Sprintf("the hourly limit of %d real emails was reached", e.cfg.MaxPerHour))
		return
	}
	// A real send takes seconds; if the caller gives up, the send still finishes (the SMTP timeout bounds it).
	reply, err := e.sender.Send(context.WithoutCancel(r.Context()), Mail{
		From: e.cfg.From, To: e.to(), MessageID: newMessageID(e.cfg.From),
		Subject: "Test email from the Notification Platform (" + strconv.Itoa(int(time.Now().Unix()%100000)) + ")",
		Body:    "If you can read this, the SMTP settings work. Nothing else was sent.",
	})
	if err != nil {
		var se *SendError
		if !errors.As(err, &se) || !se.Uncertain {
			e.cap.Refund()
		}
		writeEmailError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeEmailJSON(w, http.StatusOK, map[string]any{"accepted": true, "reply": reply})
}
