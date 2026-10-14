package test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/db"
	"notifplatform/internal/provider"
	"notifplatform/migrations"
)

// ---------- helpers ----------

// isolatedProviderPool creates a throwaway provider database on the dev Postgres, migrates it and drops it
// when the test ends, so these tests never touch the running provider's data.
func isolatedProviderPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(config.Load().ProviderDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	adminCfg := cfg.Copy()
	adminCfg.ConnConfig.Database = "postgres"
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatalf("create database: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	if _, err := db.Migrate(ctx, pool, migrations.FS, "provider"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	return pool
}

// fakeSender records every email instead of sending it.
type fakeSender struct {
	mu    sync.Mutex
	sent  []provider.Mail
	err   error
	delay time.Duration
}

func (f *fakeSender) Send(_ context.Context, m provider.Mail) (string, error) {
	time.Sleep(f.delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	f.sent = append(f.sent, m)
	return "250 2.0.0 OK fake", nil
}

func (f *fakeSender) count() int               { f.mu.Lock(); defer f.mu.Unlock(); return len(f.sent) }
func (f *fakeSender) setErr(err error)         { f.mu.Lock(); f.err = err; f.mu.Unlock() }
func (f *fakeSender) mail(i int) provider.Mail { f.mu.Lock(); defer f.mu.Unlock(); return f.sent[i] }

func testEmailConfig() provider.EmailConfig {
	return provider.EmailConfig{Host: "smtp.test", Port: "587", User: "u", Pass: "p", From: "from@example.com",
		MaxPerHour: 20, StuckAfter: 300 * time.Millisecond, Timeout: 2 * time.Second}
}

type emailEnv struct {
	p    *provider.Provider
	srv  *httptest.Server
	pool *pgxpool.Pool
	snd  *fakeSender
}

// newEmailProvider builds a provider on its own database with real email sending through snd. When real is true
// the switch is turned on with a recipient already saved.
func newEmailProvider(t *testing.T, cfg provider.EmailConfig, dedupe, real bool) *emailEnv {
	t.Helper()
	pool := isolatedProviderPool(t)
	snd := &fakeSender{}
	p := provider.New(pool, dedupe, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.SetEmail(context.Background(), cfg, snd)
	p.SetAdmin(true, "tok")
	srv := httptest.NewServer(p.Handler())
	t.Cleanup(srv.Close)
	env := &emailEnv{p: p, srv: srv, pool: pool, snd: snd}
	if real {
		if code, body := env.admin(t, "/admin/email", map[string]any{"recipient": "me@example.com", "real": true}); code != 200 {
			t.Fatalf("switch real sending on: %d %s", code, body)
		}
	}
	return env
}

func (e *emailEnv) admin(t *testing.T, path string, body any) (int, []byte) {
	t.Helper()
	return adminCall(t, http.MethodPost, e.srv.URL+path, body, "tok")
}

func (e *emailEnv) post(t *testing.T, channel, key string) (int, http.Header) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{
		"event_id": uuid.NewString(), "event_type": "PaymentSucceeded", "channel": channel,
		"payload": map[string]any{"amount_cents": 2500, "currency": "GBP", "account_id": "acct-1"},
	})
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/"+channel, bytes.NewReader(raw))
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, resp.Header
}

func (e *emailEnv) scalar(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (e *emailEnv) keyState(t *testing.T, key string) string {
	t.Helper()
	var s string
	err := e.pool.QueryRow(context.Background(), `SELECT state FROM idempotency_keys WHERE key = $1`, key).Scan(&s)
	if err != nil {
		return ""
	}
	return s
}

func skipShort(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
}

// ---------- the state machine ----------

func TestRealEmailIsSentOncePerKey(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	key := uuid.NewString() + ":email"

	code, hdr := e.post(t, "email", key)
	if code != 200 || hdr.Get("Idempotent-Replayed") != "" {
		t.Fatalf("first request: %d replayed=%q", code, hdr.Get("Idempotent-Replayed"))
	}
	for i := 0; i < 3; i++ { // the broker delivers the same message again and again
		code, hdr = e.post(t, "email", key)
		if code != 200 || hdr.Get("Idempotent-Replayed") != "true" {
			t.Fatalf("repeat %d: %d replayed=%q, want 200 and true", i, code, hdr.Get("Idempotent-Replayed"))
		}
	}
	if n := e.snd.count(); n != 1 {
		t.Fatalf("emails sent = %d, want 1", n)
	}
	if n := e.scalar(t, `SELECT count(*) FROM effects WHERE key = $1`, key); n != 1 {
		t.Fatalf("effects = %d, want 1", n)
	}
	if got := e.keyState(t, key); got != "DONE" {
		t.Fatalf("key state = %q, want DONE", got)
	}
	m := e.snd.mail(0)
	if m.To != "me@example.com" || !strings.Contains(m.Subject, "Payment succeeded: 25.00 GBP") {
		t.Fatalf("unexpected email: to=%q subject=%q", m.To, m.Subject)
	}
	if !strings.Contains(m.Body, key) {
		t.Fatalf("the body does not mention the idempotency key: %q", m.Body)
	}
}

func TestSwitchedOffTheEmailChannelNeverSends(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, false)
	if code, _ := e.post(t, "email", uuid.NewString()+":email"); code != 200 {
		t.Fatalf("status %d", code)
	}
	if e.snd.count() != 0 {
		t.Fatalf("an email was sent while real sending was off")
	}
	if n := e.scalar(t, `SELECT count(*) FROM effects`); n != 1 {
		t.Fatalf("the mock should still record its effect, got %d", n)
	}
}

func TestOnlyTheEmailChannelSendsRealEmail(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	for _, ch := range []string{"push", "webhook"} {
		if code, _ := e.post(t, ch, uuid.NewString()+":"+ch); code != 200 {
			t.Fatalf("%s: status %d", ch, code)
		}
	}
	if e.snd.count() != 0 {
		t.Fatalf("push or webhook sent an email")
	}
}

func TestCrashBetweenSendAndDoneResendsOnlyAfterTheStuckTimeout(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	crashed := false
	e.p.SetEmailAfterSend(func() error {
		if !crashed {
			crashed = true
			return errors.New("simulated crash after the email was accepted")
		}
		return nil
	})
	key := uuid.NewString() + ":email"

	if code, _ := e.post(t, "email", key); code != 500 {
		t.Fatalf("the crashed attempt answered %d, want 500", code)
	}
	if e.snd.count() != 1 || e.keyState(t, key) != "IN_PROGRESS" {
		t.Fatalf("after the crash: sent=%d state=%q, want 1 and IN_PROGRESS", e.snd.count(), e.keyState(t, key))
	}
	// An immediate retry must not send: the earlier attempt may still be running.
	code, hdr := e.post(t, "email", key)
	if code != 503 || hdr.Get("Retry-After") == "" {
		t.Fatalf("immediate retry: %d retry-after=%q, want 503 with Retry-After", code, hdr.Get("Retry-After"))
	}
	if e.snd.count() != 1 {
		t.Fatalf("an email was sent while the key was still in progress")
	}
	// After the stuck timeout the retry sends again: the documented duplicate window (at least once).
	time.Sleep(450 * time.Millisecond)
	if code, _ := e.post(t, "email", key); code != 200 {
		t.Fatalf("retry after the timeout: %d", code)
	}
	if e.snd.count() != 2 {
		t.Fatalf("emails sent = %d, want 2 (the duplicate in the crash window)", e.snd.count())
	}
	if e.snd.mail(0).MessageID == e.snd.mail(1).MessageID {
		t.Fatalf("both attempts used the same Message-ID, so a mailbox could hide the duplicate")
	}
	if n := e.scalar(t, `SELECT count(*) FROM effects WHERE key = $1`, key); n != 1 {
		t.Fatalf("effects = %d, want 1", n)
	}
	// And once DONE, further repeats are answered from the stored result.
	if code, hdr := e.post(t, "email", key); code != 200 || hdr.Get("Idempotent-Replayed") != "true" || e.snd.count() != 2 {
		t.Fatalf("after DONE: %d replayed=%q sent=%d", code, hdr.Get("Idempotent-Replayed"), e.snd.count())
	}
}

// A real send takes seconds, so the consumer's call timeout can fire while the email is on its way. The record
// after the send must survive that, or the key would stay IN_PROGRESS and the email would be sent again.
func TestTheRecordSurvivesTheCallerGivingUpMidSend(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	e.snd.delay = 400 * time.Millisecond
	key := uuid.NewString() + ":email"

	raw, _ := json.Marshal(map[string]any{"event_id": uuid.NewString(), "event_type": "PaymentSucceeded", "channel": "email",
		"payload": map[string]any{"amount_cents": 100, "currency": "GBP"}})
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/email", bytes.NewReader(raw))
	req.Header.Set("Idempotency-Key", key)
	impatient := &http.Client{Timeout: 100 * time.Millisecond}
	if resp, err := impatient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("the impatient caller was expected to time out")
	}

	waitFor(t, "the abandoned send to be recorded", 3*time.Second, func() bool { return e.keyState(t, key) == "DONE" })
	if e.snd.count() != 1 || e.scalar(t, `SELECT count(*) FROM effects WHERE key = $1`, key) != 1 {
		t.Fatalf("sent=%d, want 1 email and 1 effect", e.snd.count())
	}
	// The consumer's retry is answered from the stored result: no second email, no waiting for the stuck timeout.
	if code, hdr := e.post(t, "email", key); code != 200 || hdr.Get("Idempotent-Replayed") != "true" || e.snd.count() != 1 {
		t.Fatalf("retry: %d replayed=%q sent=%d", code, hdr.Get("Idempotent-Replayed"), e.snd.count())
	}
}

func TestConcurrentRequestsForOneKeySendOnce(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	e.snd.delay = 250 * time.Millisecond // keep the first send in flight while the others arrive
	key := uuid.NewString() + ":email"

	var wg sync.WaitGroup
	codes := make([]int, 5)
	for i := range codes {
		wg.Add(1)
		go func() { defer wg.Done(); codes[i], _ = e.post(t, "email", key) }()
	}
	wg.Wait()
	ok, busy := 0, 0
	for _, c := range codes {
		switch c {
		case 200:
			ok++
		case 503:
			busy++
		default:
			t.Fatalf("unexpected status %d in %v", c, codes)
		}
	}
	if e.snd.count() != 1 || ok < 1 {
		t.Fatalf("sent=%d statuses=%v, want exactly one email", e.snd.count(), codes)
	}
	// The requests that were told to retry now find the finished result.
	if code, hdr := e.post(t, "email", key); code != 200 || hdr.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("retry: %d replayed=%q", code, hdr.Get("Idempotent-Replayed"))
	}
	if e.snd.count() != 1 {
		t.Fatalf("emails sent = %d, want 1", e.snd.count())
	}
}

func TestHourlyCapRefusesAndLeavesNoKeyBehind(t *testing.T) {
	skipShort(t)
	cfg := testEmailConfig()
	cfg.MaxPerHour = 2
	e := newEmailProvider(t, cfg, true, true)
	keys := []string{uuid.NewString() + ":email", uuid.NewString() + ":email", uuid.NewString() + ":email"}
	want := []int{200, 200, 429}
	for i, k := range keys {
		code, hdr := e.post(t, "email", k)
		if code != want[i] {
			t.Fatalf("request %d: status %d, want %d", i, code, want[i])
		}
		if code == 429 && hdr.Get("Retry-After") == "" {
			t.Fatalf("429 without Retry-After")
		}
	}
	if e.snd.count() != 2 {
		t.Fatalf("emails sent = %d, want 2", e.snd.count())
	}
	if got := e.keyState(t, keys[2]); got != "" {
		t.Fatalf("the refused request left a key behind (%q), so its retry could never send", got)
	}
}

func TestDefiniteSendFailureLeavesNoKeySoTheRetrySends(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	key := uuid.NewString() + ":email"
	e.snd.setErr(&provider.SendError{Err: errors.New("smtp login: 535 bad credentials")})
	if code, _ := e.post(t, "email", key); code != 502 {
		t.Fatalf("status %d, want 502", code)
	}
	if got := e.keyState(t, key); got != "" {
		t.Fatalf("key state %q after a send that definitely did not happen, want none", got)
	}
	e.snd.setErr(nil)
	if code, _ := e.post(t, "email", key); code != 200 || e.snd.count() != 1 {
		t.Fatalf("retry: status %d sent=%d", code, e.snd.count())
	}
}

func TestFailedSendsDoNotUseUpTheHourlyCap(t *testing.T) {
	skipShort(t)
	cfg := testEmailConfig()
	cfg.MaxPerHour = 1
	e := newEmailProvider(t, cfg, true, true)
	e.snd.setErr(&provider.SendError{Err: errors.New("smtp login: 535 bad credentials")})
	for i := 0; i < 3; i++ { // retries against a broken SMTP account must not eat the cap
		if code, _ := e.post(t, "email", uuid.NewString()+":email"); code != 502 {
			t.Fatalf("failing send %d: status %d, want 502", i, code)
		}
	}
	e.snd.setErr(nil)
	if code, _ := e.post(t, "email", uuid.NewString()+":email"); code != 200 || e.snd.count() != 1 {
		t.Fatalf("after the account works again: status %d sent=%d, want 200 and 1", code, e.snd.count())
	}
}

func TestUncertainSendFailureKeepsTheKeyInProgress(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), true, true)
	key := uuid.NewString() + ":email"
	e.snd.setErr(&provider.SendError{Err: errors.New("connection lost after the message"), Uncertain: true})
	if code, _ := e.post(t, "email", key); code != 502 {
		t.Fatalf("status %d, want 502", code)
	}
	if got := e.keyState(t, key); got != "IN_PROGRESS" {
		t.Fatalf("key state %q, want IN_PROGRESS: the email may have been accepted", got)
	}
	e.snd.setErr(nil)
	if code, _ := e.post(t, "email", key); code != 503 {
		t.Fatalf("immediate retry: %d, want 503 (do not resend yet)", code)
	}
	time.Sleep(450 * time.Millisecond)
	if code, _ := e.post(t, "email", key); code != 200 || e.snd.count() != 1 {
		t.Fatalf("retry after the timeout: %d sent=%d", code, e.snd.count())
	}
}

func TestNegativeControlSendsEveryRequestInRealMode(t *testing.T) {
	skipShort(t)
	e := newEmailProvider(t, testEmailConfig(), false, true) // idempotency off: the control
	key := uuid.NewString() + ":email"
	for i := 0; i < 2; i++ {
		if code, _ := e.post(t, "email", key); code != 200 {
			t.Fatalf("status %d", code)
		}
	}
	if e.snd.count() != 2 || e.scalar(t, `SELECT count(*) FROM effects WHERE key = $1`, key) != 2 {
		t.Fatalf("with protection off the same key must produce two emails and two effects (got %d)", e.snd.count())
	}
}

// ---------- the operator endpoints ----------

func TestEmailSettingsRules(t *testing.T) {
	skipShort(t)
	pool := isolatedProviderPool(t)
	newProvider := func(cfg provider.EmailConfig, sender provider.Sender) *httptest.Server {
		p := provider.New(pool, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
		p.SetEmail(context.Background(), cfg, sender)
		p.SetAdmin(true, "tok")
		srv := httptest.NewServer(p.Handler())
		t.Cleanup(srv.Close)
		return srv
	}

	// Not configured (no sender): the switch cannot be turned on.
	none := newProvider(provider.EmailConfig{}, nil) // no SMTP settings at all
	code, body := adminCall(t, http.MethodPost, none.URL+"/admin/email", map[string]any{"real": true}, "tok")
	if code != 400 || !strings.Contains(string(body), "SMTP is not configured") {
		t.Fatalf("unconfigured: %d %s", code, body)
	}

	srv := newProvider(testEmailConfig(), &fakeSender{})
	call := func(body any) (int, map[string]any) {
		c, raw := adminCall(t, http.MethodPost, srv.URL+"/admin/email", body, "tok")
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return c, out
	}
	if c, _ := adminCall(t, http.MethodPost, srv.URL+"/admin/email", map[string]any{"real": true}, ""); c != 401 {
		t.Fatalf("without the token: %d, want 401", c)
	}
	if c, out := call(map[string]any{"real": true}); c != 400 {
		t.Fatalf("real without a recipient: %d %v", c, out)
	}
	if c, _ := call(map[string]any{"recipient": "not an address"}); c != 400 {
		t.Fatalf("invalid recipient accepted")
	}
	c, out := call(map[string]any{"recipient": "me@example.com", "real": true})
	if c != 200 || out["real"] != true || out["recipient"] != "m***@example.com" {
		t.Fatalf("enable: %d %v", c, out)
	}
	// The read-only status masks the recipient and never exposes credentials.
	resp, err := http.Get(srv.URL + "/email")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(raw), "me@example.com") || strings.Contains(string(raw), "\"p\"") {
		t.Fatalf("status leaks the recipient or a credential: %s", raw)
	}

	// A restart: the recipient is remembered, but real sending is off again.
	again := newProvider(testEmailConfig(), &fakeSender{})
	resp, err = http.Get(again.URL + "/email")
	if err != nil {
		t.Fatal(err)
	}
	var st map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&st)
	resp.Body.Close()
	if st["real"] != false || st["recipient"] != "m***@example.com" {
		t.Fatalf("after a restart: %v, want real=false and the saved recipient", st)
	}
}

func TestTestEmailSendsOneEmailAndCountsTowardsTheCap(t *testing.T) {
	skipShort(t)
	cfg := testEmailConfig()
	cfg.MaxPerHour = 1
	e := newEmailProvider(t, cfg, true, false)
	if code, _ := e.admin(t, "/admin/email", map[string]any{"recipient": "me@example.com"}); code != 200 {
		t.Fatal("save recipient")
	}
	code, body := e.admin(t, "/admin/email/test", nil)
	if code != 200 || e.snd.count() != 1 || !strings.Contains(string(body), "250") {
		t.Fatalf("test email: %d %s sent=%d", code, body, e.snd.count())
	}
	if code, _ := e.admin(t, "/admin/email/test", nil); code != 429 {
		t.Fatalf("second test email: %d, want 429 (cap of 1)", code)
	}
}

// ---------- the real SMTP client against a fake server ----------

type fakeSMTP struct {
	ln       net.Listener
	mode     string // "ok", "reject-rcpt", "drop-after-data"
	mu       sync.Mutex
	messages []string
}

func startFakeSMTP(t *testing.T, mode string) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeSMTP{ln: ln, mode: mode}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeSMTP) port() string { return strconv.Itoa(f.ln.Addr().(*net.TCPAddr).Port) }

func (f *fakeSMTP) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	say := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	say("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			say("250-fake")
			say("250-AUTH PLAIN")
			say("250 8BITMIME")
		case strings.HasPrefix(cmd, "AUTH"):
			say("235 2.7.0 Accepted")
		case strings.HasPrefix(cmd, "MAIL"):
			say("250 2.1.0 OK")
		case strings.HasPrefix(cmd, "RCPT"):
			if f.mode == "reject-rcpt" {
				say("550 5.1.1 no such user")
			} else {
				say("250 2.1.5 OK")
			}
		case cmd == "DATA":
			say("354 go ahead")
			var msg strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if strings.TrimRight(l, "\r\n") == "." {
					break
				}
				msg.WriteString(l)
			}
			f.mu.Lock()
			f.messages = append(f.messages, msg.String())
			f.mu.Unlock()
			if f.mode == "drop-after-data" {
				return // the connection dies before the server says whether it accepted the message
			}
			say("250 2.0.0 OK queued as fake" + strconv.Itoa(len(f.messages)))
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("502 not implemented")
		}
	}
}

func smtpConfig(f *fakeSMTP) provider.EmailConfig {
	return provider.EmailConfig{Host: "127.0.0.1", Port: f.port(), User: "u", Pass: "p", From: "from@example.com", Timeout: 2 * time.Second}
}

func TestSMTPSenderDeliversTheMessageAndReturnsTheServersReply(t *testing.T) {
	skipShort(t)
	f := startFakeSMTP(t, "ok")
	s := provider.NewSMTPSender(smtpConfig(f))
	reply, err := s.Send(context.Background(), provider.Mail{
		From: "from@example.com", To: "me@example.com", Subject: "Payment succeeded: 25.00 GBP",
		Body: "line one\nline two", MessageID: "<abc@example.com>",
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !strings.HasPrefix(reply, "250 ") || !strings.Contains(reply, "queued as fake1") {
		t.Fatalf("reply = %q, want the server's acceptance text", reply)
	}
	f.mu.Lock()
	got := ""
	if len(f.messages) == 1 {
		got = f.messages[0]
	}
	f.mu.Unlock()
	for _, want := range []string{"From: from@example.com", "To: me@example.com", "Subject: Payment succeeded: 25.00 GBP", "Message-ID: <abc@example.com>", "line two"} {
		if !strings.Contains(got, want) {
			t.Fatalf("message lacks %q:\n%s", want, got)
		}
	}
}

func TestSMTPSenderTellsAClearRefusalFromAnUnknownOutcome(t *testing.T) {
	skipShort(t)
	mail := provider.Mail{From: "from@example.com", To: "me@example.com", Subject: "s", Body: "b", MessageID: "<x@example.com>"}

	refused := provider.NewSMTPSender(smtpConfig(startFakeSMTP(t, "reject-rcpt")))
	_, err := refused.Send(context.Background(), mail)
	var se *provider.SendError
	if !errors.As(err, &se) || se.Uncertain {
		t.Fatalf("a refused recipient must be a definite failure, got %v", err)
	}

	dropped := provider.NewSMTPSender(smtpConfig(startFakeSMTP(t, "drop-after-data")))
	_, err = dropped.Send(context.Background(), mail)
	if !errors.As(err, &se) || !se.Uncertain {
		t.Fatalf("a connection lost after the message must be an uncertain failure, got %v", err)
	}

	closed := startFakeSMTP(t, "ok")
	cfg := smtpConfig(closed)
	_ = closed.ln.Close()
	_, err = provider.NewSMTPSender(cfg).Send(context.Background(), mail)
	if !errors.As(err, &se) || se.Uncertain {
		t.Fatalf("an unreachable server must be a definite failure, got %v", err)
	}
}
