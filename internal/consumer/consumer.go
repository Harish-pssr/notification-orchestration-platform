// Package consumer pulls messages for one channel and performs the side effect by
// calling the provider with an idempotency key (D5, D6, D11), retrying transient
// failures with backoff and dead-lettering the rest (D7).
package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/backoff"
	"notifplatform/internal/events"
	"notifplatform/internal/faults"
)

// Config holds the consumer's tunables.
type Config struct {
	Name        string
	Channel     events.Channel
	ProviderURL string        // e.g. http://provider:9090
	Batch       int           // messages requested per fetch
	FetchWait   time.Duration // how long an open fetch request waits at the broker
	CallTimeout time.Duration // timeout for one provider call
	RetryDelay  time.Duration // delay before retrying after a database error (not a provider failure)

	MaxAttempts int           // deliveries allowed before a message is dead-lettered (D7)
	BackoffBase time.Duration // first retry delay
	BackoffCap  time.Duration // upper bound for any retry delay
}

// Consumer is one consumer instance for one channel. Replicas share the channel's
// durable consumer, so the broker divides messages among them.
type Consumer struct {
	cfg    Config
	pool   *pgxpool.Pool
	cons   jetstream.Consumer
	http   *http.Client
	log    *slog.Logger
	faults *faults.Registry // nil means no fault injection
	paused atomic.Bool      // true while the run loop is idle because of a pause fault
}

// New builds a consumer. cons is the durable pull consumer for cfg.Channel.
func New(cfg Config, pool *pgxpool.Pool, cons jetstream.Consumer, log *slog.Logger) *Consumer {
	return &Consumer{
		cfg: cfg, pool: pool, cons: cons,
		http: &http.Client{Timeout: cfg.CallTimeout},
		log:  log.With("worker", cfg.Name, "consumer", string(cfg.Channel)),
	}
}

// SetFaults attaches the fault registry (D8). Call it before Run.
func (c *Consumer) SetFaults(f *faults.Registry) { c.faults = f }

// skipCheckDisabled reports whether the "skip if already delivered" shortcut has
// been switched off by a fault. With it off, duplicates reach the provider, which
// must suppress them by itself (the property D5 relies on).
func (c *Consumer) skipCheckDisabled() bool {
	_, off := c.faults.Active("consumer.skip_delivered_check")
	return off
}

// pauseRequested reports whether a pause fault is in force for this channel.
func (c *Consumer) pauseRequested() bool {
	_, paused := c.faults.Active("consumer." + string(c.cfg.Channel) + ".pause")
	return paused
}

// Paused reports whether this consumer has actually stopped taking work. It is set by
// the run loop itself, not by the fault: an outstanding fetch request can still
// deliver a message for a moment after the pause is requested, and the heartbeat must
// not claim "paused" before that window has closed.
func (c *Consumer) Paused() bool { return c.paused.Load() }

// Run fetches and handles messages until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("consumer started", "max_attempts", c.cfg.MaxAttempts)
	for ctx.Err() == nil {
		if c.pauseRequested() {
			// Paused by a fault (D8): stop asking for work. Messages wait safely in the
			// broker, which is exactly what the "consumer down" demo shows. Only now,
			// with no fetch outstanding, do we report ourselves as paused.
			c.paused.Store(true)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		c.paused.Store(false)
		// Fetch sends a request that stays open at the broker: messages are handed
		// over as they arrive, and an empty wait simply times out and loops.
		batch, err := c.cons.Fetch(c.cfg.Batch, jetstream.FetchMaxWait(c.cfg.FetchWait))
		if err != nil {
			c.log.Error("fetch failed", "error", err)
			sleep(ctx, time.Second)
			continue
		}
		for msg := range batch.Messages() {
			c.handle(ctx, msg)
		}
		if err := batch.Error(); err != nil && ctx.Err() == nil {
			c.log.Warn("fetch ended with error", "error", err)
		}
	}
	return nil
}

// Handle processes one message. It is exported so tests can feed a message in
// without a broker.
func (c *Consumer) Handle(ctx context.Context, msg jetstream.Msg) { c.handle(ctx, msg) }

func (c *Consumer) handle(ctx context.Context, msg jetstream.Msg) {
	var m events.Message
	if err := json.Unmarshal(msg.Data(), &m); err != nil || m.Channel != c.cfg.Channel {
		c.log.Error("unreadable message, discarding", "error", err)
		_ = msg.Term()
		return
	}
	meta, err := msg.Metadata()
	if err != nil {
		c.log.Error("no metadata", "error", err)
		_ = msg.Nak()
		return
	}
	// NumDelivered counts every delivery of this message, including redeliveries after
	// a crash that never recorded anything, so it is the attempt number (D7, D11).
	attempt := int(meta.NumDelivered)
	log := c.log.With("event_id", m.EventID, "event_type", m.EventType, "attempt", attempt)

	// One query returns both the delivery state and the payload (D6).
	var status string
	var payload json.RawMessage
	err = c.pool.QueryRow(ctx, `
		SELECT d.status, o.payload
		FROM channel_deliveries d JOIN outbox_events o ON o.id = d.event_id
		WHERE d.event_id = $1 AND d.channel = $2`, m.EventID, string(m.Channel)).Scan(&status, &payload)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		log.Error("no delivery row for message, discarding", "status", "orphan")
		_ = msg.Term()
		return
	case err != nil:
		log.Error("load delivery failed", "error", err)
		_ = msg.NakWithDelay(c.cfg.RetryDelay)
		return
	case status == "DLQ":
		// Already dead-lettered; a replay sends a new message, so this one is stale.
		log.Warn("message for a dead-lettered delivery, discarding", "status", "stale_dlq")
		_ = msg.Term()
		return
	case status == "DELIVERED" && !c.skipCheckDisabled():
		// A duplicate delivery: nothing to do. Skipping saves a provider call, but it
		// is only an optimisation; correctness comes from the provider (D5).
		if _, err := c.pool.Exec(ctx, `
			UPDATE channel_deliveries SET duplicate_count = duplicate_count + 1
			WHERE event_id = $1 AND channel = $2`, m.EventID, string(m.Channel)); err != nil {
			log.Error("count duplicate failed", "error", err)
		}
		log.Info("duplicate delivery ignored", "status", "duplicate_ignored")
		_ = msg.Ack()
		return
	}

	// Poison guard (D11): more deliveries than we allow means earlier attempts died
	// before recording anything (for example a crash on this payload). Dead-letter
	// without calling the provider again.
	if attempt > c.cfg.MaxAttempts {
		c.finishDead(ctx, log, msg, m, attempt, "exceeded max deliveries (the consumer may be crashing on this message)")
		return
	}

	callErr := c.call(ctx, m, payload)
	if callErr == nil {
		c.finishDelivered(ctx, log, msg, m, attempt)
		return
	}

	switch {
	case callErr.Permanent():
		// Retrying the identical request cannot help (D7): straight to the DLQ.
		c.finishDead(ctx, log, msg, m, attempt, "permanent error: "+callErr.Error())
	case attempt >= c.cfg.MaxAttempts:
		c.finishDead(ctx, log, msg, m, attempt, "retries exhausted: "+callErr.Error())
	default:
		// NATS owns the retry timing: we only choose the delay (D7).
		delay := backoff.Delay(attempt, c.cfg.BackoffBase, c.cfg.BackoffCap, rand.Float64)
		if callErr.RetryAfter > delay { // honour a provider's Retry-After, within our cap
			delay = min(callErr.RetryAfter, c.cfg.BackoffCap)
		}
		log.Warn("provider call failed, will retry", "error", callErr.Error(), "status", "retrying", "retry_in", delay)
		c.recordRetry(ctx, m, attempt, delay, callErr)
		_ = msg.NakWithDelay(delay)
	}
}

func (c *Consumer) finishDelivered(ctx context.Context, log *slog.Logger, msg jetstream.Msg, m events.Message, attempt int) {
	if _, err := c.pool.Exec(ctx, `
		UPDATE channel_deliveries
		SET status = 'DELIVERED', attempts = $3, delivered_at = coalesce(delivered_at, now()),
		    last_error = NULL, next_attempt_at = NULL
		WHERE event_id = $1 AND channel = $2`, m.EventID, string(m.Channel), attempt); err != nil {
		// The provider already ran the effect. Without an ack NATS redelivers, the
		// provider recognises the key, and we try this update again.
		log.Error("mark delivered failed", "error", err)
		_ = msg.NakWithDelay(c.cfg.RetryDelay)
		return
	}
	_ = msg.Ack()
	log.Info("delivered", "status", "delivered")
}

// finishDead records the dead-letter state and only then terminates the message, so
// a failure to record leaves the message to be redelivered and tried again.
func (c *Consumer) finishDead(ctx context.Context, log *slog.Logger, msg jetstream.Msg, m events.Message, attempt int, reason string) {
	if _, err := c.pool.Exec(ctx, `
		UPDATE channel_deliveries
		SET status = 'DLQ', attempts = $3, last_error = $4, next_attempt_at = NULL
		WHERE event_id = $1 AND channel = $2 AND status <> 'DELIVERED'`,
		m.EventID, string(m.Channel), attempt, reason); err != nil {
		log.Error("dead-letter write failed", "error", err)
		_ = msg.NakWithDelay(c.cfg.RetryDelay)
		return
	}
	_ = msg.Term() // stop redelivery
	log.Error("delivery dead-lettered", "status", "dlq", "reason", reason)
}

// call performs the side effect. The idempotency key makes retries and duplicate
// deliveries safe (D5). It returns nil on success.
func (c *Consumer) call(ctx context.Context, m events.Message, payload json.RawMessage) *CallError {
	body, _ := json.Marshal(map[string]any{
		"event_id": m.EventID, "event_type": m.EventType, "channel": m.Channel, "payload": payload,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/v1/%s", c.cfg.ProviderURL, m.Channel), bytes.NewReader(body))
	if err != nil {
		return &CallError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", events.DedupKey(m.EventID, m.Channel))

	resp, err := c.http.Do(req)
	if err != nil {
		return &CallError{Err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return &CallError{Status: resp.StatusCode, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	return nil
}

// recordRetry mirrors the failure to the delivery row for visibility (D7). NATS
// remains the retry scheduler; this write schedules nothing.
func (c *Consumer) recordRetry(ctx context.Context, m events.Message, attempt int, delay time.Duration, cause *CallError) {
	if _, err := c.pool.Exec(ctx, `
		UPDATE channel_deliveries
		SET status = 'RETRYING', attempts = $3, last_error = $4,
		    next_attempt_at = now() + make_interval(secs => $5)
		WHERE event_id = $1 AND channel = $2 AND status <> 'DELIVERED'`,
		m.EventID, string(m.Channel), attempt, cause.Error(), delay.Seconds()); err != nil {
		c.log.Error("record retry failed", "error", err)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
