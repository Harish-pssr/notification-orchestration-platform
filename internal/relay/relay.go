// Package relay moves outbox rows to the broker: claim, publish, finish (D4, D6).
package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/backoff"
	"notifplatform/internal/events"
	"notifplatform/internal/faults"
	"notifplatform/internal/outbox"
)

// Config holds the relay's tunables.
type Config struct {
	Name           string        // unique per instance; part of the fencing token
	Lease          time.Duration // how long a claim is valid before another relay may take it
	Batch          int           // rows claimed per round
	Poll           time.Duration // sleep when there is nothing to claim
	MaxAttempts    int
	BackoffBase    time.Duration
	BackoffCap     time.Duration
	PublishTimeout time.Duration
}

// Relay is one relay worker. Run several instances for concurrency.
type Relay struct {
	cfg    Config
	store  *outbox.Store
	js     jetstream.JetStream
	log    *slog.Logger
	faults *faults.Registry // nil means no fault injection
	paused atomic.Bool      // true while the run loop is idle because of a pause fault
}

// SetFaults attaches the fault registry (D8). Call it before Run.
func (r *Relay) SetFaults(f *faults.Registry) { r.faults = f }

// New builds a relay.
func New(cfg Config, store *outbox.Store, js jetstream.JetStream, log *slog.Logger) *Relay {
	return &Relay{cfg: cfg, store: store, js: js, log: log.With("worker", cfg.Name)}
}

// pauseRequested reports whether a pause fault is in force for the relay role.
func (r *Relay) pauseRequested() bool {
	_, paused := r.faults.Active("relay.pause")
	return paused
}

// Paused reports whether this relay has actually stopped claiming work. It is set by
// the run loop after the batch in hand is finished, so "paused" is never reported
// while a claim is still being processed.
func (r *Relay) Paused() bool { return r.paused.Load() }

// Run claims and publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("relay started", "lease", r.cfg.Lease, "batch", r.cfg.Batch)
	for ctx.Err() == nil {
		if r.pauseRequested() {
			// Paused by a fault (D8): claim nothing. Work waits safely in the outbox.
			r.paused.Store(true)
			select {
			case <-ctx.Done():
			case <-time.After(r.cfg.Poll):
			}
			continue
		}
		r.paused.Store(false)
		n, err := r.RunOnce(ctx)
		if err != nil {
			r.log.Error("relay round failed", "error", err)
		}
		if n == 0 { // idle or errored: wait before polling again
			select {
			case <-ctx.Done():
			case <-time.After(r.cfg.Poll):
			}
		}
	}
	return nil
}

// RunOnce claims one batch and processes it, returning how many rows it claimed.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	claimed, err := r.store.Claim(ctx, r.cfg.Name, r.cfg.Lease, r.cfg.Batch)
	if err != nil || len(claimed) == 0 {
		return 0, err
	}

	ids := make([]uuid.UUID, len(claimed))
	for i, c := range claimed {
		ids[i] = c.ID
	}
	pending, err := r.store.PendingChannels(ctx, ids)
	if err != nil {
		// The rows stay PROCESSING; their leases expire and another round retries.
		return len(claimed), err
	}

	for _, c := range claimed {
		r.process(ctx, c, pending[c.ID])
	}
	return len(claimed), nil
}

func (r *Relay) process(ctx context.Context, c outbox.Claimed, channels []outbox.Pending) {
	log := r.log.With("event_id", c.ID, "event_type", c.EventType, "attempt", c.Attempts)

	// Poison guard (D11): the relay itself crash-looped on this row. Dead-letter it
	// instead of publishing again.
	if c.Attempts > r.cfg.MaxAttempts {
		r.finishFailed(ctx, log, c, "exceeded max relay attempts", true)
		return
	}

	if err := r.Publish(ctx, c, channels); err != nil {
		log.Warn("publish failed", "error", err, "status", "publish_failed")
		r.finishFailed(ctx, log, c, err.Error(), c.Attempts >= r.cfg.MaxAttempts)
		return
	}

	// Fault hook (D8): the window between "broker accepted" and "database updated".
	if f, fire := r.faults.Fire(ctx, "relay.after_publish"); fire {
		switch f.Action {
		case faults.Crash:
			// Die before recording success: after the lease expires another relay
			// republishes this event. This is the worst case from the project brief.
			r.faults.Crash()
		case faults.Duplicate:
			// A second copy with fresh message IDs, so the broker's dedup window
			// cannot absorb it and the consumers really receive a duplicate.
			if err := r.publish(ctx, c, channels, true); err != nil {
				log.Warn("duplicate publish failed", "error", err)
			} else {
				log.Warn("published a duplicate copy on purpose", "status", "duplicate_injected")
			}
		}
	}

	ok, err := r.store.MarkPublished(ctx, c, r.cfg.Name)
	switch {
	case err != nil:
		// The messages are already in the broker. The row stays PROCESSING and will
		// be republished after the lease expires: a duplicate, absorbed downstream.
		log.Error("mark published failed", "error", err)
	case !ok:
		log.Warn("lease lost before finishing; another relay owns this event", "status", "fence_lost")
	default:
		log.Info("published", "status", "published", "channels", len(channels))
	}
}

// Publish sends one thin message per pending channel (D6) and waits for every
// broker acknowledgement. The NATS message ID lets the broker drop an exact repeat
// inside its dedup window. It is exported so tests can publish a claimed event and
// then deliberately not finish it, simulating a relay crash.
func (r *Relay) Publish(ctx context.Context, c outbox.Claimed, pending []outbox.Pending) error {
	return r.publish(ctx, c, pending, false)
}

// publish does the work of Publish. With duplicate=true every message gets a unique
// NATS message ID, so the broker stores it even though an identical one was just
// accepted (used by the "duplicate" fault).
func (r *Relay) publish(ctx context.Context, c outbox.Claimed, pending []outbox.Pending, duplicate bool) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()

	futures := make([]jetstream.PubAckFuture, 0, len(pending))
	for _, p := range pending {
		ch := p.Channel
		data, err := json.Marshal(events.Message{EventID: c.ID, Channel: ch, EventType: c.EventType})
		if err != nil {
			return err
		}
		msg := &nats.Msg{Subject: ch.Subject(), Data: data, Header: nats.Header{}}
		msgID := events.DedupKey(c.ID, ch)
		if p.Replays > 0 {
			// A replay is a new message, so the broker's dedup window must not drop it
			// as a repeat of the original publish.
			msgID += fmt.Sprintf(":replay-%d", p.Replays)
		}
		if duplicate {
			msgID += ":dup-" + uuid.NewString()
		}
		msg.Header.Set(nats.MsgIdHdr, msgID)

		f, err := r.js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("publish to %s: %w", ch, err)
		}
		futures = append(futures, f)
	}

	for i, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("broker rejected %s: %w", pending[i].Channel, err)
		case <-ctx.Done():
			return fmt.Errorf("waiting for broker ack: %w", ctx.Err())
		}
	}
	return nil
}

func (r *Relay) finishFailed(ctx context.Context, log *slog.Logger, c outbox.Claimed, reason string, dlq bool) {
	delay := backoff.Delay(c.Attempts, r.cfg.BackoffBase, r.cfg.BackoffCap, rand.Float64)
	ok, err := r.store.MarkFailed(ctx, c, r.cfg.Name, reason, delay, dlq)
	switch {
	case err != nil:
		log.Error("mark failed failed", "error", err)
	case !ok:
		log.Warn("lease lost before recording failure", "status", "fence_lost")
	case dlq:
		log.Error("event dead-lettered", "status", "dlq", "reason", reason)
	default:
		log.Info("scheduled retry", "status", "retrying", "in", delay)
	}
}
