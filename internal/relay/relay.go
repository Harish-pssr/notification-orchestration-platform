// Package relay moves outbox rows to the broker: claim, publish, finish (D4, D6).
package relay

import (
	"context"
	"encoding/json"
	"errors"
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
	"notifplatform/internal/metrics"
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
	BrokerBackoff  time.Duration // pause after a batch hit a broker failure (D18); 0 means 1 s
}

// brokerError marks a failure that happened while talking to the broker. It is the
// broker's fault, not the event's, so it must not use up the event's attempts (D18).
type brokerError struct{ err error }

func (e *brokerError) Error() string { return e.err.Error() }
func (e *brokerError) Unwrap() error { return e.err }

// Relay is one relay worker. Run several instances for concurrency.
type Relay struct {
	cfg           Config
	store         *outbox.Store
	js            jetstream.JetStream
	log           *slog.Logger
	faults        *faults.Registry // nil means no fault injection
	metrics       *metrics.Relay   // nil means no metrics
	paused        atomic.Bool      // true while the run loop is idle because of a pause fault
	brokerTrouble atomic.Bool      // set when a batch hit a broker failure; the run loop then backs off
}

// SetMetrics attaches the relay's metrics. Call it before Run.
func (r *Relay) SetMetrics(m *metrics.Relay) { r.metrics = m }

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
		if r.brokerTrouble.Swap(false) {
			// The broker is unreachable (D18): do not hammer the outbox with claims.
			select {
			case <-ctx.Done():
			case <-time.After(r.brokerBackoff()):
			}
		} else if n == 0 { // idle or errored: wait before polling again
			select {
			case <-ctx.Done():
			case <-time.After(r.cfg.Poll):
			}
		}
	}
	return nil
}

func (r *Relay) brokerBackoff() time.Duration {
	if r.cfg.BrokerBackoff > 0 {
		return r.cfg.BrokerBackoff
	}
	return time.Second
}

// RunOnce claims one batch and processes it, returning how many rows it claimed.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	claimStart := time.Now()
	claimed, err := r.store.Claim(ctx, r.cfg.Name, r.cfg.Lease, r.cfg.Batch)
	if err == nil && len(claimed) > 0 {
		// Only rounds that found work are timed: idle polls would swamp the histogram.
		r.metrics.Claimed(len(claimed), time.Since(claimStart))
	}
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

	brokerDown := false
	for _, c := range claimed {
		if brokerDown {
			// An earlier event in this batch just failed because the broker is
			// unreachable, so do not wait for a timeout on every other one (D18).
			r.releaseForBroker(ctx, c, "broker unavailable: not attempted")
			continue
		}
		if r.process(ctx, c, pending[c.ID]) {
			brokerDown = true
			r.brokerTrouble.Store(true)
		}
	}
	return len(claimed), nil
}

// process publishes one claimed event and finishes it. It reports whether the publish
// failed because of the broker (D18).
func (r *Relay) process(ctx context.Context, c outbox.Claimed, channels []outbox.Pending) (brokerFailed bool) {
	log := r.log.With("event_id", c.ID, "event_type", c.EventType, "attempt", c.Attempts)

	// Poison guard (D11): the relay itself crash-looped on this row. Dead-letter it
	// instead of publishing again.
	if c.Attempts > r.cfg.MaxAttempts {
		r.finishFailed(ctx, log, c, "exceeded max relay attempts", true)
		return false
	}

	publishStart := time.Now()
	err := r.Publish(ctx, c, channels)
	r.metrics.PublishDone(time.Since(publishStart), channelNames(channels), err)
	if err != nil {
		log.Warn("publish failed", "error", err, "status", "publish_failed")
		var be *brokerError
		if errors.As(err, &be) {
			// The broker's fault, not the event's: give the attempt back and retry (D18).
			r.releaseForBroker(ctx, c, "broker unavailable: "+err.Error())
			return true
		}
		r.finishFailed(ctx, log, c, err.Error(), c.Attempts >= r.cfg.MaxAttempts)
		return false
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

	finishStart := time.Now()
	ok, err := r.store.MarkPublished(ctx, c, r.cfg.Name)
	r.metrics.Finished(time.Since(finishStart), err == nil && ok)
	switch {
	case err != nil:
		// The messages are already in the broker. The row stays PROCESSING and will
		// be republished after the lease expires: a duplicate, absorbed downstream.
		log.Error("mark published failed", "error", err)
	case !ok:
		r.metrics.FenceLost()
		log.Warn("lease lost before finishing; another relay owns this event", "status", "fence_lost")
	default:
		log.Info("published", "status", "published", "channels", len(channels))
	}
	return false
}

// releaseForBroker puts an event back to RETRYING without spending an attempt (D18).
func (r *Relay) releaseForBroker(ctx context.Context, c outbox.Claimed, reason string) {
	ok, err := r.store.MarkBrokerUnavailable(ctx, c, r.cfg.Name, reason, r.brokerBackoff())
	switch {
	case err != nil:
		r.log.Error("release for broker failed", "event_id", c.ID, "error", err)
	case !ok:
		r.metrics.FenceLost()
		r.log.Warn("lease lost before releasing the event", "event_id", c.ID, "status", "fence_lost")
	default:
		r.log.Warn("broker unavailable, will retry without using an attempt", "event_id", c.ID, "status", "broker_unavailable")
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
		if p.Reconciles > 0 {
			// Same for a message the reconciler republished because the broker seemed
			// to have lost the earlier one (D22).
			msgID += fmt.Sprintf(":recon-%d", p.Reconciles)
		}
		if duplicate {
			msgID += ":dup-" + uuid.NewString()
		}
		msg.Header.Set(nats.MsgIdHdr, msgID)

		f, err := r.js.PublishMsgAsync(msg)
		if err != nil {
			return &brokerError{fmt.Errorf("publish to %s: %w", ch, err)}
		}
		futures = append(futures, f)
	}

	for i, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return &brokerError{fmt.Errorf("broker rejected %s: %w", pending[i].Channel, err)}
		case <-ctx.Done():
			return &brokerError{fmt.Errorf("waiting for broker ack: %w", ctx.Err())}
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
		r.metrics.FenceLost()
		log.Warn("lease lost before recording failure", "status", "fence_lost")
	case dlq:
		r.metrics.DeadLettered()
		log.Error("event dead-lettered", "status", "dlq", "reason", reason)
	default:
		log.Info("scheduled retry", "status", "retrying", "in", delay)
	}
}

// channelNames lists the channels of a publish, for the per-channel message counter.
func channelNames(pending []outbox.Pending) []string {
	names := make([]string, len(pending))
	for i, p := range pending {
		names[i] = string(p.Channel)
	}
	return names
}
