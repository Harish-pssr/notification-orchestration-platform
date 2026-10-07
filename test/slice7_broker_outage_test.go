package test

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/outbox"
	"notifplatform/internal/relay"
)

// fakeBroker stands in for JetStream. Embedding the interface leaves every method the
// relay does not use nil. While down is true every publish fails immediately, like a
// disconnected NATS client with its reconnect buffer switched off (D18).
type fakeBroker struct {
	jetstream.JetStream
	down  atomic.Bool
	calls atomic.Int64
}

func (b *fakeBroker) PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	b.calls.Add(1)
	if b.down.Load() {
		return nil, errors.New("nats: disconnected")
	}
	return okFuture{}, nil
}

type okFuture struct{}

func (okFuture) Ok() <-chan *jetstream.PubAck {
	ch := make(chan *jetstream.PubAck, 1)
	ch <- &jetstream.PubAck{}
	return ch
}
func (okFuture) Err() <-chan error { return nil } // never fires
func (okFuture) Msg() *nats.Msg    { return nil }

func outageRelay(t *testing.T, b *fakeBroker) (*relay.Relay, *outbox.Store, *pgxpool.Pool) {
	t.Helper()
	store, pool := isolatedStore(t)
	r := relay.New(relay.Config{
		Name: "relay-outage-test", Lease: 5 * time.Second, Batch: 10, Poll: 10 * time.Millisecond,
		MaxAttempts: 5, BackoffBase: 10 * time.Millisecond, BackoffCap: 20 * time.Millisecond,
		PublishTimeout: time.Second, BrokerBackoff: 20 * time.Millisecond,
	}, store, b, slog.Default())
	return r, store, pool
}

// D18: a broker outage must never dead-letter an event, however long it lasts and
// however few events are waiting. Before the fix an event used one of its five attempts
// on every failed publish and was dead-lettered by the fifth.
func TestBrokerOutageNeverDeadLettersAnEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	b := &fakeBroker{}
	b.down.Store(true)
	r, store, pool := outageRelay(t, b)
	id := seedOutbox(t, store, 1)[0]
	ctx := context.Background()

	for round := 0; round < 12; round++ { // more than twice MaxAttempts
		if _, err := r.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond) // let the retry time pass
		status, attempts := outboxStatus(t, pool, id)
		if status == "DLQ" {
			t.Fatalf("round %d: the event was dead-lettered by a broker outage", round)
		}
		if attempts != 0 {
			t.Fatalf("round %d: attempts = %d, want 0 (a broker failure must not spend an attempt)", round, attempts)
		}
	}
	var reason string
	_ = pool.QueryRow(ctx, `SELECT coalesce(last_error, '') FROM outbox_events WHERE id = $1`, id).Scan(&reason)
	if len(reason) < 18 || reason[:18] != "broker unavailable" {
		t.Errorf("last_error = %q, want it to say the broker was unavailable", reason)
	}

	// The broker returns: the same event is published.
	b.down.Store(false)
	time.Sleep(40 * time.Millisecond)
	if _, err := r.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if status, _ := outboxStatus(t, pool, id); status != "PUBLISHED" {
		t.Errorf("status after the broker returned = %s, want PUBLISHED", status)
	}
}

// One failing publish releases the rest of the batch without trying them, so a relay
// does not wait out a timeout per event while the broker is down.
func TestBrokerFailureReleasesTheRestOfTheBatchWithoutTryingIt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	b := &fakeBroker{}
	b.down.Store(true)
	r, store, pool := outageRelay(t, b)
	ids := seedOutbox(t, store, 4)

	if n, err := r.RunOnce(context.Background()); err != nil || n != 4 {
		t.Fatalf("RunOnce = %d, %v; want 4 claimed", n, err)
	}
	if got := b.calls.Load(); got != 1 {
		t.Errorf("publish was attempted %d times, want 1 (the other three are released untried)", got)
	}
	for _, id := range ids {
		status, attempts := outboxStatus(t, pool, id)
		if status != "RETRYING" || attempts != 0 {
			t.Errorf("event %s: status %s attempts %d, want RETRYING with 0 attempts used", id, status, attempts)
		}
	}
}
