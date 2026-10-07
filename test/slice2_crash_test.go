package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
	"notifplatform/internal/relay"
)

// The worst case from the project description: a relay publishes to the broker and
// dies BEFORE recording success. The real relay container must take the event over
// after the lease expires and publish it again; the consumers and the idempotent
// provider must still produce exactly one effect per channel.
func TestRelayCrashAfterPublishIsRecoveredWithoutDuplicateEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	pool := platformPool(t)
	store := outbox.NewStore(pool)

	nc, err := nats.Connect(config.Load().NATSURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.EnsureStream(ctx, js); err != nil {
		t.Fatal(err)
	}
	crashing := relay.New(relay.Config{Name: "relay-doomed", PublishTimeout: 5 * time.Second}, store, js, slog.Default())

	// Create events until our "doomed" relay wins the race against the real relay
	// container (which polls every 200ms and would normally claim first).
	var claimed outbox.Claimed
	for attempt := 0; ; attempt++ {
		if attempt > 50 {
			t.Fatal("could not claim a fresh event ahead of the real relay")
		}
		res, err := store.CreateEvent(ctx, outbox.CreateInput{
			Key: uuid.NewString(), RequestHash: "h", EventType: events.PaymentSucceeded,
			Write: func(ctx context.Context, tx pgx.Tx, id uuid.UUID) (outbox.Aggregate, error) {
				return outbox.Aggregate{Type: "payment", ID: uuid.NewString(), Payload: map[string]any{"crash": true}}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		// A 2s lease: short enough for a quick test, far longer than a publish.
		got, err := store.Claim(ctx, "relay-doomed", 2*time.Second, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 1 && got[0].ID == res.EventID {
			claimed = got[0]
			break
		}
	}

	// Publish to the broker, then "crash": never call MarkPublished.
	pending, err := store.PendingChannels(ctx, []uuid.UUID{claimed.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := crashing.Publish(ctx, claimed, pending[claimed.ID]); err != nil {
		t.Fatalf("publish before crash: %v", err)
	}
	id := claimed.ID.String()

	// The real relay takes over after the lease and the event completes.
	waitDelivered(t, id, 3)

	var status string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT status, attempts FROM outbox_events WHERE id = $1`, claimed.ID).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" || attempts < 2 {
		t.Fatalf("outbox row = %s after %d attempts; want PUBLISHED by a second relay (attempts >= 2)", status, attempts)
	}

	// Whatever duplicates the broker or consumers saw, the provider ran each effect once.
	var effects, distinctKeys int
	err = providerPool(t).QueryRow(ctx,
		`SELECT count(*), count(DISTINCT key) FROM effects WHERE key LIKE $1`, id+":%").Scan(&effects, &distinctKeys)
	if err != nil {
		t.Fatal(err)
	}
	if effects != 3 || distinctKeys != 3 {
		t.Fatalf("effects=%d distinct keys=%d, want 3 and 3 (one per channel)", effects, distinctKeys)
	}
}
