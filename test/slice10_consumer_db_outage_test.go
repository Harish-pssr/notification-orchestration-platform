package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/consumer"
	"notifplatform/internal/events"
)

// downDatabase returns a pool pointed at a port where nothing listens, so every query fails at
// once: a database that is down. It needs no running stack.
func downDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://notif:notif@127.0.0.1:1/platform?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func dbOutageConsumer(t *testing.T, cfg consumer.Config) *consumer.Consumer {
	t.Helper()
	cfg.Name, cfg.Channel, cfg.ProviderURL = "test", events.Email, "http://127.0.0.1:1"
	cfg.CallTimeout, cfg.MaxAttempts = time.Second, 5
	cfg.BackoffBase, cfg.BackoffCap = 100*time.Millisecond, 5*time.Second
	return consumer.New(cfg, downDatabase(t), nil, slog.Default())
}

// The Postgres-kill chaos run found that a database outage of about eight seconds made the broker give up
// on in-flight messages: the consumer asked for a redelivery after a fixed second each time, and the
// broker stops after MaxDeliver (MaxAttempts + 3 = 8) deliveries without telling us. The delay after a
// database error must therefore grow with the delivery count, so that all eight deliveries span a long
// outage and not eight seconds of it.
func TestDatabaseErrorRetryDelayGrowsWithEveryDelivery(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: 100 * time.Millisecond, RetryCap: 10 * time.Second})
	id := uuid.NewString()

	delay := func(delivery uint64) time.Duration {
		msg := newFakeMsg(t, id, events.Email, delivery)
		c.Handle(context.Background(), msg)
		if len(msg.naks) != 1 || msg.acks != 0 || msg.terms != 0 {
			t.Fatalf("delivery %d: naks=%v acks=%d terms=%d, want one delayed nak and nothing else", delivery, msg.naks, msg.acks, msg.terms)
		}
		return msg.naks[0]
	}

	// Equal jitter: between half and all of base * 2^(delivery-1), capped at 10 s.
	checks := []struct {
		delivery uint64
		lo, hi   time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{7, 3200 * time.Millisecond, 6400 * time.Millisecond},
		{8, 5 * time.Second, 10 * time.Second}, // 12.8 s nominal, capped
	}
	for _, ck := range checks {
		if d := delay(ck.delivery); d < ck.lo || d > ck.hi {
			t.Errorf("delivery %d: retry delay %v outside [%v, %v]", ck.delivery, d, ck.lo, ck.hi)
		}
	}
}

// With production settings (1 s base, 30 s cap) the seven delays between the eight deliveries add up to at
// least 45 s even with the lowest jitter, so a database outage of that length cannot use up the broker's
// delivery limit.
func TestProductionDefaultsSurviveADatabaseOutageOfFortyFiveSeconds(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: time.Second, RetryCap: 30 * time.Second})
	id := uuid.NewString()
	var shortest time.Duration
	for delivery := uint64(1); delivery <= 7; delivery++ {
		msg := newFakeMsg(t, id, events.Email, delivery)
		c.Handle(context.Background(), msg)
		if len(msg.naks) != 1 {
			t.Fatalf("delivery %d: naks=%v, want one", delivery, msg.naks)
		}
		// Half of the nominal delay is the least equal jitter can return.
		nominal := time.Second << (delivery - 1)
		if nominal > 30*time.Second {
			nominal = 30 * time.Second
		}
		if msg.naks[0] < nominal/2 || msg.naks[0] > nominal {
			t.Errorf("delivery %d: delay %v outside [%v, %v]", delivery, msg.naks[0], nominal/2, nominal)
		}
		shortest += nominal / 2
	}
	if shortest < 45*time.Second {
		t.Errorf("the guaranteed outage tolerance is %v, want at least 45s", shortest)
	}
}

func TestDatabaseErrorRetryDelayStaysFixedWhenNoCapIsConfigured(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: 100 * time.Millisecond}) // RetryCap left at zero
	msg := newFakeMsg(t, uuid.NewString(), events.Email, 6)
	c.Handle(context.Background(), msg)
	if len(msg.naks) != 1 || msg.naks[0] < 50*time.Millisecond || msg.naks[0] > 100*time.Millisecond {
		t.Errorf("naks = %v, want one delay of at most the fixed RetryDelay (100ms)", msg.naks)
	}
}
