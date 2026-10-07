package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests exercise D4 directly on the Store, in an isolated database.

func TestConcurrentClaimsAreDisjoint(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, _ := isolatedStore(t)
	const events, workers = 300, 8
	seedOutbox(t, store, events)

	var mu sync.Mutex
	owner := map[uuid.UUID]string{}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		name := "relay-" + string(rune('a'+w))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := store.Claim(context.Background(), name, 30*time.Second, 7)
				if err != nil {
					t.Errorf("%s: claim: %v", name, err)
					return
				}
				if len(claimed) == 0 {
					return
				}
				mu.Lock()
				for _, c := range claimed {
					if prev, dup := owner[c.ID]; dup {
						t.Errorf("event %s claimed by both %s and %s", c.ID, prev, name)
					}
					owner[c.ID] = name
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(owner) != events {
		t.Fatalf("claimed %d distinct events, want %d", len(owner), events)
	}
}

func TestLeaseExpiryLetsAnotherWorkerTakeOver(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 1)
	ctx := context.Background()

	a, err := store.Claim(ctx, "relay-a", time.Second, 10)
	if err != nil || len(a) != 1 || a[0].Attempts != 1 {
		t.Fatalf("relay-a claim = %+v, err=%v; want one row at attempt 1", a, err)
	}

	// relay-a "crashes": it never finishes. Before the lease expires nobody else may take the row.
	if b, _ := store.Claim(ctx, "relay-b", time.Second, 10); len(b) != 0 {
		t.Fatalf("relay-b claimed %d rows during relay-a's lease, want 0", len(b))
	}

	time.Sleep(1200 * time.Millisecond) // lease expires

	b, err := store.Claim(ctx, "relay-b", 30*time.Second, 10)
	if err != nil || len(b) != 1 || b[0].ID != ids[0] || b[0].Attempts != 2 {
		t.Fatalf("relay-b claim after expiry = %+v, err=%v; want the same row at attempt 2", b, err)
	}

	// relay-a wakes up late and tries to finish: the fence must reject it.
	if ok, _ := store.MarkPublished(ctx, a[0], "relay-a"); ok {
		t.Fatal("stale relay-a was allowed to finish a row it no longer owns")
	}
	if status, _ := outboxStatus(t, pool, ids[0]); status != "PROCESSING" {
		t.Fatalf("status after rejected finish = %s, want PROCESSING", status)
	}

	if ok, err := store.MarkPublished(ctx, b[0], "relay-b"); err != nil || !ok {
		t.Fatalf("relay-b finish = %v, %v; want true", ok, err)
	}
	if status, _ := outboxStatus(t, pool, ids[0]); status != "PUBLISHED" {
		t.Fatalf("final status = %s, want PUBLISHED", status)
	}
}

// The fencing token is (worker, attempts): the same worker re-claiming after its own
// lease expired must not be confused with its earlier attempt.
func TestFenceRejectsAWorkersOwnStaleAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, _ := isolatedStore(t)
	seedOutbox(t, store, 1)
	ctx := context.Background()

	first, _ := store.Claim(ctx, "relay-a", time.Second, 10)
	time.Sleep(1200 * time.Millisecond)
	second, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10)
	if len(first) != 1 || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("claims = %+v then %+v", first, second)
	}

	if ok, _ := store.MarkPublished(ctx, first[0], "relay-a"); ok {
		t.Fatal("a stale attempt by the same worker passed the fence")
	}
	if ok, _ := store.MarkPublished(ctx, second[0], "relay-a"); !ok {
		t.Fatal("the current attempt was rejected")
	}
}

func TestFailedRowWaitsForItsBackoff(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 1)
	ctx := context.Background()

	c, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10)
	if ok, err := store.MarkFailed(ctx, c[0], "relay-a", "broker down", time.Second, false); err != nil || !ok {
		t.Fatalf("MarkFailed = %v, %v", ok, err)
	}
	if status, _ := outboxStatus(t, pool, ids[0]); status != "RETRYING" {
		t.Fatalf("status = %s, want RETRYING", status)
	}
	if again, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10); len(again) != 0 {
		t.Fatal("row was claimed before its backoff elapsed")
	}

	time.Sleep(1200 * time.Millisecond)
	again, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10)
	if len(again) != 1 || again[0].Attempts != 2 {
		t.Fatalf("claim after backoff = %+v, want one row at attempt 2", again)
	}
}

func TestDeadLetteredRowIsNeverClaimedAgain(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 1)
	ctx := context.Background()

	c, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10)
	if ok, _ := store.MarkFailed(ctx, c[0], "relay-a", "gave up", 0, true); !ok {
		t.Fatal("MarkFailed(dlq) rejected")
	}
	if status, _ := outboxStatus(t, pool, ids[0]); status != "DLQ" {
		t.Fatalf("status = %s, want DLQ", status)
	}
	time.Sleep(100 * time.Millisecond)
	if again, _ := store.Claim(ctx, "relay-a", 30*time.Second, 10); len(again) != 0 {
		t.Fatal("a dead-lettered row was claimed again")
	}
}

// Partial publish: only PENDING deliveries are published (D6 refinement).
func TestPendingChannelsSkipsFinishedDeliveries(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 1)
	ctx := context.Background()

	if _, err := pool.Exec(ctx,
		`UPDATE channel_deliveries SET status = 'DELIVERED' WHERE event_id = $1 AND channel = 'email'`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE channel_deliveries SET status = 'RETRYING' WHERE event_id = $1 AND channel = 'push'`, ids[0]); err != nil {
		t.Fatal(err)
	}

	got, err := store.PendingChannels(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	if chans := got[ids[0]]; len(chans) != 1 || chans[0].Channel != "webhook" {
		t.Fatalf("pending channels = %v, want only [webhook]", chans)
	}
}
