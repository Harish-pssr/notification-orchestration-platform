package test

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
	"notifplatform/internal/relay"
)

// publishedEvent seeds one event, takes it through claim and MarkPublished, and
// backdates its publish time, which is exactly what a lost broker message looks like
// from the database: PUBLISHED long ago, deliveries still open.
func publishedEvent(t *testing.T, store *outbox.Store, pool *pgxpool.Pool, age time.Duration) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	id := seedOutbox(t, store, 1)[0]
	got, err := store.Claim(ctx, "w", time.Minute, 1)
	if err != nil || len(got) != 1 || got[0].ID != id {
		t.Fatalf("claim = %v, %v", got, err)
	}
	if ok, err := store.MarkPublished(ctx, got[0], "w"); err != nil || !ok {
		t.Fatalf("mark published = %v, %v", ok, err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE outbox_events SET published_at = now() - make_interval(secs => $2) WHERE id = $1`,
		id, age.Seconds()); err != nil {
		t.Fatal(err)
	}
	return id
}

func reconcileIn(after time.Duration) outbox.ReconcileInput {
	return outbox.ReconcileInput{After: after, MaxRepairs: 3, Batch: 100}
}

// The core of D22: an event the outbox calls PUBLISHED whose deliveries are still open
// long afterwards goes back through the relay, with the delivery attempts reset.
func TestReconcilerReturnsLostDeliveriesToTheRelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	id := publishedEvent(t, store, pool, time.Hour)

	res, err := store.Reconcile(ctx, reconcileIn(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res.Repaired != 3 || res.Events != 1 || res.DeadLettered != 0 {
		t.Fatalf("result = %+v, want 3 deliveries repaired, 1 event returned, none dead-lettered", res)
	}
	if status, attempts := outboxStatus(t, pool, id); status != "PENDING" || attempts != 0 {
		t.Errorf("event = %s with %d attempts, want PENDING with 0 so the relay claims it", status, attempts)
	}
	rows, err := pool.Query(ctx, `SELECT status, reconcile_count, attempts FROM channel_deliveries WHERE event_id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var status string
		var count, attempts int
		if err := rows.Scan(&status, &count, &attempts); err != nil {
			t.Fatal(err)
		}
		n++
		if status != "PENDING" || count != 1 || attempts != 0 {
			t.Errorf("delivery = %s count=%d attempts=%d, want PENDING 1 0", status, count, attempts)
		}
	}
	if n != 3 {
		t.Errorf("saw %d deliveries, want 3", n)
	}

	// The relay now republishes them, and the message carries the repair count so the
	// broker's dedup window cannot drop it.
	pending, err := store.PendingChannels(ctx, []uuid.UUID{id})
	if err != nil {
		t.Fatal(err)
	}
	if len(pending[id]) != 3 || pending[id][0].Reconciles != 1 {
		t.Errorf("pending = %+v, want 3 channels each with Reconciles=1", pending[id])
	}
}

// What must NOT be touched: anything that is healthy or recent, or not the reconciler's business.
func TestReconcilerLeavesHealthyDeliveriesAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()

	recent := publishedEvent(t, store, pool, 30*time.Second) // published a moment ago
	delivered := publishedEvent(t, store, pool, time.Hour)   // old, but all delivered
	retrying := publishedEvent(t, store, pool, time.Hour)    // old, retry planned for the future
	notPublished := seedOutbox(t, store, 1)[0]               // still PENDING in the outbox
	if _, err := pool.Exec(ctx, `UPDATE channel_deliveries SET status='DELIVERED', delivered_at=now() WHERE event_id=$1`, delivered); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE channel_deliveries SET status='RETRYING', next_attempt_at=now()+interval '5 minutes' WHERE event_id=$1`, retrying); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET published_at = NULL WHERE id=$1`, notPublished); err != nil {
		t.Fatal(err)
	}

	res, err := store.Reconcile(ctx, reconcileIn(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res.Repaired != 0 || res.Events != 0 || res.DeadLettered != 0 {
		t.Fatalf("result = %+v, want nothing touched", res)
	}
	for _, id := range []uuid.UUID{recent, delivered, retrying} {
		if status, _ := outboxStatus(t, pool, id); status != "PUBLISHED" {
			t.Errorf("event %s = %s, want it left PUBLISHED", id, status)
		}
	}
	if status, _ := outboxStatus(t, pool, notPublished); status != "PENDING" {
		t.Errorf("unpublished event = %s, want it left to the relay", status)
	}
}

// Each repair doubles the wait before the next one, so a slow-but-alive delivery is
// not republished in a tight loop.
func TestReconcilerWaitsLongerAfterEachRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	id := publishedEvent(t, store, pool, 15*time.Minute)
	if _, err := pool.Exec(ctx, `UPDATE channel_deliveries SET reconcile_count = 1 WHERE event_id = $1`, id); err != nil {
		t.Fatal(err)
	}

	// After one repair the wait is 2 x 10 minutes, so 15 minutes of silence is not enough.
	if res, err := store.Reconcile(ctx, reconcileIn(10*time.Minute)); err != nil || res.Repaired != 0 {
		t.Fatalf("after 15 min with one earlier repair: %+v, %v; want nothing repaired", res, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET published_at = now() - interval '25 minutes' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if res, err := store.Reconcile(ctx, reconcileIn(10*time.Minute)); err != nil || res.Repaired != 3 {
		t.Fatalf("after 25 min: %+v, %v; want 3 repaired", res, err)
	}
}

// The attempt cap: a delivery that keeps getting lost is dead-lettered, visible and
// replayable, instead of being republished forever.
func TestReconcilerGivesUpAfterTheRepairLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	id := publishedEvent(t, store, pool, 24*time.Hour)
	if _, err := pool.Exec(ctx, `UPDATE channel_deliveries SET reconcile_count = 3 WHERE event_id = $1`, id); err != nil {
		t.Fatal(err)
	}

	res, err := store.Reconcile(ctx, reconcileIn(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if res.Repaired != 0 || res.DeadLettered != 3 || res.Events != 0 {
		t.Fatalf("result = %+v, want 3 dead-lettered and nothing returned to the relay", res)
	}
	if status, _ := outboxStatus(t, pool, id); status != "PUBLISHED" {
		t.Errorf("event = %s, want it left PUBLISHED (only its deliveries failed)", status)
	}
	var status, reason string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM channel_deliveries WHERE event_id=$1 LIMIT 1`, id).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "DLQ" || !strings.Contains(reason, "lost by the broker") {
		t.Errorf("delivery = %s %q, want DLQ with a reason that names the loss", status, reason)
	}

	// An operator replay starts the count again.
	if _, err := store.Replay(ctx, outbox.ReplayInput{EventID: &id}); err != nil {
		t.Fatal(err)
	}
	var count int
	_ = pool.QueryRow(ctx, `SELECT max(reconcile_count) FROM channel_deliveries WHERE event_id=$1`, id).Scan(&count)
	if count != 0 {
		t.Errorf("reconcile_count after replay = %d, want 0", count)
	}
}

// Several relays run a reconciler at once. They must take disjoint events: every
// delivery is repaired exactly once, never twice.
func TestConcurrentReconcilersRepairEachDeliveryOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	const nEvents = 40
	for i := 0; i < nEvents; i++ {
		publishedEvent(t, store, pool, time.Hour)
	}

	var wg sync.WaitGroup
	results := make([]outbox.ReconcileResult, 6)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := store.Reconcile(ctx, outbox.ReconcileInput{After: 10 * time.Minute, MaxRepairs: 3, Batch: 7})
			if err != nil {
				t.Errorf("reconcile %d: %v", i, err)
			}
			results[i] = res
		}()
	}
	wg.Wait()

	total := 0
	for _, r := range results {
		total += r.Repaired
	}
	var maxCount, repairedRows int
	_ = pool.QueryRow(ctx, `SELECT coalesce(max(reconcile_count),0), count(*) FILTER (WHERE reconcile_count > 0) FROM channel_deliveries`).Scan(&maxCount, &repairedRows)
	if maxCount != 1 {
		t.Errorf("max reconcile_count = %d, want 1: a delivery was repaired twice", maxCount)
	}
	if total != repairedRows {
		t.Errorf("reported %d repairs but %d rows changed", total, repairedRows)
	}
}

// The republished message gets its own broker message ID. Without it the broker's
// dedup window could drop the repair as a repeat of the lost original.
func TestReconciledMessageHasItsOwnBrokerMessageID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	b := &idRecordingBroker{}
	store, pool := isolatedStore(t)
	r := relay.New(relay.Config{
		Name: "relay-recon-id", Lease: 5 * time.Second, Batch: 10, Poll: 10 * time.Millisecond,
		MaxAttempts: 5, BackoffBase: 10 * time.Millisecond, BackoffCap: 20 * time.Millisecond,
		PublishTimeout: time.Second,
	}, store, b, slog.Default())
	ctx := context.Background()
	id := publishedEvent(t, store, pool, time.Hour)
	if _, err := store.Reconcile(ctx, reconcileIn(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n, err := r.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay RunOnce = %d, %v; want it to claim the returned event", n, err)
	}
	if len(b.ids) != 3 {
		t.Fatalf("published %d messages, want 3", len(b.ids))
	}
	for _, got := range b.ids {
		if !strings.HasPrefix(got, id.String()+":") || !strings.HasSuffix(got, ":recon-1") {
			t.Errorf("message id %q, want <event>:<channel>:recon-1", got)
		}
	}
}

type idRecordingBroker struct {
	fakeBroker
	ids []string
}

func (b *idRecordingBroker) PublishMsgAsync(m *nats.Msg, o ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	b.ids = append(b.ids, m.Header.Get(nats.MsgIdHdr))
	return b.fakeBroker.PublishMsgAsync(m, o...)
}

// The experiment from the action plan: the broker loses messages while deliveries are
// in flight. Consumers are paused so the messages are certainly still in the stream;
// the stream is purged (as if the broker's storage were lost); with nothing else
// happening the deliveries would stay PENDING forever. The reconciler finds them, the
// relay republishes them, and every delivery ends DELIVERED with exactly one provider
// effect per channel.
func TestReconcilerRecoversDeliveriesAfterTheBrokerStreamIsPurged(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	resetFaults(t)
	pool := platformPool(t)
	for _, role := range []string{"consumer-email", "consumer-push", "consumer-webhook"} {
		waitRoleStatus(t, role, "UP", 20*time.Second)
	}

	for _, ch := range events.AllChannels {
		setFault(t, "consumer."+string(ch)+".pause", map[string]any{"action": "pause"})
	}
	for _, role := range []string{"consumer-email", "consumer-push", "consumer-webhook"} {
		waitRoleStatus(t, role, "PAUSED", 15*time.Second)
	}

	const n = 20
	ids := make([]string, n)
	for i := range ids {
		ids[i] = createPayment(t)
	}
	waitFor(t, "every event to be PUBLISHED", 20*time.Second, func() bool {
		var open int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE id::text = ANY($1) AND status <> 'PUBLISHED'`, ids).Scan(&open)
		return open == 0
	})

	// The broker loses everything it holds.
	nc, err := nats.Connect(config.Load().NATSURL)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, broker.StreamName)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx); err != nil {
		t.Fatal(err)
	}

	// Resume the consumers. With the messages gone, nothing arrives.
	clearFaults(t)
	time.Sleep(3 * time.Second)
	for _, id := range ids {
		if s := readDelivery(t, pool, id, events.Email).status; s != "PENDING" {
			t.Fatalf("email delivery = %s after the purge, want PENDING (lost, not delivered): the setup did not lose the message", s)
		}
	}

	// The reconciler, with a wait short enough for a test (production default: 10 minutes).
	rec := relay.NewReconciler(relay.ReconcilerConfig{After: 2 * time.Second, MaxRepairs: 3, Batch: 100},
		outbox.NewStore(pool), slog.Default())
	repaired := 0
	waitFor(t, "the reconciler to repair every lost delivery", 30*time.Second, func() bool {
		repaired += rec.RunOnce(ctx).Repaired
		var open int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM channel_deliveries WHERE event_id::text = ANY($1) AND reconcile_count = 0`, ids).Scan(&open)
		return open == 0
	})
	if repaired < n*3 {
		t.Errorf("reconciler repaired %d deliveries, want at least %d", repaired, n*3)
	}

	for _, id := range ids {
		waitDelivered(t, id, 3)
		for _, ch := range events.AllChannels {
			if requests, effects := providerKeyCounts(t, id, ch); requests != 1 || effects != 1 {
				t.Errorf("event %s %s: provider requests=%d effects=%d, want exactly 1 and 1", id, ch, requests, effects)
			}
		}
	}
}
