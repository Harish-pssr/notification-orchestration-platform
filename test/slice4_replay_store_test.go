package test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
)

// Replay semantics on the Store, in an isolated database (D7).
func TestReplayResetsDeadDeliveriesAndReturnsEventsToTheRelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	ids := seedOutbox(t, store, 3)
	published, inFlight, relayDead := ids[0], ids[1], ids[2]

	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	outboxStatusOf := func(id uuid.UUID) string {
		t.Helper()
		var s string
		if err := pool.QueryRow(ctx, `SELECT status FROM outbox_events WHERE id = $1`, id).Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	// published: the relay finished, the webhook delivery was dead-lettered.
	exec(`UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1`, published)
	exec(`UPDATE channel_deliveries SET status = 'DLQ', attempts = 5, last_error = 'retries exhausted' WHERE event_id = $1 AND channel = 'webhook'`, published)
	// inFlight: a dead delivery whose event the relay has not finished publishing.
	exec(`UPDATE outbox_events SET status = 'PROCESSING' WHERE id = $1`, inFlight)
	exec(`UPDATE channel_deliveries SET status = 'DLQ' WHERE event_id = $1 AND channel = 'webhook'`, inFlight)
	// relayDead: the relay itself gave up on this event.
	exec(`UPDATE outbox_events SET status = 'DLQ', attempts = 5 WHERE id = $1`, relayDead)

	if _, err := store.Replay(ctx, outbox.ReplayInput{}); err == nil {
		t.Error("a replay with no selector must be refused")
	}

	res, err := store.Replay(ctx, outbox.ReplayInput{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deliveries != 1 || res.Skipped != 1 || res.Events != 2 {
		t.Errorf("result = %+v, want 1 delivery reset, 1 skipped (event still publishing), 2 events returned (published + relay-dead)", res)
	}

	// The dead delivery is reset and its event is back with the relay.
	var status string
	var attempts, replays int
	var lastErr *string
	if err := pool.QueryRow(ctx,
		`SELECT status, attempts, replay_count, last_error FROM channel_deliveries WHERE event_id = $1 AND channel = 'webhook'`,
		published).Scan(&status, &attempts, &replays, &lastErr); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || attempts != 0 || replays != 1 || lastErr != nil {
		t.Errorf("replayed delivery = %s attempts=%d replays=%d err=%v, want PENDING 0 1 nil", status, attempts, replays, lastErr)
	}
	if s := outboxStatusOf(published); s != "PENDING" {
		t.Errorf("replayed event has outbox status %s, want PENDING", s)
	}
	if s := outboxStatusOf(relayDead); s != "PENDING" {
		t.Errorf("relay-dead event has outbox status %s after replay, want PENDING", s)
	}

	// The delivery of the unfinished event is untouched and stays dead.
	var skipped string
	if err := pool.QueryRow(ctx, `SELECT status FROM channel_deliveries WHERE event_id = $1 AND channel = 'webhook'`, inFlight).Scan(&skipped); err != nil {
		t.Fatal(err)
	}
	if skipped != "DLQ" {
		t.Errorf("a delivery of an unfinished event was replayed: status %s", skipped)
	}

	// The relay now publishes the replayed delivery with a replay-specific message ID.
	pending, err := store.PendingChannels(ctx, []uuid.UUID{published})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range pending[published] {
		if p.Channel == events.Webhook && p.Replays == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("pending deliveries = %+v, want webhook with Replays=1", pending[published])
	}
}

func TestReplayCanBeNarrowedByChannelOrEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ctx := context.Background()
	ids := seedOutbox(t, store, 2)
	for _, id := range ids {
		for _, sql := range []string{
			`UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1`,
			`UPDATE channel_deliveries SET status = 'DLQ' WHERE event_id = $1 AND channel IN ('webhook', 'push')`,
		} {
			if _, err := pool.Exec(ctx, sql, id); err != nil {
				t.Fatal(err)
			}
		}
	}

	res, err := store.Replay(ctx, outbox.ReplayInput{Channel: "webhook"})
	if err != nil || res.Deliveries != 2 {
		t.Fatalf("channel replay = %+v, %v; want both webhook deliveries", res, err)
	}
	// The first replay handed both events back to the relay (status PENDING), so a
	// replay of their other dead delivery would be skipped until the relay has
	// finished with them. Let it finish.
	for _, id := range ids {
		if _, err := pool.Exec(ctx, `UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
	res, err = store.Replay(ctx, outbox.ReplayInput{EventID: &ids[0]})
	if err != nil || res.Deliveries != 1 {
		t.Fatalf("event replay = %+v, %v; want only that event's remaining push delivery", res, err)
	}

	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_deliveries WHERE status = 'DLQ'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Errorf("%d deliveries still dead, want 1 (the other event's push)", remaining)
	}
}
