package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ReconcileInput tunes one reconciler pass (D22).
type ReconcileInput struct {
	// After is how long an open delivery may sit without any sign of life before it
	// counts as lost. It must be longer than the longest legitimate retry window
	// (attempts x backoff cap + AckWait) and longer than the broker's dedup window.
	// Each repair doubles the wait for the next one (up to 16x).
	After time.Duration
	// MaxRepairs is how many times one delivery is republished before the reconciler
	// gives up and dead-letters it (an operator can then replay it).
	MaxRepairs int
	// Batch is the most events handled in one pass.
	Batch int
}

// ReconcileResult reports what one pass changed.
type ReconcileResult struct {
	Repaired     int // deliveries returned to PENDING for the relay to republish
	Events       int // events handed back to the relay
	DeadLettered int // deliveries that used up MaxRepairs
}

// stuckPredicate is the definition of "lost": the delivery is still open although
// its event was published, and nothing has happened to it for longer than the
// (growing) wait. Signs of life are the event's last publish and the consumer's
// planned retry time, whichever is later. $1 is After in seconds.
const stuckPredicate = `
	d.status IN ('PENDING', 'RETRYING') AND o.status = 'PUBLISHED'
	AND greatest(o.published_at, coalesce(d.next_attempt_at, o.published_at))
	    + make_interval(secs => $1 * power(2, least(d.reconcile_count, 4))) < now()`

// Reconcile finds deliveries that look lost by the broker and puts them back through
// the normal path (D22). It republishes nothing itself: like Replay it only changes
// rows (stuck deliveries return to PENDING with their attempts reset, their events
// return to the relay), so a crash at any point loses nothing and the relay does the
// publishing with the usual lease and fencing. Repairing a delivery that was merely
// slow is harmless: the consumer ignores a DELIVERED delivery and the provider
// recognises the idempotency key (D5).
//
// Safe to run from several relays at once: events are locked with SKIP LOCKED, so
// concurrent passes take disjoint sets.
func (s *Store) Reconcile(ctx context.Context, in ReconcileInput) (ReconcileResult, error) {
	var res ReconcileResult
	if in.After <= 0 || in.MaxRepairs <= 0 || in.Batch <= 0 {
		return res, fmt.Errorf("reconcile needs positive After, MaxRepairs and Batch")
	}
	after := in.After.Seconds()

	// Cheap look first, with no locks: most passes find nothing. The join starts from
	// the partial index of open deliveries, so published rows are never scanned.
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT d.event_id
		FROM channel_deliveries d JOIN outbox_events o ON o.id = d.event_id
		WHERE `+stuckPredicate+`
		LIMIT $2`, after, in.Batch)
	if err != nil {
		return res, fmt.Errorf("find stuck deliveries: %w", err)
	}
	var candidates []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return res, err
		}
		candidates = append(candidates, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}
	if len(candidates) == 0 {
		return res, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock the events (in id order, skipping ones another pass or a replay holds), then
	// re-check the predicate under the lock: a consumer may have delivered meanwhile.
	lockRows, err := tx.Query(ctx, `
		SELECT id FROM outbox_events
		WHERE id = ANY($1) AND status = 'PUBLISHED'
		ORDER BY id FOR UPDATE SKIP LOCKED`, candidates)
	if err != nil {
		return res, fmt.Errorf("lock events: %w", err)
	}
	var locked []uuid.UUID
	for lockRows.Next() {
		var id uuid.UUID
		if err := lockRows.Scan(&id); err != nil {
			lockRows.Close()
			return res, err
		}
		locked = append(locked, id)
	}
	lockRows.Close()
	if err := lockRows.Err(); err != nil {
		return res, err
	}
	if len(locked) == 0 {
		return res, nil
	}

	// Out of repairs: stop looping and make the loss visible and replayable. This runs
	// before the repair step so a delivery just repaired is not judged by its new count.
	tag, err := tx.Exec(ctx, `
		UPDATE channel_deliveries d
		SET status = 'DLQ', next_attempt_at = NULL,
		    last_error = 'lost by the broker: the reconciler republished it ' || d.reconcile_count || ' times without success'
		FROM outbox_events o
		WHERE o.id = d.event_id AND d.event_id = ANY($3) AND d.reconcile_count >= $2
		  AND `+stuckPredicate, after, in.MaxRepairs, locked)
	if err != nil {
		return res, fmt.Errorf("dead-letter exhausted deliveries: %w", err)
	}
	res.DeadLettered = int(tag.RowsAffected())

	repaired, err := tx.Query(ctx, `
		UPDATE channel_deliveries d
		SET status = 'PENDING', attempts = 0, last_error = NULL, next_attempt_at = NULL,
		    reconcile_count = d.reconcile_count + 1
		FROM outbox_events o
		WHERE o.id = d.event_id AND d.event_id = ANY($3) AND d.reconcile_count < $2
		  AND `+stuckPredicate+`
		RETURNING d.event_id`, after, in.MaxRepairs, locked)
	if err != nil {
		return res, fmt.Errorf("repair deliveries: %w", err)
	}
	seen := map[uuid.UUID]bool{}
	var repairedEvents []uuid.UUID
	for repaired.Next() {
		var id uuid.UUID
		if err := repaired.Scan(&id); err != nil {
			repaired.Close()
			return res, err
		}
		res.Repaired++
		if !seen[id] {
			seen[id] = true
			repairedEvents = append(repairedEvents, id)
		}
	}
	repaired.Close()
	if err := repaired.Err(); err != nil {
		return res, err
	}

	if len(repairedEvents) > 0 {
		tag, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'PENDING', attempts = 0, next_attempt_at = now(), last_error = NULL,
			    locked_by = NULL, locked_until = NULL
			WHERE id = ANY($1)`, repairedEvents)
		if err != nil {
			return res, fmt.Errorf("return events to relay: %w", err)
		}
		res.Events = int(tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}
