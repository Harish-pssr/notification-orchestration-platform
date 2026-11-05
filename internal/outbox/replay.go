package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// DeadDelivery is a channel delivery that exhausted its retries or failed permanently.
type DeadDelivery struct {
	EventID     uuid.UUID `json:"event_id"`
	EventType   string    `json:"event_type"`
	Channel     string    `json:"channel"`
	Attempts    int       `json:"attempts"`
	LastError   *string   `json:"last_error"`
	ReplayCount int       `json:"replay_count"`
	CreatedAt   time.Time `json:"created_at"`
}

// DeadEvent is an outbox event the relay gave up publishing.
type DeadEvent struct {
	ID        uuid.UUID `json:"id"`
	EventType string    `json:"event_type"`
	Attempts  int       `json:"attempts"`
	LastError *string   `json:"last_error"`
	CreatedAt time.Time `json:"created_at"`
}

// DeadLetters is everything an operator can replay.
type DeadLetters struct {
	Deliveries []DeadDelivery `json:"deliveries"`
	Events     []DeadEvent    `json:"events"`
}

// ListDeadLetters returns dead-lettered deliveries and events, newest first.
func (s *Store) ListDeadLetters(ctx context.Context, limit int) (*DeadLetters, error) {
	out := &DeadLetters{Deliveries: []DeadDelivery{}, Events: []DeadEvent{}}

	rows, err := s.pool.Query(ctx, `
		SELECT d.event_id, o.event_type, d.channel, d.attempts, d.last_error, d.replay_count, o.created_at
		FROM channel_deliveries d JOIN outbox_events o ON o.id = d.event_id
		WHERE d.status = 'DLQ'
		ORDER BY d.event_id DESC, d.channel LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead deliveries: %w", err)
	}
	for rows.Next() {
		var d DeadDelivery
		if err := rows.Scan(&d.EventID, &d.EventType, &d.Channel, &d.Attempts, &d.LastError, &d.ReplayCount, &d.CreatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		out.Deliveries = append(out.Deliveries, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `
		SELECT id, event_type, attempts, last_error, created_at
		FROM outbox_events WHERE status = 'DLQ' ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e DeadEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Attempts, &e.LastError, &e.CreatedAt); err != nil {
			return nil, err
		}
		out.Events = append(out.Events, e)
	}
	return out, rows.Err()
}

// ReplayInput selects what to replay. At least one of EventID, Channel or All must
// be set, so an empty request cannot replay everything by accident.
type ReplayInput struct {
	EventID *uuid.UUID // only this event
	Channel string     // only this channel ("" = every channel)
	All     bool       // explicitly replay everything that matches
}

// ReplayResult reports what a replay changed.
type ReplayResult struct {
	Deliveries int `json:"deliveries"` // dead-lettered deliveries put back to PENDING
	Events     int `json:"events"`     // outbox events handed back to the relay
	Skipped    int `json:"skipped"`    // matching dead deliveries whose event is not PUBLISHED yet
}

// Replay puts dead-lettered work back through the normal path (D7): matching dead
// deliveries return to PENDING with their attempts reset, and their events return
// to the relay, which publishes only PENDING deliveries with a fresh message ID.
// Going through the relay means a crash after this transaction loses nothing: the
// rows are simply PENDING and due. Replaying a delivery whose effect already ran
// is harmless because the provider recognises the idempotency key (D5).
func (s *Store) Replay(ctx context.Context, in ReplayInput) (ReplayResult, error) {
	var res ReplayResult
	if in.EventID == nil && in.Channel == "" && !in.All {
		return res, fmt.Errorf("replay needs an event id, a channel, or all=true")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var channel any
	if in.Channel != "" {
		channel = in.Channel
	}

	// Dead deliveries whose event is still being published are left alone: the relay
	// may already have decided which channels to publish. They can be replayed once
	// the event is PUBLISHED.
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM channel_deliveries d JOIN outbox_events o ON o.id = d.event_id
		WHERE d.status = 'DLQ' AND o.status <> 'PUBLISHED'
		  AND ($1::uuid IS NULL OR d.event_id = $1) AND ($2::text IS NULL OR d.channel = $2)`,
		in.EventID, channel).Scan(&res.Skipped); err != nil {
		return res, fmt.Errorf("count skipped: %w", err)
	}

	rows, err := tx.Query(ctx, `
		UPDATE channel_deliveries d
		SET status = 'PENDING', attempts = 0, last_error = NULL, next_attempt_at = NULL,
		    replay_count = d.replay_count + 1, reconcile_count = 0
		FROM outbox_events o
		WHERE o.id = d.event_id AND o.status = 'PUBLISHED' AND d.status = 'DLQ'
		  AND ($1::uuid IS NULL OR d.event_id = $1) AND ($2::text IS NULL OR d.channel = $2)
		RETURNING d.event_id`, in.EventID, channel)
	if err != nil {
		return res, fmt.Errorf("reset deliveries: %w", err)
	}
	var eventIDs []uuid.UUID
	seen := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return res, err
		}
		res.Deliveries++
		if !seen[id] {
			seen[id] = true
			eventIDs = append(eventIDs, id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}

	if len(eventIDs) > 0 {
		tag, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'PENDING', attempts = 0, next_attempt_at = now(), last_error = NULL,
			    locked_by = NULL, locked_until = NULL
			WHERE id = ANY($1)`, eventIDs)
		if err != nil {
			return res, fmt.Errorf("return events to relay: %w", err)
		}
		res.Events += int(tag.RowsAffected())
	}

	// Events the relay itself dead-lettered are replayed only when no channel filter
	// narrows the request, since the filter concerns channel deliveries.
	if in.Channel == "" {
		tag, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET status = 'PENDING', attempts = 0, next_attempt_at = now(), last_error = NULL,
			    locked_by = NULL, locked_until = NULL
			WHERE status = 'DLQ' AND ($1::uuid IS NULL OR id = $1)`, in.EventID)
		if err != nil {
			return res, fmt.Errorf("replay dead events: %w", err)
		}
		res.Events += int(tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return res, err
	}
	return res, nil
}
