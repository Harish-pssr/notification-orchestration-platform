// Package outbox owns all SQL for outbox_events, channel_deliveries and
// api_idempotency: atomic event creation (D3, D12), claiming (D4) and finishing.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/events"
)

// ErrKeyReuse means an Idempotency-Key was reused with a different request (D12).
var ErrKeyReuse = errors.New("idempotency key reused with a different request")

// Store is the outbox repository.
type Store struct {
	pool *pgxpool.Pool
	// Routes decides which channels an event type is delivered to. It defaults to
	// events.RoutesFor; tests replace it to inject failures.
	Routes func(events.Type) []events.Channel
}

// NewStore returns a Store using the standard routing table.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Routes: events.RoutesFor}
}

// Aggregate describes the business record a BusinessWrite created.
type Aggregate struct {
	Type    string // e.g. "payment"
	ID      string
	Payload any // becomes outbox_events.payload
}

// BusinessWrite performs the business-state change inside the caller's
// transaction. It must use tx, never the pool, or atomicity is lost.
type BusinessWrite func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (Aggregate, error)

// CreateInput is one request to create an event.
type CreateInput struct {
	Key         string // the client's Idempotency-Key
	RequestHash string // hash of method + path + body
	EventType   events.Type
	Write       BusinessWrite
}

// CreateResult is what the API returns for a create (first call or replay).
type CreateResult struct {
	EventID    uuid.UUID
	Replayed   bool
	StatusCode int
	Body       []byte
}

// CreateEvent writes the idempotency record, the business row, the outbox row and
// one delivery row per routed channel in a single transaction: all succeed or none
// do.
func (s *Store) CreateEvent(ctx context.Context, in CreateInput) (CreateResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin: %w", err)
	}
	// Rollback after a successful Commit is a harmless no-op, so this is the
	// standard way to guarantee cleanup on every early return.
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim the key. If another transaction holds the same key, this statement
	// waits for it to finish (Postgres unique-index behaviour), then reports 0 rows.
	tag, err := tx.Exec(ctx,
		`INSERT INTO api_idempotency (key, request_hash) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		in.Key, in.RequestHash)
	if err != nil {
		return CreateResult{}, fmt.Errorf("claim idempotency key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var hash string
		var code *int
		var body []byte
		err := tx.QueryRow(ctx,
			`SELECT request_hash, status_code, response FROM api_idempotency WHERE key = $1`,
			in.Key).Scan(&hash, &code, &body)
		if err != nil {
			return CreateResult{}, fmt.Errorf("read stored response: %w", err)
		}
		if hash != in.RequestHash {
			return CreateResult{}, ErrKeyReuse
		}
		if code == nil {
			// Committed rows are always complete, so this means corruption, not a race.
			return CreateResult{}, fmt.Errorf("idempotency key %q has no stored response", in.Key)
		}
		var stored struct {
			EventID uuid.UUID `json:"event_id"`
		}
		_ = json.Unmarshal(body, &stored)
		return CreateResult{EventID: stored.EventID, Replayed: true, StatusCode: *code, Body: body}, nil
	}

	eventID, err := uuid.NewV7() // time-ordered, see D3
	if err != nil {
		return CreateResult{}, fmt.Errorf("generate event id: %w", err)
	}

	agg, err := in.Write(ctx, tx, eventID)
	if err != nil {
		return CreateResult{}, fmt.Errorf("business write: %w", err)
	}
	payload, err := json.Marshal(agg.Payload)
	if err != nil {
		return CreateResult{}, fmt.Errorf("marshal payload: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload)
		 VALUES ($1, $2, $3, $4, $5)`,
		eventID, string(in.EventType), agg.Type, agg.ID, payload); err != nil {
		return CreateResult{}, fmt.Errorf("insert outbox event: %w", err)
	}

	channels := s.Routes(in.EventType)
	names := make([]string, len(channels))
	for i, c := range channels {
		names[i] = string(c)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_deliveries (event_id, channel) SELECT $1, unnest($2::text[])`,
		eventID, names); err != nil {
		return CreateResult{}, fmt.Errorf("insert deliveries: %w", err)
	}

	const status = 201
	body, err := json.Marshal(map[string]any{
		"event_id":     eventID,
		"event_type":   in.EventType,
		"aggregate_id": agg.ID,
		"channels":     names,
	})
	if err != nil {
		return CreateResult{}, fmt.Errorf("marshal response: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE api_idempotency SET status_code = $2, response = $3 WHERE key = $1`,
		in.Key, status, body); err != nil {
		return CreateResult{}, fmt.Errorf("store response: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, fmt.Errorf("commit: %w", err)
	}
	return CreateResult{EventID: eventID, StatusCode: status, Body: body}, nil
}

// Claimed is an outbox row a worker now owns.
type Claimed struct {
	ID        uuid.UUID
	EventType events.Type
	Attempts  int // after this claim; with the worker name it is the fencing token (D4)
}

// Claim takes up to limit due rows for worker, in one short statement (D4).
//
// FOR UPDATE SKIP LOCKED makes concurrent claimers skip rows another claimer is
// locking instead of waiting. The lock lasts only for this statement; the lease
// is the data written here. Expired leases are claimable because next_attempt_at
// is set to the lease expiry.
func (s *Store) Claim(ctx context.Context, worker string, lease time.Duration, limit int) ([]Claimed, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE outbox_events o
		SET status = 'PROCESSING',
		    locked_by = $1,
		    locked_until = now() + make_interval(secs => $2),
		    next_attempt_at = now() + make_interval(secs => $2),
		    attempts = o.attempts + 1
		WHERE o.id IN (
			SELECT id FROM outbox_events
			WHERE status IN ('PENDING', 'RETRYING', 'PROCESSING')
			  AND next_attempt_at <= now()
			ORDER BY next_attempt_at, id
			LIMIT $3
			FOR UPDATE SKIP LOCKED)
		RETURNING o.id, o.event_type, o.attempts`,
		worker, lease.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	var out []Claimed
	for rows.Next() {
		var c Claimed
		var typ string
		if err := rows.Scan(&c.ID, &typ, &c.Attempts); err != nil {
			return nil, fmt.Errorf("scan claim: %w", err)
		}
		c.EventType = events.Type(typ)
		out = append(out, c)
	}
	return out, rows.Err()
}

// Pending is one delivery the relay still has to publish.
type Pending struct {
	Channel events.Channel
	// Replays is how many times an operator replayed this delivery. It becomes part
	// of the broker message ID so a replay is never mistaken for the original publish.
	Replays int
	// Reconciles is how many times the reconciler republished this delivery (D22). It
	// is part of the message ID for the same reason as Replays.
	Reconciles int
}

// PendingChannels returns, per event, the deliveries that are still PENDING
// (D6: only those are published by the relay).
func (s *Store) PendingChannels(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]Pending, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, channel, replay_count, reconcile_count FROM channel_deliveries
		 WHERE event_id = ANY($1) AND status = 'PENDING' ORDER BY event_id, channel`, ids)
	if err != nil {
		return nil, fmt.Errorf("pending channels: %w", err)
	}
	defer rows.Close()

	out := make(map[uuid.UUID][]Pending)
	for rows.Next() {
		var id uuid.UUID
		var ch string
		var replays, reconciles int
		if err := rows.Scan(&id, &ch, &replays, &reconciles); err != nil {
			return nil, err
		}
		out[id] = append(out[id], Pending{Channel: events.Channel(ch), Replays: replays, Reconciles: reconciles})
	}
	return out, rows.Err()
}

// MarkPublished finishes a claimed row. It reports false when the fence failed:
// the lease was lost to another worker, so this worker must do nothing more.
func (s *Store) MarkPublished(ctx context.Context, c Claimed, worker string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'PUBLISHED', published_at = now(), last_error = NULL,
		    locked_by = NULL, locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'PROCESSING'`,
		c.ID, worker, c.Attempts)
	if err != nil {
		return false, fmt.Errorf("mark published: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkBrokerUnavailable releases a claimed event after a failure that was the broker's
// fault, not the event's (D18): the event goes back to RETRYING and the attempt this
// claim used is given back, so an outage of any length never dead-letters it. It is
// fenced like MarkFailed.
func (s *Store) MarkBrokerUnavailable(ctx context.Context, c Claimed, worker, reason string, retryIn time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'RETRYING', attempts = greatest(attempts - 1, 0), last_error = $4,
		    next_attempt_at = now() + make_interval(secs => $5),
		    locked_by = NULL, locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'PROCESSING'`,
		c.ID, worker, c.Attempts, reason, retryIn.Seconds())
	if err != nil {
		return false, fmt.Errorf("mark broker unavailable: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkFailed records a failed publish: RETRYING after retryIn, or DLQ when dlq is
// true. It is fenced like MarkPublished.
func (s *Store) MarkFailed(ctx context.Context, c Claimed, worker, reason string, retryIn time.Duration, dlq bool) (bool, error) {
	status := "RETRYING"
	if dlq {
		status = "DLQ"
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = $4, last_error = $5,
		    next_attempt_at = now() + make_interval(secs => $6),
		    locked_by = NULL, locked_until = NULL
		WHERE id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'PROCESSING'`,
		c.ID, worker, c.Attempts, status, reason, retryIn.Seconds())
	if err != nil {
		return false, fmt.Errorf("mark failed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
