package outbox

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// LatencyWindow is how many of the most recent deliveries feed the live latency tile.
const LatencyWindow = 200

// Summary is the exact state of the system as counted from the database (D9).
type Summary struct {
	// Events counts outbox rows by status (PENDING, PROCESSING, RETRYING, PUBLISHED, DLQ).
	Events map[string]int64 `json:"events"`
	// Channels counts delivery rows by channel, then by status.
	Channels map[string]map[string]int64 `json:"channels"`
	// Latency covers the last LatencyWindow delivered notifications:
	// event created -> delivered, in milliseconds. It is a live view; benchmark
	// numbers are computed offline from complete data.
	Latency struct {
		Count int64   `json:"count"`
		P50Ms float64 `json:"p50_ms"`
		P95Ms float64 `json:"p95_ms"`
	} `json:"latency"`
}

// Backlog counts work that is still waiting: outbox events not yet published, and
// deliveries not yet done per channel. Both queries match the partial indexes (D3),
// so they stay cheap however many finished rows exist.
func (s *Store) Backlog(ctx context.Context) (outboxWaiting int64, deliveries map[string]int64, err error) {
	if err = s.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE status IN ('PENDING', 'RETRYING', 'PROCESSING')`).Scan(&outboxWaiting); err != nil {
		return 0, nil, fmt.Errorf("count outbox backlog: %w", err)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT channel, count(*) FROM channel_deliveries WHERE status IN ('PENDING', 'RETRYING') GROUP BY channel`)
	if err != nil {
		return 0, nil, fmt.Errorf("count delivery backlog: %w", err)
	}
	defer rows.Close()
	deliveries = map[string]int64{}
	for rows.Next() {
		var ch string
		var n int64
		if err := rows.Scan(&ch, &n); err != nil {
			return 0, nil, err
		}
		deliveries[ch] = n
	}
	return outboxWaiting, deliveries, rows.Err()
}

// DuplicateSummary returns how many deliveries completed and how many duplicate
// messages the consumers recognised and ignored (D5).
// With a non-nil eventID the counts cover only that event.
func (s *Store) DuplicateSummary(ctx context.Context, eventID *uuid.UUID) (delivered, ignored int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status = 'DELIVERED'), coalesce(sum(duplicate_count), 0)
		FROM channel_deliveries
		WHERE $1::uuid IS NULL OR event_id = $1`, eventID).Scan(&delivered, &ignored)
	return
}

// Summary counts events and deliveries by state and computes recent latency.
func (s *Store) Summary(ctx context.Context) (*Summary, error) {
	out := &Summary{Events: map[string]int64{}, Channels: map[string]map[string]int64{}}

	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM outbox_events GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count events: %w", err)
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		out.Events[status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `SELECT channel, status, count(*) FROM channel_deliveries GROUP BY channel, status`)
	if err != nil {
		return nil, fmt.Errorf("count deliveries: %w", err)
	}
	for rows.Next() {
		var channel, status string
		var n int64
		if err := rows.Scan(&channel, &status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		if out.Channels[channel] == nil {
			out.Channels[channel] = map[string]int64{}
		}
		out.Channels[channel][status] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	err = s.pool.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(percentile_cont(0.5)  WITHIN GROUP (ORDER BY ms), 0),
		       coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 0)
		FROM (SELECT extract(epoch FROM (d.delivered_at - o.created_at)) * 1000 AS ms
		      FROM channel_deliveries d JOIN outbox_events o ON o.id = d.event_id
		      WHERE d.delivered_at IS NOT NULL
		      ORDER BY d.delivered_at DESC LIMIT $1) recent`, LatencyWindow).
		Scan(&out.Latency.Count, &out.Latency.P50Ms, &out.Latency.P95Ms)
	if err != nil {
		return nil, fmt.Errorf("recent latency: %w", err)
	}
	return out, nil
}
