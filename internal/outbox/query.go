package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventSummary is one row of the dashboard's event list.
type EventSummary struct {
	ID         uuid.UUID         `json:"id"`
	Type       string            `json:"type"`
	Status     string            `json:"status"`
	Attempts   int               `json:"attempts"`
	CreatedAt  time.Time         `json:"created_at"`
	Deliveries map[string]string `json:"deliveries"` // channel -> delivery status
}

// ListEvents returns the newest events first. UUIDv7 ids sort by creation time,
// so ordering by id descending is ordering by age.
func (s *Store) ListEvents(ctx context.Context, limit int) ([]EventSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT o.id, o.event_type, o.status, o.attempts, o.created_at,
		       coalesce(jsonb_object_agg(d.channel, d.status) FILTER (WHERE d.channel IS NOT NULL), '{}')
		FROM (SELECT * FROM outbox_events ORDER BY id DESC LIMIT $1) o
		LEFT JOIN channel_deliveries d ON d.event_id = o.id
		GROUP BY o.id, o.event_type, o.status, o.attempts, o.created_at
		ORDER BY o.id DESC`, limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	out := []EventSummary{}
	for rows.Next() {
		var e EventSummary
		var deliveries []byte
		if err := rows.Scan(&e.ID, &e.Type, &e.Status, &e.Attempts, &e.CreatedAt, &deliveries); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(deliveries, &e.Deliveries); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// DeliveryDetail is one channel's delivery state.
type DeliveryDetail struct {
	Channel     string     `json:"channel"`
	Status      string     `json:"status"`
	Attempts    int        `json:"attempts"`
	LastError   *string    `json:"last_error"`
	DeliveredAt *time.Time `json:"delivered_at"`
	// DuplicateCount is how many extra messages for this delivery the consumer
	// recognised and ignored.
	DuplicateCount int `json:"duplicate_count"`
}

// EventDetail is the full state of one event.
type EventDetail struct {
	ID          uuid.UUID        `json:"id"`
	Type        string           `json:"type"`
	AggregateID string           `json:"aggregate_id"`
	Payload     json.RawMessage  `json:"payload"`
	Status      string           `json:"status"`
	Attempts    int              `json:"attempts"`
	LastError   *string          `json:"last_error"`
	CreatedAt   time.Time        `json:"created_at"`
	PublishedAt *time.Time       `json:"published_at"`
	Deliveries  []DeliveryDetail `json:"deliveries"`
}

// GetEvent returns one event with its deliveries, or (nil, nil) if it does not exist.
func (s *Store) GetEvent(ctx context.Context, id uuid.UUID) (*EventDetail, error) {
	var e EventDetail
	err := s.pool.QueryRow(ctx, `
		SELECT id, event_type, aggregate_id, payload, status, attempts, last_error, created_at, published_at
		FROM outbox_events WHERE id = $1`, id).
		Scan(&e.ID, &e.Type, &e.AggregateID, &e.Payload, &e.Status, &e.Attempts, &e.LastError, &e.CreatedAt, &e.PublishedAt)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get event: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT channel, status, attempts, last_error, delivered_at, duplicate_count
		FROM channel_deliveries WHERE event_id = $1 ORDER BY channel`, id)
	if err != nil {
		return nil, fmt.Errorf("get deliveries: %w", err)
	}
	defer rows.Close()
	e.Deliveries = []DeliveryDetail{}
	for rows.Next() {
		var d DeliveryDetail
		if err := rows.Scan(&d.Channel, &d.Status, &d.Attempts, &d.LastError, &d.DeliveredAt, &d.DuplicateCount); err != nil {
			return nil, err
		}
		e.Deliveries = append(e.Deliveries, d)
	}
	return &e, rows.Err()
}
