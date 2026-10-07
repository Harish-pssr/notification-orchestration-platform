package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
)

const apiURL = "http://localhost:8080"

func platformPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), config.Load().PlatformDSN)
	if err != nil {
		t.Fatalf("connect platform db: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func providerPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), config.Load().ProviderDSN)
	if err != nil {
		t.Fatalf("connect provider db: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type apiResponse struct {
	Status   int
	Replayed bool
	Body     map[string]any
}

func postEvent(t *testing.T, path, key string, body any) apiResponse {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, apiURL+path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	out := apiResponse{Status: resp.StatusCode, Replayed: resp.Header.Get("Idempotent-Replayed") == "true"}
	_ = json.Unmarshal(data, &out.Body)
	return out
}

func payment() map[string]any {
	return map[string]any{"account_id": "acct-test", "amount_cents": 4200, "currency": "USD"}
}

// waitDelivered polls until the outbox row is PUBLISHED and every delivery is DELIVERED.
func waitDelivered(t *testing.T, id string, wantChannels int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(apiURL + "/api/events/" + id)
		if err == nil {
			var e struct {
				Status     string `json:"status"`
				Deliveries []struct {
					Channel, Status string
				} `json:"deliveries"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&e)
			resp.Body.Close()
			delivered := 0
			for _, d := range e.Deliveries {
				if d.Status == "DELIVERED" {
					delivered++
				}
			}
			last = fmt.Sprintf("outbox=%s delivered=%d/%d", e.Status, delivered, len(e.Deliveries))
			if e.Status == "PUBLISHED" && len(e.Deliveries) == wantChannels && delivered == wantChannels {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("event %s not fully delivered in time (%s)", id, last)
}

func TestHappyPathPaymentReachesAllChannelsExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	r := postEvent(t, "/api/events/payment-succeeded", uuid.NewString(), payment())
	if r.Status != 201 {
		t.Fatalf("status %d: %v", r.Status, r.Body)
	}
	id := r.Body["event_id"].(string)

	waitDelivered(t, id, 3)

	// The provider ran exactly one effect per channel for this event.
	var effects int
	err := providerPool(t).QueryRow(context.Background(),
		`SELECT count(*) FROM effects WHERE key LIKE $1`, id+":%").Scan(&effects)
	if err != nil || effects != 3 {
		t.Fatalf("provider effects for event = %d (err=%v), want 3", effects, err)
	}
}

func TestRoutingMatchesTable(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	cases := []struct {
		path string
		body map[string]any
		want int
	}{
		{"/api/events/booking-confirmed", map[string]any{"customer_id": "c1", "item": "room-7"}, 1},
		{"/api/events/password-changed", map[string]any{"user_id": "u1"}, 2},
	}
	for _, c := range cases {
		r := postEvent(t, c.path, uuid.NewString(), c.body)
		if r.Status != 201 {
			t.Fatalf("%s: status %d %v", c.path, r.Status, r.Body)
		}
		waitDelivered(t, r.Body["event_id"].(string), c.want)
	}
}

func TestIdempotencyKeySemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	key := uuid.NewString()
	first := postEvent(t, "/api/events/payment-succeeded", key, payment())
	again := postEvent(t, "/api/events/payment-succeeded", key, payment())

	if first.Status != 201 || again.Status != 201 {
		t.Fatalf("statuses %d and %d, want 201 and 201", first.Status, again.Status)
	}
	if first.Replayed || !again.Replayed {
		t.Errorf("replayed flags = %v, %v; want false, true", first.Replayed, again.Replayed)
	}
	if first.Body["event_id"] != again.Body["event_id"] {
		t.Errorf("retry created a different event: %v vs %v", first.Body["event_id"], again.Body["event_id"])
	}

	// Exactly one event and one payment exist for that key.
	var events, payments int
	pool := platformPool(t)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE id = $1`, first.Body["event_id"]).Scan(&events)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE id::text = $1`, first.Body["aggregate_id"]).Scan(&payments)
	if events != 1 || payments != 1 {
		t.Errorf("events=%d payments=%d, want 1 and 1", events, payments)
	}

	// Same key, different body: rejected.
	other := payment()
	other["amount_cents"] = 9999
	if r := postEvent(t, "/api/events/payment-succeeded", key, other); r.Status != 422 {
		t.Errorf("reused key with different body: status %d, want 422", r.Status)
	}
	// Same key, different endpoint: rejected.
	if r := postEvent(t, "/api/events/password-changed", key, map[string]any{"user_id": "u"}); r.Status != 422 {
		t.Errorf("reused key on another endpoint: status %d, want 422", r.Status)
	}
	// Missing key: rejected.
	if r := postEvent(t, "/api/events/payment-succeeded", "", payment()); r.Status != 400 {
		t.Errorf("missing key: status %d, want 400", r.Status)
	}
}

func TestInvalidBodyIsRejectedWithoutSideEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	r := postEvent(t, "/api/events/payment-succeeded", uuid.NewString(),
		map[string]any{"account_id": "a", "amount_cents": -5, "currency": "USD"})
	if r.Status != 400 {
		t.Fatalf("status %d, want 400", r.Status)
	}
	r = postEvent(t, "/api/events/payment-succeeded", uuid.NewString(),
		map[string]any{"account_id": "a", "amount_cents": 5, "currency": "USD", "surprise": 1})
	if r.Status != 400 {
		t.Fatalf("unknown field: status %d, want 400", r.Status)
	}
}

// The central atomicity guarantee: if any part of the transaction fails, nothing
// is committed: no business row, no outbox row, no idempotency record.
func TestEventCreationIsAtomic(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pool := platformPool(t)
	store := outbox.NewStore(pool)
	// A routing table that returns an invalid channel makes the delivery insert
	// fail AFTER the business row and outbox row were written inside the transaction.
	store.Routes = func(events.Type) []events.Channel { return []events.Channel{"carrier-pigeon"} }

	paymentID := uuid.New()
	key := uuid.NewString()
	_, err := store.CreateEvent(context.Background(), outbox.CreateInput{
		Key: key, RequestHash: "h", EventType: events.PaymentSucceeded,
		Write: func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (outbox.Aggregate, error) {
			_, err := tx.Exec(ctx,
				`INSERT INTO payments (id, account_id, amount_cents, currency, status) VALUES ($1, 'a', 1, 'USD', 'SUCCESS')`, paymentID)
			return outbox.Aggregate{Type: "payment", ID: paymentID.String(), Payload: map[string]any{}}, err
		},
	})
	if err == nil {
		t.Fatal("expected the transaction to fail")
	}

	var payments, keys int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM payments WHERE id = $1`, paymentID).Scan(&payments)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM api_idempotency WHERE key = $1`, key).Scan(&keys)
	if payments != 0 || keys != 0 {
		t.Fatalf("rollback incomplete: payments=%d idempotency keys=%d, want 0 and 0", payments, keys)
	}
}

func TestSummaryEndpointCountsStates(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	r := postEvent(t, "/api/events/payment-succeeded", uuid.NewString(), payment())
	waitDelivered(t, r.Body["event_id"].(string), 3)

	resp, err := http.Get(apiURL + "/api/summary")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s struct {
		Events   map[string]int64            `json:"events"`
		Channels map[string]map[string]int64 `json:"channels"`
		Latency  struct {
			Count int64   `json:"count"`
			P50   float64 `json:"p50_ms"`
		} `json:"latency"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	if s.Events["PUBLISHED"] < 1 {
		t.Errorf("PUBLISHED events = %d, want at least 1", s.Events["PUBLISHED"])
	}
	for _, ch := range []string{"email", "push", "webhook"} {
		if s.Channels[ch]["DELIVERED"] < 1 {
			t.Errorf("channel %s delivered = %d, want at least 1", ch, s.Channels[ch]["DELIVERED"])
		}
	}
	if s.Latency.Count < 1 || s.Latency.P50 <= 0 {
		t.Errorf("latency = %+v, want a positive sample", s.Latency)
	}
}
