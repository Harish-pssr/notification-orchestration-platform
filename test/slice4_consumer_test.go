package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/consumer"
	"notifplatform/internal/events"
)

// fakeMsg is a broker message that records what the consumer did with it, so the
// retry / dead-letter decisions can be tested without a broker or real waiting.
type fakeMsg struct {
	data      []byte
	delivered uint64
	acks      int
	terms     int
	naks      []time.Duration // NakWithDelay delays; a plain Nak records 0
}

func newFakeMsg(t *testing.T, eventID string, ch events.Channel, delivered uint64) *fakeMsg {
	t.Helper()
	body := map[string]any{"event_id": eventID, "channel": ch, "event_type": events.PaymentSucceeded}
	raw, _ := json.Marshal(body)
	return &fakeMsg{data: raw, delivered: delivered}
}

func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivered}, nil
}
func (m *fakeMsg) Data() []byte                       { return m.data }
func (m *fakeMsg) Headers() nats.Header               { return nats.Header{} }
func (m *fakeMsg) Subject() string                    { return "notify.email" }
func (m *fakeMsg) Reply() string                      { return "" }
func (m *fakeMsg) Ack() error                         { m.acks++; return nil }
func (m *fakeMsg) DoubleAck(context.Context) error    { m.acks++; return nil }
func (m *fakeMsg) Nak() error                         { m.naks = append(m.naks, 0); return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error { m.naks = append(m.naks, d); return nil }
func (m *fakeMsg) InProgress() error                  { return nil }
func (m *fakeMsg) Term() error                        { m.terms++; return nil }
func (m *fakeMsg) TermWithReason(string) error        { m.terms++; return nil }

type deliveryState struct {
	status    string
	attempts  int
	lastError string
}

func readDelivery(t *testing.T, pool *pgxpool.Pool, eventID string, ch events.Channel) deliveryState {
	t.Helper()
	var s deliveryState
	var lastErr *string
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts, last_error FROM channel_deliveries WHERE event_id = $1 AND channel = $2`,
		eventID, string(ch)).Scan(&s.status, &s.attempts, &lastErr)
	if err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	if lastErr != nil {
		s.lastError = *lastErr
	}
	return s
}

// consumerFixture builds a consumer on an isolated database pointing at a fake provider
// that answers every call with the given status (and optional Retry-After).
func consumerFixture(t *testing.T, providerStatus int, retryAfter string, calls *atomic.Int64) (*consumer.Consumer, *pgxpool.Pool, string) {
	t.Helper()
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(providerStatus)
	}))
	t.Cleanup(srv.Close)

	c := consumer.New(consumer.Config{
		Name: "test", Channel: events.Email, ProviderURL: srv.URL,
		CallTimeout: 2 * time.Second, RetryDelay: 50 * time.Millisecond,
		MaxAttempts: 5, BackoffBase: 100 * time.Millisecond, BackoffCap: 5 * time.Second,
	}, pool, nil, slog.Default())
	return c, pool, ids[0].String()
}

func TestTransientErrorSchedulesARetryWithinTheJitterRange(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, pool, id := consumerFixture(t, 500, "", &calls)

	// Attempt 2: nominal delay is base * 2 = 200ms, equal jitter gives [100ms, 200ms].
	msg := newFakeMsg(t, id, events.Email, 2)
	c.Handle(context.Background(), msg)

	if len(msg.naks) != 1 || msg.terms != 0 || msg.acks != 0 {
		t.Fatalf("naks=%v terms=%d acks=%d, want exactly one delayed nak", msg.naks, msg.terms, msg.acks)
	}
	if d := msg.naks[0]; d < 100*time.Millisecond || d > 200*time.Millisecond {
		t.Errorf("retry delay %v outside the equal-jitter range [100ms, 200ms]", d)
	}
	s := readDelivery(t, pool, id, events.Email)
	if s.status != "RETRYING" || s.attempts != 2 || !strings.Contains(s.lastError, "500") {
		t.Errorf("delivery = %+v, want RETRYING at attempt 2 mentioning 500", s)
	}
}

func TestPermanentErrorIsDeadLetteredWithoutRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, pool, id := consumerFixture(t, 400, "", &calls)

	msg := newFakeMsg(t, id, events.Email, 1)
	c.Handle(context.Background(), msg)

	if msg.terms != 1 || len(msg.naks) != 0 {
		t.Fatalf("terms=%d naks=%v, want the message terminated, not retried", msg.terms, msg.naks)
	}
	s := readDelivery(t, pool, id, events.Email)
	if s.status != "DLQ" || s.attempts != 1 || !strings.Contains(s.lastError, "permanent") {
		t.Errorf("delivery = %+v, want DLQ at attempt 1 with a permanent-error reason", s)
	}
	if calls.Load() != 1 {
		t.Errorf("provider called %d times, want 1", calls.Load())
	}
}

func TestExhaustedRetriesAreDeadLettered(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, pool, id := consumerFixture(t, 503, "", &calls)

	msg := newFakeMsg(t, id, events.Email, 5) // the last allowed attempt
	c.Handle(context.Background(), msg)

	if msg.terms != 1 || len(msg.naks) != 0 {
		t.Fatalf("terms=%d naks=%v, want terminated", msg.terms, msg.naks)
	}
	s := readDelivery(t, pool, id, events.Email)
	if s.status != "DLQ" || s.attempts != 5 || !strings.Contains(s.lastError, "retries exhausted") {
		t.Errorf("delivery = %+v, want DLQ at attempt 5, retries exhausted", s)
	}
}

func TestRetryAfterHeaderLengthensTheDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, _, id := consumerFixture(t, 429, "3", &calls)

	msg := newFakeMsg(t, id, events.Email, 1) // our own backoff would be at most 100ms
	c.Handle(context.Background(), msg)

	if len(msg.naks) != 1 || msg.naks[0] != 3*time.Second {
		t.Fatalf("naks = %v, want one nak of exactly the 3s Retry-After", msg.naks)
	}
}

// The poison guard (D11): a message delivered more times than we allow means earlier
// attempts died before recording anything. It must be dead-lettered WITHOUT calling the
// provider again.
func TestPoisonGuardDeadLettersWithoutCallingTheProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, pool, id := consumerFixture(t, 200, "", &calls) // a provider that would succeed

	msg := newFakeMsg(t, id, events.Email, 6) // sixth delivery, max is 5
	c.Handle(context.Background(), msg)

	if calls.Load() != 0 {
		t.Fatalf("provider was called %d times; the guard must act before any call", calls.Load())
	}
	if msg.terms != 1 {
		t.Errorf("terms = %d, want 1", msg.terms)
	}
	s := readDelivery(t, pool, id, events.Email)
	if s.status != "DLQ" || !strings.Contains(s.lastError, "exceeded max deliveries") {
		t.Errorf("delivery = %+v, want DLQ with the max-deliveries reason", s)
	}
}

func TestSuccessfulCallMarksDeliveredAndAcks(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	var calls atomic.Int64
	c, pool, id := consumerFixture(t, 200, "", &calls)

	msg := newFakeMsg(t, id, events.Email, 1)
	c.Handle(context.Background(), msg)

	if msg.acks != 1 || msg.terms != 0 || len(msg.naks) != 0 {
		t.Fatalf("acks=%d terms=%d naks=%v", msg.acks, msg.terms, msg.naks)
	}
	if s := readDelivery(t, pool, id, events.Email); s.status != "DELIVERED" || s.attempts != 1 {
		t.Errorf("delivery = %+v, want DELIVERED at attempt 1", s)
	}
}
