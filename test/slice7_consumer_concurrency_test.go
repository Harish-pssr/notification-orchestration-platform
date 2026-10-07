package test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/consumer"
	"notifplatform/internal/events"
)

// ackMsg is a fakeMsg that announces its ack on a channel, so a test can wait for it
// without sharing the plain counters between goroutines.
type ackMsg struct {
	*fakeMsg
	acked chan struct{}
	once  sync.Once
}

func (m *ackMsg) Ack() error {
	m.once.Do(func() { close(m.acked) })
	return nil
}

// queueConsumer stands in for the broker's pull consumer: Fetch hands out up to n queued
// messages. Embedding the interface leaves every other method nil, which is fine because
// the consumer only calls Fetch.
type queueConsumer struct {
	jetstream.Consumer
	mu    sync.Mutex
	queue []jetstream.Msg
	asked []int // the n of every Fetch, to check the fetch size follows the free slots
}

func (q *queueConsumer) Fetch(n int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	q.mu.Lock()
	q.asked = append(q.asked, n)
	take := min(n, len(q.queue))
	got := q.queue[:take]
	q.queue = q.queue[take:]
	q.mu.Unlock()
	if take == 0 {
		time.Sleep(5 * time.Millisecond) // an empty fetch normally waits at the broker
	}
	ch := make(chan jetstream.Msg, take)
	for _, m := range got {
		ch <- m
	}
	close(ch)
	return fakeBatch{ch}, nil
}

type fakeBatch struct{ ch chan jetstream.Msg }

func (b fakeBatch) Messages() <-chan jetstream.Msg { return b.ch }
func (b fakeBatch) Error() error                   { return nil }

// TestSlowMessageDoesNotHoldUpTheOthers is the point of D15: with a worker pool a message
// that takes a long time occupies one slot, and everything else is delivered meanwhile.
func TestSlowMessageDoesNotHoldUpTheOthers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	ids := seedOutbox(t, store, 6)

	slowID := ids[0].String()
	release := make(chan struct{})
	var inFlight, peak atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond) // every call takes a while, so overlap is certain
		// Only the first event's request waits further; the key starts with the event id.
		if key := r.Header.Get("Idempotency-Key"); len(key) >= len(slowID) && key[:len(slowID)] == slowID {
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	q := &queueConsumer{}
	msgs := make([]*ackMsg, len(ids))
	for i, id := range ids {
		msgs[i] = &ackMsg{fakeMsg: newFakeMsg(t, id.String(), events.Email, 1), acked: make(chan struct{})}
		q.queue = append(q.queue, msgs[i])
	}

	c := consumer.New(consumer.Config{
		Name: "test", Channel: events.Email, ProviderURL: srv.URL,
		Batch: 10, Concurrency: 4, FetchWait: 100 * time.Millisecond,
		CallTimeout: 10 * time.Second, RetryDelay: 50 * time.Millisecond,
		MaxAttempts: 5, BackoffBase: 100 * time.Millisecond, BackoffCap: time.Second,
	}, pool, q, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()

	// While the first message is stuck, the other five must all be acknowledged. With a
	// serial loop or a batch barrier they would not be.
	for i := 1; i < len(msgs); i++ {
		select {
		case <-msgs[i].acked:
		case <-time.After(5 * time.Second):
			t.Fatalf("message %d was not delivered while message 0 was slow", i)
		}
	}
	select {
	case <-msgs[0].acked:
		t.Fatal("the slow message finished before it was released")
	default:
	}

	close(release)
	select {
	case <-msgs[0].acked:
	case <-time.After(5 * time.Second):
		t.Fatal("the slow message was never delivered after release")
	}
	cancel()
	<-done

	if p := peak.Load(); p < 2 || p > 4 {
		t.Errorf("peak concurrent provider calls = %d, want between 2 and the limit of 4", p)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, n := range q.asked {
		if n > 4 {
			t.Errorf("a fetch asked for %d messages with only 4 slots", n)
		}
	}
	for _, id := range ids {
		if s := readDelivery(t, pool, id.String(), events.Email); s.status != "DELIVERED" {
			t.Errorf("event %s is %s, want DELIVERED", id, s.status)
		}
	}
}
