package test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/consumer"
	"notifplatform/internal/events"
)

// downDatabase returns a pool pointed at a port where nothing listens, so every query fails at
// once: a database that is down. It needs no running stack.
func downDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://notif:notif@127.0.0.1:1/platform?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func dbOutageConsumer(t *testing.T, cfg consumer.Config) *consumer.Consumer {
	t.Helper()
	cfg.Name, cfg.Channel, cfg.ProviderURL = "test", events.Email, "http://127.0.0.1:1"
	cfg.CallTimeout, cfg.MaxAttempts = time.Second, 5
	cfg.BackoffBase, cfg.BackoffCap = 100*time.Millisecond, 5*time.Second
	return consumer.New(cfg, downDatabase(t), nil, slog.Default())
}

// The Postgres-kill chaos run found that a database outage of about eight seconds made the broker give up
// on in-flight messages: the consumer asked for a redelivery after a fixed second each time, and the
// broker stops after MaxDeliver (MaxAttempts + 3 = 8) deliveries without telling us. The delay after a
// database error must therefore grow with the delivery count, so that all eight deliveries span a long
// outage and not eight seconds of it.
func TestDatabaseErrorRetryDelayGrowsWithEveryDelivery(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: 100 * time.Millisecond, RetryCap: 10 * time.Second})
	id := uuid.NewString()

	delay := func(delivery uint64) time.Duration {
		msg := newFakeMsg(t, id, events.Email, delivery)
		c.Handle(context.Background(), msg)
		if len(msg.naks) != 1 || msg.acks != 0 || msg.terms != 0 {
			t.Fatalf("delivery %d: naks=%v acks=%d terms=%d, want one delayed nak and nothing else", delivery, msg.naks, msg.acks, msg.terms)
		}
		return msg.naks[0]
	}

	// Equal jitter: between half and all of base * 2^(delivery-1), capped at 10 s.
	checks := []struct {
		delivery uint64
		lo, hi   time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{7, 3200 * time.Millisecond, 6400 * time.Millisecond},
		{8, 5 * time.Second, 10 * time.Second}, // 12.8 s nominal, capped
	}
	for _, ck := range checks {
		if d := delay(ck.delivery); d < ck.lo || d > ck.hi {
			t.Errorf("delivery %d: retry delay %v outside [%v, %v]", ck.delivery, d, ck.lo, ck.hi)
		}
	}
}

// With production settings (1 s base, 30 s cap) the seven delays between the eight deliveries add up to at
// least 45 s even with the lowest jitter, so a database outage of that length cannot use up the broker's
// delivery limit.
func TestProductionDefaultsSurviveADatabaseOutageOfFortyFiveSeconds(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: time.Second, RetryCap: 30 * time.Second})
	id := uuid.NewString()
	var shortest time.Duration
	for delivery := uint64(1); delivery <= 7; delivery++ {
		msg := newFakeMsg(t, id, events.Email, delivery)
		c.Handle(context.Background(), msg)
		if len(msg.naks) != 1 {
			t.Fatalf("delivery %d: naks=%v, want one", delivery, msg.naks)
		}
		// Half of the nominal delay is the least equal jitter can return.
		nominal := time.Second << (delivery - 1)
		if nominal > 30*time.Second {
			nominal = 30 * time.Second
		}
		if msg.naks[0] < nominal/2 || msg.naks[0] > nominal {
			t.Errorf("delivery %d: delay %v outside [%v, %v]", delivery, msg.naks[0], nominal/2, nominal)
		}
		shortest += nominal / 2
	}
	if shortest < 45*time.Second {
		t.Errorf("the guaranteed outage tolerance is %v, want at least 45s", shortest)
	}
}

func TestDatabaseErrorRetryDelayStaysFixedWhenNoCapIsConfigured(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{RetryDelay: 100 * time.Millisecond}) // RetryCap left at zero
	msg := newFakeMsg(t, uuid.NewString(), events.Email, 6)
	c.Handle(context.Background(), msg)
	if len(msg.naks) != 1 || msg.naks[0] < 50*time.Millisecond || msg.naks[0] > 100*time.Millisecond {
		t.Errorf("naks = %v, want one delay of at most the fixed RetryDelay (100ms)", msg.naks)
	}
}

// While the database is down the consumer must HOLD the message (telling the broker "still working")
// and not give it back after every failed query: each redelivery counts as a delivery attempt, which
// in the Postgres-kill chaos run first made the broker drop messages (MaxDeliver) and then
// dead-lettered a healthy delivery as "retries exhausted". If the outage outlasts DBWait the message
// is given back once, as before.
func TestMessageIsHeldWhileTheDatabaseIsDownThenGivenBackAfterDBWait(t *testing.T) {
	c := dbOutageConsumer(t, consumer.Config{
		RetryDelay: 50 * time.Millisecond, RetryCap: 100 * time.Millisecond, DBWait: 500 * time.Millisecond,
	})
	msg := newFakeMsg(t, uuid.NewString(), events.Email, 1)
	start := time.Now()
	c.Handle(context.Background(), msg)
	elapsed := time.Since(start)

	if elapsed < 450*time.Millisecond {
		t.Errorf("the consumer gave the message back after %v, want it held for about DBWait (500ms)", elapsed)
	}
	if msg.working < 2 {
		t.Errorf("the consumer said \"still working\" %d times, want it to keep telling the broker while it holds the message", msg.working)
	}
	if len(msg.naks) != 1 || msg.acks != 0 || msg.terms != 0 {
		t.Errorf("naks=%v acks=%d terms=%d, want the message given back exactly once after DBWait", msg.naks, msg.acks, msg.terms)
	}
}

// flakyProxy sits between the consumer and Postgres. Down closes every connection and refuses new
// ones, like a database that was killed; Up lets traffic through again.
type flakyProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	down   bool
	conns  map[net.Conn]struct{}
}

func newFlakyProxy(t *testing.T, target string) *flakyProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &flakyProxy{ln: ln, target: target, conns: map[net.Conn]struct{}{}}
	t.Cleanup(func() { _ = ln.Close(); p.setDown(true) })
	go p.serve()
	return p
}

func (p *flakyProxy) port() int { return p.ln.Addr().(*net.TCPAddr).Port }

func (p *flakyProxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for c := range p.conns {
			_ = c.Close()
		}
		p.conns = map[net.Conn]struct{}{}
	}
}

func (p *flakyProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.down {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.conns[client], p.conns[upstream] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close(); _ = client.Close() }()
		go func() { _, _ = io.Copy(client, upstream); _ = upstream.Close(); _ = client.Close() }()
	}
}

// A database outage shorter than DBWait must cost the delivery nothing: the consumer holds the message,
// the database comes back, and the delivery is completed as the FIRST attempt, acknowledged, never
// given back to the broker (so no delivery attempt was spent on the outage).
func TestAnOutageShorterThanDBWaitCostsNoDeliveryAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	store, pool := isolatedStore(t)
	id := seedOutbox(t, store, 1)[0].String()

	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer provider.Close()

	cfg := pool.Config().Copy()
	proxy := newFlakyProxy(t, net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))))
	cfg.ConnConfig.Host, cfg.ConnConfig.Port = "127.0.0.1", uint16(proxy.port())
	cfg.ConnConfig.ConnectTimeout = time.Second
	viaProxy, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool via proxy: %v", err)
	}
	t.Cleanup(viaProxy.Close)

	c := consumer.New(consumer.Config{
		Name: "test", Channel: events.Email, ProviderURL: provider.URL, CallTimeout: 2 * time.Second,
		RetryDelay: 50 * time.Millisecond, RetryCap: 200 * time.Millisecond, DBWait: 10 * time.Second,
		MaxAttempts: 5, BackoffBase: 100 * time.Millisecond, BackoffCap: 5 * time.Second,
	}, viaProxy, nil, slog.Default())

	proxy.setDown(true) // the database is gone
	msg := newFakeMsg(t, id, events.Email, 1)
	done := make(chan struct{})
	go func() { defer close(done); c.Handle(context.Background(), msg) }()

	time.Sleep(800 * time.Millisecond) // an outage of nearly a second
	proxy.setDown(false)

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the consumer did not finish after the database came back")
	}
	if msg.acks != 1 || len(msg.naks) != 0 || msg.terms != 0 {
		t.Errorf("acks=%d naks=%v terms=%d, want the message acknowledged and never given back", msg.acks, msg.naks, msg.terms)
	}
	if msg.working < 1 {
		t.Errorf("the consumer never said \"still working\" while holding the message")
	}
	s := readDelivery(t, pool, id, events.Email)
	if s.status != "DELIVERED" || s.attempts != 1 {
		t.Errorf("delivery = %+v, want DELIVERED as attempt 1: the outage must not cost an attempt", s)
	}
}
