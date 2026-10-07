package metrics

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNilMetricsAreSafeNoOps(t *testing.T) {
	var a *API
	var r *Relay
	var c *Consumer
	var p *Provider
	// None of these may panic: components run without metrics in tests.
	a.Created("PaymentSucceeded", time.Millisecond)
	a.Replayed()
	a.Rejected("bad_request")
	r.Claimed(3, time.Millisecond)
	r.PublishDone(time.Millisecond, []string{"email"}, nil)
	r.Finished(time.Millisecond, true)
	r.FenceLost()
	r.DeadLettered()
	c.Received()
	c.Outcome(OutcomeDelivered)
	c.ProviderCall(time.Millisecond, true)
	c.Latency(0.1)
	p.Request("email", ResultExecuted, time.Millisecond)
}

func TestEveryComponentRegistersOnceWithoutClashes(t *testing.T) {
	reg := NewRegistry()
	// MustRegister panics on a duplicate name, so reaching the end is the assertion.
	NewAPI(reg)
	NewRelay(reg)
	NewConsumer(reg, "email")
	NewProvider(reg)
	reg.MustRegister(NewStateCollector(func(context.Context) (Snapshot, error) { return Snapshot{}, nil }, slog.Default()))
}

func TestRelayCountersFollowTheCallsMade(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewRelay(reg)

	m.Claimed(5, time.Millisecond)
	m.PublishDone(2*time.Millisecond, []string{"email", "push", "webhook"}, nil)
	m.PublishDone(2*time.Millisecond, nil, errors.New("broker down"))
	m.Finished(time.Millisecond, true)
	m.Finished(time.Millisecond, false) // fence lost: not counted as published
	m.FenceLost()

	check := func(c prometheus.Collector, want float64, what string) {
		t.Helper()
		if got := testutil.ToFloat64(c); got != want {
			t.Errorf("%s = %v, want %v", what, got, want)
		}
	}
	check(m.claimedEvents, 5, "claimed events")
	check(m.publishedEvts, 1, "published events")
	check(m.failures, 1, "publish failures")
	check(m.fenceLost, 1, "fence lost")
	check(m.messages.WithLabelValues("webhook"), 1, "webhook messages")
}

func TestConsumerOutcomesAreCountedPerChannel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewConsumer(reg, "webhook")
	m.Received()
	m.Received()
	m.Outcome(OutcomeDelivered)
	m.Outcome(OutcomeRetried)
	m.ProviderCall(time.Millisecond, false)

	if got := testutil.ToFloat64(m.received.WithLabelValues("webhook")); got != 2 {
		t.Errorf("received = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.outcomes.WithLabelValues("webhook", OutcomeRetried)); got != 1 {
		t.Errorf("retried = %v, want 1", got)
	}
	m.Latency(-1) // a negative latency (clock oddity) must be ignored, not recorded
	if n := testutil.CollectAndCount(m.latency); n != 0 {
		t.Errorf("a negative latency created %d series, want 0", n)
	}
}

// The guard that keeps D9's promise: no metric may carry an unbounded label such as an
// event id, which would create a time series per event.
func TestOnlyLowCardinalityLabelsAreUsed(t *testing.T) {
	reg := NewRegistry()
	a, r, c, p := NewAPI(reg), NewRelay(reg), NewConsumer(reg, "email"), NewProvider(reg)
	a.Created("PaymentSucceeded", time.Millisecond)
	a.Rejected("key_reuse")
	r.PublishDone(time.Millisecond, []string{"email"}, nil)
	c.Received()
	c.Outcome(OutcomeDelivered)
	c.ProviderCall(time.Millisecond, true)
	c.Latency(0.05)
	p.Request("email", ResultExecuted, time.Millisecond)
	reg.MustRegister(NewStateCollector(func(context.Context) (Snapshot, error) {
		return Snapshot{
			DeliveriesWaiting: map[string]int64{"email": 1}, BrokerPending: map[string]uint64{"email": 1},
			RoleLive: map[string]int{"relay": 1}, RoleUp: map[string]bool{"relay": true},
		}, nil
	}, slog.Default()))

	allowed := map[string]bool{"channel": true, "event_type": true, "outcome": true, "result": true, "reason": true,
		"role": true, "le": true, "quantile": true, "version": true}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if !allowed[l.GetName()] {
					t.Errorf("metric %s has label %q, which is not on the low-cardinality list", f.GetName(), l.GetName())
				}
			}
		}
	}
}

func TestStateCollectorReportsGaugesFromTheSnapshot(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewStateCollector(func(context.Context) (Snapshot, error) {
		return Snapshot{
			OutboxWaiting:     7,
			DeliveriesWaiting: map[string]int64{"webhook": 4},
			DeliveriesDead:    map[string]int64{"webhook": 2},
			BrokerPending:     map[string]uint64{"webhook": 3},
			RoleLive:          map[string]int{"relay": 2, "consumer-webhook": 0},
			RoleUp:            map[string]bool{"relay": true},
			RolePaused:        map[string]bool{},
		}, nil
	}, slog.Default()))

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			key := f.GetName()
			for _, l := range m.GetLabel() {
				key += "{" + l.GetValue() + "}"
			}
			values[key] = m.GetGauge().GetValue()
		}
	}
	want := map[string]float64{
		"notif_outbox_waiting_events":            7,
		"notif_deliveries_waiting{webhook}":      4,
		"notif_deliveries_dead{webhook}":         2,
		"notif_broker_pending_messages{webhook}": 3,
		"notif_role_live_instances{relay}":       2,
		"notif_role_up{relay}":                   1,
		"notif_role_up{consumer-webhook}":        0,
	}
	for k, v := range want {
		if got, ok := values[k]; !ok || got != v {
			t.Errorf("%s = %v (present=%v), want %v", k, got, ok, v)
		}
	}
}

// If the snapshot cannot be read, the collector must say so (an error counter) and emit
// NO gauges: zeros would look like "backlog is empty" exactly when we are blind.
func TestStateCollectorEmitsNoGaugesWhenTheSnapshotFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewStateCollector(func(context.Context) (Snapshot, error) {
		return Snapshot{}, errors.New("database unreachable")
	}, slog.Default()))

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	if names["notif_outbox_waiting_events"] {
		t.Error("a backlog gauge was emitted although the snapshot failed")
	}
	if !names["notif_state_scrape_errors_total"] {
		t.Error("the scrape error counter is missing")
	}
}
