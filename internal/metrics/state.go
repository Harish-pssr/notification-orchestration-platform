package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Snapshot is the system's backlog and health at one instant. It is read from
// Postgres and the broker each time Prometheus scrapes (D9), by the API only, so
// running several relays or consumers never double-counts a gauge.
type Snapshot struct {
	OutboxWaiting     int64            // outbox events not yet published
	DeliveriesWaiting map[string]int64 // per channel: PENDING + RETRYING deliveries
	DeliveriesDead    map[string]int64 // per channel: DLQ deliveries
	BrokerStream      uint64           // messages stored in the stream
	BrokerPending     map[string]uint64
	BrokerInflight    map[string]int
	RoleLive          map[string]int // live instances per role
	RoleUp            map[string]bool
	RolePaused        map[string]bool
}

// SnapshotFunc produces a Snapshot.
type SnapshotFunc func(ctx context.Context) (Snapshot, error)

type stateCollector struct {
	fn  SnapshotFunc
	log *slog.Logger

	outbox, stream                   *prometheus.Desc
	waiting, dead, pending, inflight *prometheus.Desc
	live, up, paused                 *prometheus.Desc
	scrapeErrors                     prometheus.Counter
}

// NewStateCollector returns a collector whose gauges are computed on every scrape.
func NewStateCollector(fn SnapshotFunc, log *slog.Logger) prometheus.Collector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(Namespace, "", name), help, labels, nil)
	}
	return &stateCollector{
		fn: fn, log: log,
		outbox:   d("outbox_waiting_events", "Outbox events not yet published (PENDING, PROCESSING, RETRYING)."),
		stream:   d("broker_stream_messages", "Messages stored in the broker stream."),
		waiting:  d("deliveries_waiting", "Deliveries not yet done (PENDING, RETRYING), by channel.", "channel"),
		dead:     d("deliveries_dead", "Dead-lettered deliveries awaiting an operator, by channel.", "channel"),
		pending:  d("broker_pending_messages", "Messages waiting in the broker for a channel's consumer.", "channel"),
		inflight: d("broker_inflight_messages", "Messages delivered to a channel's consumer and not yet acknowledged.", "channel"),
		live:     d("role_live_instances", "Instances of a role with a fresh heartbeat.", "role"),
		up:       d("role_up", "1 when the role is UP (at least one live instance and not all paused).", "role"),
		paused:   d("role_paused", "1 when every live instance of the role is paused.", "role"),
		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "state_scrape_errors_total",
			Help: "Scrapes where the backlog and health snapshot could not be read.",
		}),
	}
}

func (c *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.outbox, c.stream, c.waiting, c.dead, c.pending, c.inflight, c.live, c.up, c.paused} {
		ch <- d
	}
	c.scrapeErrors.Describe(ch)
}

func (c *stateCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snap, err := c.fn(ctx)
	if err != nil {
		// Emit nothing rather than stale or zero values that would look like "all clear".
		c.log.Warn("could not read state for metrics", "error", err)
		c.scrapeErrors.Inc()
		c.scrapeErrors.Collect(ch)
		return
	}
	c.scrapeErrors.Collect(ch)

	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}

	gauge(c.outbox, float64(snap.OutboxWaiting))
	gauge(c.stream, float64(snap.BrokerStream))
	for channel, n := range snap.DeliveriesWaiting {
		gauge(c.waiting, float64(n), channel)
	}
	for channel, n := range snap.DeliveriesDead {
		gauge(c.dead, float64(n), channel)
	}
	for channel, n := range snap.BrokerPending {
		gauge(c.pending, float64(n), channel)
	}
	for channel, n := range snap.BrokerInflight {
		gauge(c.inflight, float64(n), channel)
	}
	for role, n := range snap.RoleLive {
		gauge(c.live, float64(n), role)
		gauge(c.up, b2f(snap.RoleUp[role]), role)
		gauge(c.paused, b2f(snap.RolePaused[role]), role)
	}
}
