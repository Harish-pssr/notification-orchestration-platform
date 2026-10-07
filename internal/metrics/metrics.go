// Package metrics defines the Prometheus metrics of every process (D9).
//
// Rules the metrics follow:
//   - Labels are low-cardinality only (channel, event_type, outcome, result, reason,
//     role). Never an event id: that would create a time series per event.
//   - Counters and histograms are recorded by the process that does the work, and
//     Prometheus sums replicas. Backlog gauges are computed in ONE place (the API, at
//     scrape time) so replicas never double-count them.
//   - Histograms here are live and approximate (bucketed). Benchmark percentiles are
//     computed offline from exact database timestamps, never from these.
//   - Every method is safe on a nil receiver, so a component works unchanged with no
//     metrics configured (as in tests).
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric name.
const Namespace = "notif"

// Bucket layouts. Durations of single operations are milliseconds to seconds;
// end-to-end latency can stretch to minutes during an outage.
var (
	opBuckets      = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
)

// NewRegistry returns a registry that already carries the Go runtime and process
// collectors. They expose each process's own CPU and memory, which is how a benchmark
// can show that the bottleneck was the infrastructure and not the Go runtime (D2).
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves reg in the Prometheus text format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Serve runs a small HTTP server exposing only /metrics, for processes (relay,
// consumers) that have no HTTP server of their own. It returns when ctx is cancelled.
func Serve(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler(reg))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server failed", "error", err)
	}
}

// seconds converts a duration to float seconds, the unit Prometheus expects.
func seconds(d time.Duration) float64 { return d.Seconds() }
