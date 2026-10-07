package bench

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Percentiles holds the percentiles of one latency series, in milliseconds.
type Percentiles struct {
	N   int64   `json:"n"`
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

// Analysis is the exact, database-clock result for one run (D13). Every event and
// delivery in the measurement window is included; nothing is sampled or bucketed.
type Analysis struct {
	Run            string                 `json:"run"`
	Events         int64                  `json:"events"`           // all events of the run
	InWindow       int64                  `json:"events_in_window"` // created inside the measurement window
	Completed      int64                  `json:"completed_in_window"`
	Incomplete     int64                  `json:"incomplete_in_window"`
	DeadLettered   int64                  `json:"dead_lettered_deliveries"`
	WarmupS        float64                `json:"warmup_s"`
	WindowS        float64                `json:"window_s"`
	CommitRatePerS float64                `json:"events_per_s_in_window"`
	EndToEnd       Percentiles            `json:"end_to_end"`  // created to the LAST delivery of the event
	Publish        Percentiles            `json:"publish"`     // created to the relay recording PUBLISHED
	Channels       map[string]Percentiles `json:"per_channel"` // created to that channel's delivery
}

// AnalyzeConfig selects the run and the measurement window.
type AnalyzeConfig struct {
	Run    string
	Warmup time.Duration // events created in this period after the run's first are discarded
	Window time.Duration // measurement window length
}

// scanPercentiles runs a query returning (count, array[p50, p95, p99, max]).
func scanPercentiles(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) (Percentiles, error) {
	var p Percentiles
	var vals []float64
	if err := pool.QueryRow(ctx, query, args...).Scan(&p.N, &vals); err != nil {
		return p, err
	}
	if len(vals) == 4 {
		p.P50, p.P95, p.P99, p.Max = vals[0], vals[1], vals[2], vals[3]
	}
	return p, nil
}

// The window is defined on the database clock, relative to the run's first event:
// events created in [first + warmup, first + warmup + window).
const windowCTE = `
	WITH ev AS (SELECT id, created_at FROM outbox_events WHERE payload->>'account_id' = $1),
	     w  AS (SELECT min(created_at) + make_interval(secs => $2) AS lo,
	                   min(created_at) + make_interval(secs => $2 + $3) AS hi FROM ev),
	     inwin AS (SELECT ev.id, ev.created_at FROM ev, w WHERE ev.created_at >= w.lo AND ev.created_at < w.hi)`

const pctExpr = `coalesce(array(SELECT x FROM unnest(
	percentile_cont(ARRAY[0.5, 0.95, 0.99, 1.0]) WITHIN GROUP (ORDER BY %s)) x), '{}')`

// Analyze computes exact latencies for one run from database timestamps.
func Analyze(ctx context.Context, pool *pgxpool.Pool, cfg AnalyzeConfig) (Analysis, error) {
	a := Analysis{Run: cfg.Run, Channels: map[string]Percentiles{}, WarmupS: cfg.Warmup.Seconds(), WindowS: cfg.Window.Seconds()}
	w, wd := cfg.Warmup.Seconds(), cfg.Window.Seconds()

	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'account_id' = $1`, cfg.Run).Scan(&a.Events); err != nil {
		return a, err
	}
	if a.Events == 0 {
		return a, fmt.Errorf("no events found for run %q", cfg.Run)
	}
	err := pool.QueryRow(ctx, windowCTE+`
		SELECT (SELECT count(*) FROM inwin),
		       (SELECT count(*) FROM inwin i WHERE NOT EXISTS
		            (SELECT 1 FROM channel_deliveries d WHERE d.event_id = i.id AND d.status <> 'DELIVERED')),
		       (SELECT count(*) FROM channel_deliveries d JOIN ev ON ev.id = d.event_id WHERE d.status = 'DLQ')`,
		cfg.Run, w, wd).Scan(&a.InWindow, &a.Completed, &a.DeadLettered)
	if err != nil {
		return a, fmt.Errorf("count window: %w", err)
	}
	a.Incomplete = a.InWindow - a.Completed
	if wd > 0 {
		a.CommitRatePerS = float64(a.InWindow) / wd
	}

	// End to end: created to the LAST delivery of the event, for events fully delivered.
	if a.EndToEnd, err = scanPercentiles(ctx, pool, windowCTE+`
		, done AS (
		    SELECT i.created_at, max(d.delivered_at) AS last_at
		    FROM inwin i JOIN channel_deliveries d ON d.event_id = i.id
		    GROUP BY i.id, i.created_at
		    HAVING bool_and(d.status = 'DELIVERED'))
		SELECT count(*), `+fmt.Sprintf(pctExpr, "extract(epoch FROM (last_at - created_at)) * 1000")+`
		FROM done`, cfg.Run, w, wd); err != nil {
		return a, fmt.Errorf("end-to-end: %w", err)
	}

	// Relay stage: created to published.
	if a.Publish, err = scanPercentiles(ctx, pool, windowCTE+`
		, pub AS (SELECT o.published_at - i.created_at AS dt FROM inwin i
		          JOIN outbox_events o ON o.id = i.id WHERE o.published_at IS NOT NULL)
		SELECT count(*), `+fmt.Sprintf(pctExpr, "extract(epoch FROM dt) * 1000")+`
		FROM pub`, cfg.Run, w, wd); err != nil {
		return a, fmt.Errorf("publish latency: %w", err)
	}

	for _, ch := range []string{"email", "push", "webhook"} {
		p, err := scanPercentiles(ctx, pool, windowCTE+`
			, one AS (SELECT d.delivered_at - i.created_at AS dt FROM inwin i
			          JOIN channel_deliveries d ON d.event_id = i.id AND d.channel = $4 AND d.status = 'DELIVERED')
			SELECT count(*), `+fmt.Sprintf(pctExpr, "extract(epoch FROM dt) * 1000")+`
			FROM one`, cfg.Run, w, wd, ch)
		if err != nil {
			return a, fmt.Errorf("channel %s: %w", ch, err)
		}
		a.Channels[ch] = p
	}
	return a, nil
}

// FormatAnalysis renders an Analysis for a person.
func FormatAnalysis(a Analysis) string {
	out := fmt.Sprintf("run %s: %d events; window %.0fs after %.0fs warm-up holds %d (%d completed, %d incomplete, %d dead-lettered deliveries)\n",
		a.Run, a.Events, a.WindowS, a.WarmupS, a.InWindow, a.Completed, a.Incomplete, a.DeadLettered)
	line := func(name string, p Percentiles) {
		out += fmt.Sprintf("  %-22s n=%-7d p50 %8.1f ms  p95 %8.1f ms  p99 %8.1f ms  max %8.1f ms\n", name, p.N, p.P50, p.P95, p.P99, p.Max)
	}
	line("end-to-end (all ch.)", a.EndToEnd)
	line("created -> published", a.Publish)
	for _, ch := range []string{"email", "push", "webhook"} {
		line("created -> "+ch, a.Channels[ch])
	}
	return out
}

// Backlog is the unfinished work at one instant, counted exactly from the database.
type Backlog struct {
	Events     int64 `json:"events"`     // events with at least one delivery not yet done
	Deliveries int64 `json:"deliveries"` // deliveries PENDING or RETRYING
	Dead       int64 `json:"dead"`       // deliveries dead-lettered
}

// ReadBacklog counts unfinished work. Every delivery row is created with its event, in
// PENDING, so "events with an open delivery" is exactly "events not fully delivered".
func ReadBacklog(ctx context.Context, pool *pgxpool.Pool) (Backlog, error) {
	var b Backlog
	err := pool.QueryRow(ctx, `
		SELECT (SELECT count(DISTINCT event_id) FROM channel_deliveries WHERE status IN ('PENDING', 'RETRYING')),
		       (SELECT count(*) FROM channel_deliveries WHERE status IN ('PENDING', 'RETRYING')),
		       (SELECT count(*) FROM channel_deliveries WHERE status = 'DLQ')`).Scan(&b.Events, &b.Deliveries, &b.Dead)
	return b, err
}

// Monitor samples exact counts once per interval into a CSV until ctx ends, so backlog
// growth and recovery can be read off afterwards (D13). Timestamps are database time.
func Monitor(ctx context.Context, pool *pgxpool.Pool, path string, interval time.Duration) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"db_epoch_s", "events", "published", "outbox_waiting", "deliveries_delivered", "deliveries_waiting", "deliveries_dead",
		"outbox_live_tup", "outbox_dead_tup", "outbox_autovacuums", "outbox_autoanalyzes"})

	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		var epoch float64
		var events, published, waiting, delivered, dWaiting, dead int64
		var live, deadTup, autovac, autoanalyze int64
		qctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := pool.QueryRow(qctx, `
			SELECT extract(epoch FROM clock_timestamp()),
			       (SELECT count(*) FROM outbox_events),
			       (SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'),
			       (SELECT count(*) FROM outbox_events WHERE status IN ('PENDING', 'RETRYING', 'PROCESSING')),
			       (SELECT count(*) FROM channel_deliveries WHERE status = 'DELIVERED'),
			       (SELECT count(*) FROM channel_deliveries WHERE status IN ('PENDING', 'RETRYING')),
			       (SELECT count(*) FROM channel_deliveries WHERE status = 'DLQ'),
			       coalesce((SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'outbox_events'), 0),
			       coalesce((SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'outbox_events'), 0),
			       coalesce((SELECT autovacuum_count FROM pg_stat_user_tables WHERE relname = 'outbox_events'), 0),
			       coalesce((SELECT autoanalyze_count FROM pg_stat_user_tables WHERE relname = 'outbox_events'), 0)`).
			Scan(&epoch, &events, &published, &waiting, &delivered, &dWaiting, &dead, &live, &deadTup, &autovac, &autoanalyze)
		cancel()
		if err == nil {
			_ = w.Write([]string{strconv.FormatFloat(epoch, 'f', 3, 64), strconv.FormatInt(events, 10), strconv.FormatInt(published, 10),
				strconv.FormatInt(waiting, 10), strconv.FormatInt(delivered, 10), strconv.FormatInt(dWaiting, 10), strconv.FormatInt(dead, 10),
				strconv.FormatInt(live, 10), strconv.FormatInt(deadTup, 10), strconv.FormatInt(autovac, 10), strconv.FormatInt(autoanalyze, 10)})
			w.Flush()
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}
