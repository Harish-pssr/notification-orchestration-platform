// Package bench holds the benchmark tooling shared by cmd/loadgen (the command line)
// and cmd/bench (the orchestrator): an open-loop load generator, exact analysis read
// from database timestamps, and a backlog monitor (D13).
package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// The open-loop generator (D13).
//
// An open-loop generator sends at a fixed rate no matter how slowly the system answers.
// A closed-loop one waits for each answer before sending the next, so when the system
// slows down the generator slows down with it, queueing is hidden, and latency looks
// better than any real user would see ("coordinated omission").
//
// To keep the numbers honest this generator:
//   - schedules every request at a fixed intended time (start + i/rate),
//   - measures API latency from that INTENDED time, not from when the request was
//     actually written, so a slow system shows up as latency instead of vanishing,
//   - records how late it was in sending (schedule lag) so a reader can tell whether
//     the generator itself kept up (it must never be the bottleneck).

// intendedAt is when request i should be sent. It is computed from the start time
// rather than accumulated, so timer jitter cannot make the schedule drift.
func intendedAt(start time.Time, i int, rate float64) time.Time {
	return start.Add(time.Duration(float64(i) / rate * float64(time.Second)))
}

// percentile returns the p-th percentile (0..1) of sorted values by linear
// interpolation, or 0 for an empty slice.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}
	pos := p * float64(n-1)
	lo := int(pos)
	if lo >= n-1 {
		return sorted[n-1]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

type sample struct {
	apiLatency time.Duration // end - INTENDED send time
	lag        time.Duration // actual send - intended send time
	status     int           // 0 = transport error
	sent       bool
}

// RunSummary is what one open-loop run reports. Latencies are in milliseconds.
type RunSummary struct {
	Run          string  `json:"run"`
	TargetRate   float64 `json:"target_rate"`
	DurationS    float64 `json:"duration_s"`
	Planned      int     `json:"planned"`
	Sent         int     `json:"sent"`
	Created      int     `json:"created"`       // HTTP 201
	Rejected     int     `json:"rejected"`      // any other HTTP status
	Errors       int     `json:"errors"`        // transport errors and timeouts
	Skipped      int     `json:"skipped"`       // not sent because the in-flight cap was reached
	AchievedRate float64 `json:"achieved_rate"` // created per second over the run
	MaxInflight  int64   `json:"max_inflight"`
	LagP50Ms     float64 `json:"schedule_lag_p50_ms"`
	LagP99Ms     float64 `json:"schedule_lag_p99_ms"`
	LagMaxMs     float64 `json:"schedule_lag_max_ms"`
	APIP50Ms     float64 `json:"api_p50_ms"`
	APIP95Ms     float64 `json:"api_p95_ms"`
	APIP99Ms     float64 `json:"api_p99_ms"`
	APIMaxMs     float64 `json:"api_max_ms"`
	GeneratorOK  bool    `json:"generator_kept_up"` // p99 schedule lag under 50 ms and nothing skipped
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// summarize turns raw samples into a RunSummary. Pure function, unit-tested.
func summarize(run string, rate float64, duration time.Duration, samples []sample, skipped int, maxInflight int64) RunSummary {
	s := RunSummary{Run: run, TargetRate: rate, DurationS: duration.Seconds(), Planned: len(samples), Skipped: skipped, MaxInflight: maxInflight}
	var api, lag []float64
	for _, x := range samples {
		if !x.sent {
			continue
		}
		s.Sent++
		lag = append(lag, ms(x.lag))
		switch {
		case x.status == 0:
			s.Errors++
		case x.status == http.StatusCreated:
			s.Created++
		default:
			s.Rejected++
		}
		if x.status != 0 { // a transport error has no meaningful latency
			api = append(api, ms(x.apiLatency))
		}
	}
	sort.Float64s(api)
	sort.Float64s(lag)
	s.LagP50Ms, s.LagP99Ms = percentile(lag, 0.5), percentile(lag, 0.99)
	if n := len(lag); n > 0 {
		s.LagMaxMs = lag[n-1]
	}
	s.APIP50Ms, s.APIP95Ms, s.APIP99Ms = percentile(api, 0.5), percentile(api, 0.95), percentile(api, 0.99)
	if n := len(api); n > 0 {
		s.APIMaxMs = api[n-1]
	}
	if duration > 0 {
		s.AchievedRate = float64(s.Created) / duration.Seconds()
	}
	s.GeneratorOK = s.Skipped == 0 && s.LagP99Ms < 50
	return s
}

// OpenLoopConfig configures one run.
type OpenLoopConfig struct {
	URL         string        // API base URL, e.g. http://api:8080
	Rate        float64       // events per second
	Duration    time.Duration // how long to send
	Run         string        // run id stored in every event's account_id so results can be found
	MaxInflight int           // safety cap on concurrent requests; exceeding it is counted as skipped
	Timeout     time.Duration // per-request timeout
}

// RunOpenLoop sends PaymentSucceeded events at cfg.Rate for cfg.Duration, open loop.
func RunOpenLoop(ctx context.Context, cfg OpenLoopConfig) (RunSummary, error) {
	if cfg.Rate <= 0 || cfg.Duration <= 0 {
		return RunSummary{}, fmt.Errorf("rate and duration must be positive")
	}
	if cfg.Run == "" {
		cfg.Run = "run-" + uuid.NewString()[:8]
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 20000
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	client := &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns: cfg.MaxInflight, MaxIdleConnsPerHost: cfg.MaxInflight,
			IdleConnTimeout: 90 * time.Second,
		},
	}
	endpoint := cfg.URL + "/api/events/payment-succeeded"
	body, _ := json.Marshal(map[string]any{"account_id": cfg.Run, "amount_cents": 1999, "currency": "USD"})
	total := int(cfg.Rate * cfg.Duration.Seconds())
	samples := make([]sample, total)

	var (
		wg       sync.WaitGroup
		inflight atomic.Int64
		peak     atomic.Int64
		skipped  int
	)
	send := func(i int, intended time.Time) {
		defer wg.Done()
		defer inflight.Add(-1)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", uuid.NewString())

		actual := time.Now()
		resp, err := client.Do(req)
		status := 0
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			status = resp.StatusCode
		}
		samples[i] = sample{apiLatency: time.Since(intended), lag: actual.Sub(intended), status: status, sent: true}
	}

	start := time.Now()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for next := 0; next < total; {
		select {
		case <-ctx.Done():
			wg.Wait()
			return summarize(cfg.Run, cfg.Rate, time.Since(start), samples, skipped, peak.Load()), ctx.Err()
		case <-ticker.C:
		}
		now := time.Now()
		// Send everything whose intended time has arrived. If the loop fell behind, this
		// catches up, and the lag is recorded against each request's intended time.
		for next < total && !intendedAt(start, next, cfg.Rate).After(now) {
			if inflight.Load() >= int64(cfg.MaxInflight) {
				skipped++
				next++
				continue
			}
			cur := inflight.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			wg.Add(1)
			go send(next, intendedAt(start, next, cfg.Rate))
			next++
		}
	}
	wg.Wait()
	return summarize(cfg.Run, cfg.Rate, cfg.Duration, samples, skipped, peak.Load()), nil
}

// FormatRun renders a RunSummary for a person.
func FormatRun(s RunSummary) string {
	return fmt.Sprintf("run %s: target %.0f/s for %.0fs\n"+
		"  sent %d of %d planned (skipped %d): %d created, %d rejected, %d errors; achieved %.1f created/s\n"+
		"  API latency from intended send time: p50 %.1f ms, p95 %.1f ms, p99 %.1f ms, max %.1f ms\n"+
		"  generator schedule lag: p50 %.2f ms, p99 %.2f ms, max %.2f ms (max in flight %d); generator kept up: %v\n",
		s.Run, s.TargetRate, s.DurationS, s.Sent, s.Planned, s.Skipped, s.Created, s.Rejected, s.Errors, s.AchievedRate,
		s.APIP50Ms, s.APIP95Ms, s.APIP99Ms, s.APIMaxMs, s.LagP50Ms, s.LagP99Ms, s.LagMaxMs, s.MaxInflight, s.GeneratorOK)
}
