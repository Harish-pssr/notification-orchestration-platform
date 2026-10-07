package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/bench"
)

// env is everything a step needs: the stack to drive and the database to read.
type env struct {
	stack    bench.Stack
	pool     *pgxpool.Pool // platform database
	provider *pgxpool.Pool // the mock provider's database
	admin    bench.Admin   // fault injection through the API (reliability and recovery runs)
	outDir   string
}

// stepResult is one fixed-rate step, fully measured (D13).
type stepResult struct {
	Label      string           `json:"label"`
	Relays     int              `json:"relays"`
	Consumers  int              `json:"consumers"`
	Rate       float64          `json:"rate"`
	Load       bench.RunSummary `json:"load"`
	BacklogEnd bench.Backlog    `json:"backlog_at_end_of_load"`
	DrainS     float64          `json:"drain_after_load_s"` // wall clock, load end to empty backlog; -1 if it did not drain
	Analysis   bench.Analysis   `json:"analysis"`
	Verdict    bench.Verdict    `json:"verdict"`
	Monitor    string           `json:"monitor_csv"`
	// CPU is per Compose service over the load period (docker stats samples; 100 = one
	// core). It shows which component was busy, and whether the Go processes or the
	// infrastructure limited throughput.
	CPU map[string]cpuStat `json:"cpu"`
}

type cpuStat struct {
	Mean float64 `json:"mean"`
	Max  float64 `json:"max"`
	N    int     `json:"samples"`
}

// stepConfig describes one fixed-rate step.
type stepConfig struct {
	Label          string
	Relays         int
	Consumers      int
	Rate           float64
	Duration       time.Duration
	Warmup, Window time.Duration
	DrainTimeout   time.Duration
	SkipReset      bool
}

// runStep resets the system, applies load at a fixed open-loop rate, waits for the
// backlog to drain, and measures everything exactly from the database.
func (e env) runStep(ctx context.Context, c stepConfig) (stepResult, error) {
	res := stepResult{Label: c.Label, Relays: c.Relays, Consumers: c.Consumers, Rate: c.Rate}
	if !c.SkipReset {
		if err := bench.Reset(ctx); err != nil {
			return res, fmt.Errorf("reset: %w", err)
		}
	}

	run := fmt.Sprintf("%s-%.0f-%s", c.Label, c.Rate, uuid.NewString()[:6])
	res.Monitor = filepath.Join(e.outDir, run+".csv")

	// Sample progress once per second on the database clock for the whole step.
	monCtx, stopMon := context.WithCancel(ctx)
	monDone := make(chan struct{})
	go func() {
		defer close(monDone)
		_ = bench.Monitor(monCtx, e.pool, res.Monitor, time.Second)
	}()
	defer func() { stopMon(); <-monDone }()

	// Sample per-service CPU in the background while the load runs.
	cpuCtx, stopCPU := context.WithCancel(ctx)
	cpuDone := make(chan map[string]cpuStat, 1)
	go func() { cpuDone <- sampleCPU(cpuCtx, e.stack) }()

	sum, err := e.stack.RunLoad(ctx, bench.OpenLoopConfig{Rate: c.Rate, Duration: c.Duration, Run: run})
	stopCPU()
	res.CPU = <-cpuDone
	if err != nil {
		return res, err
	}
	res.Load = sum

	if res.BacklogEnd, err = bench.ReadBacklog(ctx, e.pool); err != nil {
		return res, err
	}

	// Wait for the backlog to drain (wall clock, from the moment the load ended).
	drainStart := time.Now()
	res.DrainS = -1
	deadline := drainStart.Add(c.DrainTimeout)
	for time.Now().Before(deadline) {
		b, err := bench.ReadBacklog(ctx, e.pool)
		if err != nil {
			return res, err
		}
		if b.Events == 0 {
			res.DrainS = time.Since(drainStart).Seconds()
			break
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	if res.Analysis, err = bench.Analyze(ctx, e.pool, bench.AnalyzeConfig{Run: run, Warmup: c.Warmup, Window: c.Window}); err != nil {
		return res, err
	}
	res.Verdict = bench.Judge(res.Load, res.BacklogEnd, res.Analysis)
	return res, nil
}

// line renders a one-line result for the console.
func (r stepResult) line() string {
	verdict := "sustained"
	if !r.Verdict.Sustained {
		verdict = "NOT sustained: " + r.Verdict.Reason
	}
	drain := fmt.Sprintf("%.1fs", r.DrainS)
	if r.DrainS < 0 {
		drain = "did not drain"
	}
	return fmt.Sprintf("%s  R=%d C=%d  rate %5.0f/s  achieved %6.1f  API p99 %6.1f ms  backlog@end %5d  drain %-13s  e2e p50/p95/p99 %7.0f/%7.0f/%7.0f ms  %s",
		r.Label, r.Relays, r.Consumers, r.Rate, r.Load.AchievedRate, r.Load.APIP99Ms, r.BacklogEnd.Events, drain,
		r.Analysis.EndToEnd.P50, r.Analysis.EndToEnd.P95, r.Analysis.EndToEnd.P99, verdict)
}

// sampleCPU polls docker stats until ctx ends and summarises each service's CPU.
func sampleCPU(ctx context.Context, stack bench.Stack) map[string]cpuStat {
	sums := map[string]cpuStat{}
	for ctx.Err() == nil {
		snap, err := stack.CPUByService(ctx)
		if err == nil {
			for svc, pct := range snap {
				st := sums[svc]
				st.Mean += pct // running sum until the end
				st.N++
				if pct > st.Max {
					st.Max = pct
				}
				sums[svc] = st
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	for svc, st := range sums {
		if st.N > 0 {
			st.Mean /= float64(st.N)
		}
		sums[svc] = st
	}
	return sums
}
