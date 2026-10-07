package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/bench"
	"notifplatform/internal/config"
)

// runLoad is `loadgen run`: open-loop load through the HTTP API (D13).
func runLoad(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	url := fs.String("url", "http://localhost:8080", "API base URL")
	rate := fs.Float64("rate", 100, "events per second to send (open loop)")
	duration := fs.Duration("duration", 30*time.Second, "how long to send")
	run := fs.String("run", "", "run id stored in each event so results can be found later (default: random)")
	maxInflight := fs.Int("max-inflight", 20000, "safety cap on concurrent requests; exceeding it is counted as skipped")
	timeout := fs.Duration("timeout", 30*time.Second, "per-request timeout")
	asJSON := fs.Bool("json", false, "print the summary as one JSON line")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	summary, err := bench.RunOpenLoop(ctx, bench.OpenLoopConfig{
		URL: *url, Rate: *rate, Duration: *duration, Run: *run, MaxInflight: *maxInflight, Timeout: *timeout,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(summary)
	}
	fmt.Print(bench.FormatRun(summary))
	return nil
}

// analyzeRun is `loadgen analyze`: exact latency percentiles from database timestamps.
func analyzeRun(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)
	run := fs.String("run", "", "run id (required)")
	warmup := fs.Duration("warmup", 10*time.Second, "discard events created in this period after the first")
	window := fs.Duration("window", 60*time.Second, "measurement window length")
	asJSON := fs.Bool("json", false, "machine-readable output")
	_ = fs.Parse(args)
	if *run == "" {
		return fmt.Errorf("-run is required")
	}
	pool, err := pgxpool.New(context.Background(), config.Load().PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	a, err := bench.Analyze(context.Background(), pool, bench.AnalyzeConfig{Run: *run, Warmup: *warmup, Window: *window})
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(a)
	}
	fmt.Print(bench.FormatAnalysis(a))
	return nil
}

// monitorRun is `loadgen monitor`: sample progress once per interval into a CSV.
func monitorRun(args []string) error {
	fs := flag.NewFlagSet("monitor", flag.ExitOnError)
	out := fs.String("out", "monitor.csv", "CSV file to write")
	interval := fs.Duration("interval", time.Second, "sampling interval")
	duration := fs.Duration("duration", 0, "stop after this long (0 = until interrupted)")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	pool, err := pgxpool.New(ctx, config.Load().PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	return bench.Monitor(ctx, pool, *out, *interval)
}

// backlog is `loadgen backlog`: unfinished work right now, as JSON.
func backlog() error {
	pool, err := pgxpool.New(context.Background(), config.Load().PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	b, err := bench.ReadBacklog(context.Background(), pool)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(b)
}
