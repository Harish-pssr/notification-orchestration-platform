package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/bench"
	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/events"
	"notifplatform/internal/experiment"
)

// recoveryRun is one repetition of an outage test.
type recoveryRun struct {
	Service  string                   `json:"service"`
	Rep      int                      `json:"repetition"`
	Rate     float64                  `json:"rate"`
	StopAt   float64                  `json:"stopped_at_db_epoch_s"`
	StartAt  float64                  `json:"started_at_db_epoch_s"`
	Recovery bench.RecoveryResult     `json:"recovery"`
	Load     bench.RunSummary         `json:"load"`
	Counts   bench.Counts             `json:"counts"`
	Verdict  bench.ReliabilityVerdict `json:"verdict"`
	Monitor  string                   `json:"monitor_csv"`
	DrainS   float64                  `json:"drain_after_load_s"`
}

// recoveryReport collects every repetition.
type recoveryReport struct {
	Run         string            `json:"run"`
	Relays      int               `json:"relays"`
	Consumers   int               `json:"consumers"`
	Outage      string            `json:"outage"`
	Runs        []recoveryRun     `json:"runs"`
	Environment bench.Environment `json:"environment"`
}

// dbNow reads the database clock, so outage times are on the same clock as the samples.
func (e env) dbNow(ctx context.Context) (float64, error) {
	var t float64
	err := e.pool.QueryRow(ctx, `SELECT extract(epoch FROM clock_timestamp())`).Scan(&t)
	return t, err
}

// recovery is `bench recovery`: steady load, a real outage of one service, and how the
// system catches up afterwards (D17).
func recovery(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("recovery", flag.ExitOnError)
	cm := addCommon(fs)
	service := fs.String("service", "relay", "Compose service to stop (relay or nats), or nats-purge to delete every message in the broker stream under load (D22)")
	rate := fs.Float64("rate", 150, "arrival rate, events per second (about 70% of capacity)")
	relays := fs.Int("relays", 4, "relay instances")
	consumers := fs.Int("consumers", 4, "consumer instances per channel")
	outage := fs.Duration("outage", 60*time.Second, "how long the service stays stopped")
	steady := fs.Duration("steady", 30*time.Second, "steady load before the outage")
	after := fs.Duration("after", 90*time.Second, "load continues this long after the restart")
	repeats := fs.Int("repeats", 3, "repetitions, each from a clean state")
	drain := fs.Duration("drain-timeout", 5*time.Minute, "how long to wait for everything to be delivered after the load")
	_ = fs.Parse(args)
	if *service != "relay" && *service != "nats" && *service != purgeService {
		return fmt.Errorf("-service must be relay, nats or %s", purgeService)
	}

	e, closeEnv, err := cm.open(ctx)
	if err != nil {
		return err
	}
	defer closeEnv()

	run := fmt.Sprintf("recovery-%s-%s", *service, time.Now().UTC().Format("20060102T150405Z"))
	rep := recoveryReport{Run: run, Relays: *relays, Consumers: *consumers,
		Outage: fmt.Sprintf("%s stopped for %s", *service, *outage), Environment: bench.CaptureEnvironment(ctx)}
	params := map[string]any{
		"service_stopped": *service, "outage_s": outage.Seconds(), "rate_per_s": *rate,
		"relays": *relays, "consumers_per_channel": *consumers, "repetitions": *repeats,
	}
	mark := func(kind, text string) {
		fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), text)
		if err := experiment.Mark(context.Background(), e.pool, run, kind, text, params); err != nil {
			fmt.Fprintln(os.Stderr, "marker:", err)
		}
	}
	defer func() { // never leave the service stopped
		if *service != purgeService {
			_ = e.stack.Start(context.Background(), *service)
		}
		_ = e.stack.Scale(context.Background(), *relays, *consumers)
	}()

	perEvent := len(events.RoutesFor(events.PaymentSucceeded))
	for i := 1; i <= *repeats; i++ {
		fmt.Printf("=== %s: repetition %d of %d ===\n", run, i, *repeats)
		r, err := e.recoveryOnce(ctx, recoveryConfig{
			run: run, service: *service, rep: i, rate: *rate, relays: *relays, consumers: *consumers,
			outage: *outage, steady: *steady, after: *after, drain: *drain, perEvent: perEvent, mark: mark,
		})
		if err != nil {
			return err
		}
		rep.Runs = append(rep.Runs, r)
		r.print()
	}
	mark(experiment.End, "finished all repetitions")

	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*cm.out, run+".json"), raw, 0o644)
}

type recoveryConfig struct {
	run, service          string
	rep                   int
	rate                  float64
	relays, consumers     int
	outage, steady, after time.Duration
	drain                 time.Duration
	perEvent              int
	mark                  func(kind, text string)
}

func (e env) recoveryOnce(ctx context.Context, c recoveryConfig) (recoveryRun, error) {
	out := recoveryRun{Service: c.service, Rep: c.rep, Rate: c.rate, DrainS: -1}
	if err := bench.Reset(ctx); err != nil {
		return out, fmt.Errorf("reset: %w", err)
	}
	if err := e.stack.Scale(ctx, c.relays, c.consumers); err != nil {
		return out, err
	}
	if err := e.stack.WaitReady(ctx, c.relays, c.consumers, 90*time.Second); err != nil {
		return out, err
	}

	out.Monitor = filepath.Join(e.outDir, fmt.Sprintf("%s-rep%d.csv", c.run, c.rep))
	monCtx, stopMon := context.WithCancel(ctx)
	monDone := make(chan struct{})
	go func() { defer close(monDone); _ = bench.Monitor(monCtx, e.pool, out.Monitor, time.Second) }()
	var stopOnce sync.Once
	stopMonitor := func() { stopOnce.Do(func() { stopMon(); <-monDone }) }
	defer stopMonitor()

	total := c.steady + c.outage + c.after
	c.mark(experiment.Start, fmt.Sprintf("repetition %d of the %s outage: steady load", c.rep, c.service))
	var wg sync.WaitGroup
	var loadErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		out.Load, loadErr = e.stack.RunLoad(ctx, bench.OpenLoopConfig{Rate: c.rate, Duration: total, Run: fmt.Sprintf("%s-%d", c.run, c.rep)})
	}()

	select {
	case <-ctx.Done():
		wg.Wait()
		return out, ctx.Err()
	case <-time.After(c.steady):
	}
	var stopAt, startAt float64
	var err error
	if c.service == purgeService {
		// Not an outage: the broker loses every message it holds, once, under load.
		c.mark(experiment.Phase, fmt.Sprintf("repetition %d: purging every message from the broker stream", c.rep))
		if stopAt, err = e.dbNow(ctx); err != nil {
			return out, err
		}
		if err := purgeStream(ctx); err != nil {
			return out, err
		}
		startAt = stopAt
		c.mark(experiment.Phase, fmt.Sprintf("repetition %d: stream purged, the reconciler has to find the lost deliveries", c.rep))
	} else {
		c.mark(experiment.Phase, fmt.Sprintf("repetition %d: stopping %s", c.rep, c.service))
		if stopAt, err = e.dbNow(ctx); err != nil {
			return out, err
		}
		if err := e.stack.Stop(ctx, c.service); err != nil {
			return out, err
		}
		time.Sleep(c.outage)
		if startAt, err = e.dbNow(ctx); err != nil {
			return out, err
		}
		if err := e.stack.Start(ctx, c.service); err != nil {
			return out, err
		}
		c.mark(experiment.Phase, fmt.Sprintf("repetition %d: %s restarted, recovering", c.rep, c.service))
	}
	out.StopAt, out.StartAt = stopAt, startAt
	wg.Wait()
	if loadErr != nil {
		return out, loadErr
	}

	// Wait for the backlog to clear, then read the final state.
	drainStart := time.Now()
	for time.Since(drainStart) < c.drain {
		counts, err := bench.ReadCounts(ctx, e.pool, e.provider)
		if err != nil {
			return out, err
		}
		if counts.NotFinished == 0 {
			out.DrainS = time.Since(drainStart).Seconds()
			break
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	time.Sleep(2 * time.Second) // let the monitor take a last sample
	stopMonitor()

	if out.Counts, err = bench.ReadCounts(ctx, e.pool, e.provider); err != nil {
		return out, err
	}
	out.Verdict = bench.JudgeReliability(out.Load.Created, c.perEvent, out.Counts, false)
	samples, err := bench.ReadSamples(out.Monitor)
	if err != nil {
		return out, err
	}
	out.Recovery = bench.AnalyzeRecovery(samples, stopAt, startAt, c.rate, c.perEvent)
	return out, nil
}

func (r recoveryRun) print() {
	v := r.Recovery
	fmt.Printf("  outage %.0f s | peak backlog: %d events in the outbox, %d deliveries undelivered (steady about %.0f)\n",
		v.OutageS, v.PeakOutboxWaiting, v.PeakDeliveriesWaiting, v.SteadyDeliveriesWait)
	if v.RecoveryS < 0 {
		fmt.Println("  recovery: backlog did NOT return to normal")
	} else {
		fmt.Printf("  recovery: %.0f s after restart (below %.0f deliveries) | peak delivery rate %.0f/s\n", v.RecoveryS, v.ThresholdDeliveries, v.PeakDeliveryRatePerS)
	}
	c := r.Counts
	fmt.Printf("  accepted %d, stored %d, delivered %d of %d, dead-lettered %d, duplicate side effects %d => ",
		r.Load.Created, c.Events, c.Delivered, c.Deliveries, c.DeadLettered, c.DuplicateSideEffects)
	if r.Verdict.Pass {
		fmt.Println("PASS (nothing lost)")
		return
	}
	fmt.Println("FAIL")
	for _, f := range r.Verdict.Failures {
		fmt.Println("    -", f)
	}
}

// purgeService is the -service value that deletes the broker's stored messages instead
// of stopping a container (D22).
const purgeService = "nats-purge"

// purgeStream deletes every message in the NOTIFY stream, as if the broker's storage
// had been lost. It is a simulation of loss through the broker's own API, not a disk failure.
func purgeStream(ctx context.Context) error {
	nc, err := nats.Connect(config.Load().NATSURL, nats.Name("bench-purge"))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	stream, err := js.Stream(ctx, broker.StreamName)
	if err != nil {
		return err
	}
	return stream.Purge(ctx)
}
