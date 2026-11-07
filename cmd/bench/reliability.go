package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"notifplatform/internal/bench"
	"notifplatform/internal/events"
	"notifplatform/internal/experiment"
)

// crashRecord is one relay kill during a reliability run.
type crashRecord struct {
	PlannedAtS float64 `json:"planned_at_s"` // offset from the start of the load
	At         string  `json:"at"`
	Container  string  `json:"container"`
}

// reliabilityReport is everything the reliability command measured (D17).
type reliabilityReport struct {
	Run       string           `json:"run"`
	Control   bool             `json:"negative_control"`
	Relays    int              `json:"relays"`
	Consumers int              `json:"consumers"`
	Rate      float64          `json:"rate"`
	DupProb   float64          `json:"duplicate_probability"`
	ErrProb   float64          `json:"provider_error_probability"`
	Load      bench.RunSummary `json:"load"`
	Crashes   []crashRecord    `json:"relay_crashes"`
	// Present only when the run asked for them (P1d, a bigger chaos run).
	ConsumerCrashes []crashRecord            `json:"consumer_crashes,omitempty"`
	PostgresCrashes []crashRecord            `json:"postgres_crashes,omitempty"`
	UnknownOutcome  int                      `json:"requests_with_unknown_outcome,omitempty"` // API errors while Postgres was down
	DrainS          float64                  `json:"drain_after_load_s"`                      // -1 if it did not finish
	Counts          bench.Counts             `json:"counts"`
	Verdict         bench.ReliabilityVerdict `json:"verdict"`
	Environment     bench.Environment        `json:"environment"`
	StartedAt       string                   `json:"started_at"`
}

// reliability is `bench reliability`: load through the API while duplicates, relay
// crashes and provider errors are injected, then count exactly what happened (D17).
func reliability(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reliability", flag.ExitOnError)
	cm := addCommon(fs)
	total := fs.Int("events", 100000, "events to create")
	rate := fs.Float64("rate", 150, "arrival rate, events per second (keep it below capacity)")
	relays := fs.Int("relays", 4, "relay instances")
	consumers := fs.Int("consumers", 4, "consumer instances per channel")
	crashes := fs.Int("crashes", 3, "relay containers to kill at random times during the load")
	consumerKills := fs.Int("consumer-kills", 0, "consumer containers (any channel) to kill at random times during the load")
	postgresKills := fs.Int("postgres-kills", 0, "times to SIGKILL the platform Postgres during the load; it is started again after about 5 s and recovers from its log")
	dup := fs.Float64("duplicates", 0.05, "fraction of events the relay publishes twice")
	errRate := fs.Float64("errors", 0.02, "fraction of provider requests that fail with 503")
	control := fs.Bool("control", false, "negative control: switch the idempotency protections off (expects duplicate side effects)")
	drain := fs.Duration("drain-timeout", 10*time.Minute, "how long to wait for everything to be delivered after the load")
	label := fs.String("label", "", "label for the output file")
	_ = fs.Parse(args)

	e, closeEnv, err := cm.open(ctx)
	if err != nil {
		return err
	}
	defer closeEnv()
	if *label == "" {
		*label = "reliability"
		if *control {
			*label = "reliability-control"
		}
	}
	run := fmt.Sprintf("%s-%s", *label, time.Now().UTC().Format("20060102T150405Z"))
	duration := time.Duration(float64(*total) / *rate * float64(time.Second))
	rep := reliabilityReport{
		Run: run, Control: *control, Relays: *relays, Consumers: *consumers, Rate: *rate,
		DupProb: *dup, ErrProb: *errRate, StartedAt: time.Now().UTC().Format(time.RFC3339),
		Environment: bench.CaptureEnvironment(ctx),
	}
	params := map[string]any{
		"events": *total, "rate_per_s": *rate, "relays": *relays, "consumers_per_channel": *consumers,
		"duplicate_probability": *dup, "provider_error_probability": *errRate, "relay_crashes": *crashes,
		"consumer_kills": *consumerKills, "postgres_kills": *postgresKills,
		"idempotency_protections": map[bool]string{false: "on", true: "OFF (negative control)"}[*control],
	}
	mark := func(kind, text string) {
		fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), text)
		if err := experiment.Mark(context.Background(), e.pool, run, kind, text, params); err != nil {
			fmt.Fprintln(os.Stderr, "marker:", err)
		}
	}
	// Whatever happens, leave the system as we found it.
	defer func() {
		if err := e.admin.Restore(); err != nil {
			fmt.Fprintln(os.Stderr, "WARNING: could not restore normal settings:", err)
		}
		_ = e.stack.Scale(context.Background(), *relays, *consumers)
	}()

	fmt.Printf("=== %s: %d events at %.0f/s (about %s), %d relay(s), %d consumer(s) per channel ===\n",
		run, *total, *rate, duration.Round(time.Second), *relays, *consumers)
	if err := bench.Reset(ctx); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	if err := e.stack.Scale(ctx, *relays, *consumers); err != nil {
		return err
	}
	if err := e.stack.WaitReady(ctx, *relays, *consumers, 90*time.Second); err != nil {
		return err
	}

	// The experiment's markers start only after the reset (the reset never touches them,
	// but a "start" marker before a destructive step would be misleading).
	mark(experiment.Start, "start: configuring failure injection")
	if err := e.admin.Restore(); err != nil {
		return fmt.Errorf("start from normal settings: %w", err)
	}
	if err := e.admin.SetFault(ctx, "relay.after_publish", map[string]any{"action": "duplicate", "probability": *dup}); err != nil {
		return fmt.Errorf("enable duplicates: %w", err)
	}
	for _, ch := range events.AllChannels {
		if err := e.admin.SetProviderFailure(ctx, string(ch), 503, *errRate); err != nil {
			return fmt.Errorf("enable provider errors: %w", err)
		}
	}
	if *control {
		if err := e.admin.SetFault(ctx, "consumer.skip_delivered_check", map[string]any{"action": "disable"}); err != nil {
			return err
		}
		if err := e.admin.SetProviderDedupe(ctx, false); err != nil {
			return err
		}
	}
	time.Sleep(3 * time.Second) // every process polls the fault table about once a second

	// Load runs in the background; the crash scheduler runs beside it.
	loadCtx, cancelLoad := context.WithCancel(ctx)
	defer cancelLoad()
	var wg sync.WaitGroup
	var loadSum bench.RunSummary
	var loadErr error
	loadStart := time.Now()
	mark(experiment.Phase, fmt.Sprintf("load running: %d events at %.0f/s", *total, *rate))
	wg.Add(1)
	go func() {
		defer wg.Done()
		loadSum, loadErr = e.stack.RunLoad(loadCtx, bench.OpenLoopConfig{Rate: *rate, Duration: duration, Run: run})
	}()

	if !*control && (*crashes > 0 || *consumerKills > 0 || *postgresKills > 0) {
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
		counts := map[string]int{}
		totals := map[string]int{chaosRelay: *crashes, chaosConsumer: *consumerKills, chaosPostgres: *postgresKills}
		for _, ev := range planChaos(rng, duration, *crashes, *consumerKills, *postgresKills) {
			select {
			case <-ctx.Done():
				cancelLoad()
				wg.Wait()
				return ctx.Err()
			case <-time.After(time.Until(loadStart.Add(ev.At))):
			}
			counts[ev.Kind]++
			label := fmt.Sprintf("%s crash %d of %d", ev.Kind, counts[ev.Kind], totals[ev.Kind])
			var victim string
			switch ev.Kind {
			case chaosRelay, chaosConsumer:
				var names []string
				var err error
				if ev.Kind == chaosRelay {
					names, err = e.stack.ContainerNames(ctx, "relay")
				} else {
					for _, ch := range events.AllChannels {
						n, e2 := e.stack.ContainerNames(ctx, "consumer-"+string(ch))
						names, err = append(names, n...), errors.Join(err, e2)
					}
				}
				if err != nil || len(names) == 0 {
					fmt.Fprintf(os.Stderr, "no %s container to kill: %v\n", ev.Kind, err)
					continue
				}
				victim = names[rng.IntN(len(names))]
			case chaosPostgres:
				names, err := e.stack.ContainerNames(ctx, "postgres-platform")
				if err != nil || len(names) == 0 {
					fmt.Fprintln(os.Stderr, "no platform Postgres container to kill:", err)
					continue
				}
				victim = names[0]
			}
			mark(experiment.Phase, fmt.Sprintf("%s: SIGKILL %s", label, victim))
			if err := e.stack.Kill(ctx, victim); err != nil {
				fmt.Fprintln(os.Stderr, err)
				continue
			}
			rec := crashRecord{PlannedAtS: ev.At.Seconds(), At: time.Now().UTC().Format(time.RFC3339), Container: victim}
			switch ev.Kind {
			case chaosRelay:
				rep.Crashes = append(rep.Crashes, rec)
			case chaosConsumer:
				rep.ConsumerCrashes = append(rep.ConsumerCrashes, rec)
			case chaosPostgres:
				rep.PostgresCrashes = append(rep.PostgresCrashes, rec)
			}
			if ev.Kind == chaosPostgres {
				time.Sleep(5 * time.Second) // a short outage: the database is simply gone
				if err := e.stack.Start(ctx, "postgres-platform"); err != nil {
					fmt.Fprintln(os.Stderr, "start Postgres again:", err)
				}
				mark(experiment.Phase, "platform Postgres started again; waiting for it to accept connections")
				for i := 0; i < 60; i++ { // crash recovery takes a few seconds
					if e.pool.Ping(ctx) == nil {
						break
					}
					time.Sleep(time.Second)
				}
				continue
			}
			time.Sleep(2 * time.Second)
			if err := e.stack.Scale(ctx, *relays, *consumers); err != nil { // bring the container back
				fmt.Fprintln(os.Stderr, "restore containers:", err)
			}
		}
	}
	wg.Wait()
	if loadErr != nil {
		return loadErr
	}
	rep.Load = loadSum
	fmt.Printf("load finished: %d created, %d rejected, %d errors\n", loadSum.Created, loadSum.Rejected, loadSum.Errors)

	// Wait until every delivery is finished, one way or the other.
	mark(experiment.Phase, "load finished: waiting for every delivery to finish")
	rep.DrainS = -1
	drainStart := time.Now()
	for time.Since(drainStart) < *drain {
		c, err := bench.ReadCounts(ctx, e.pool, e.provider)
		if err != nil {
			return err
		}
		if c.NotFinished == 0 && c.Events >= int64(loadSum.Created) {
			rep.DrainS = time.Since(drainStart).Seconds()
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	if rep.Counts, err = bench.ReadCounts(ctx, e.pool, e.provider); err != nil {
		return err
	}
	unknown := 0
	if len(rep.PostgresCrashes) > 0 {
		// A request that failed while the database was down (a timeout, a dropped connection or
		// a 5xx reply) may still have committed just before the reply was lost.
		unknown = loadSum.Errors + loadSum.Rejected
		rep.UnknownOutcome = unknown
	}
	rep.Verdict = bench.JudgeReliabilityUnknown(loadSum.Created, unknown, len(events.RoutesFor(events.PaymentSucceeded)), rep.Counts, *control)
	mark(experiment.End, fmt.Sprintf("finished: %s", map[bool]string{true: "PASS", false: "FAIL"}[rep.Verdict.Pass]))
	rep.print()

	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*cm.out, run+".json"), raw, 0o644)
}

func (r reliabilityReport) print() {
	c := r.Counts
	fmt.Println()
	fmt.Printf("events accepted by the API      %8d\n", r.Load.Created)
	fmt.Printf("events stored                   %8d\n", c.Events)
	fmt.Printf("deliveries                      %8d  (delivered %d, dead-lettered %d, unfinished %d)\n", c.Deliveries, c.Delivered, c.DeadLettered, c.NotFinished)
	fmt.Printf("deliveries that needed a retry  %8d\n", c.Retried)
	fmt.Printf("relay crashes                   %8d\n", len(r.Crashes))
	if len(r.ConsumerCrashes) > 0 || len(r.PostgresCrashes) > 0 {
		fmt.Printf("consumer crashes                %8d\n", len(r.ConsumerCrashes))
		fmt.Printf("platform Postgres kills         %8d  (%d API requests with an unknown outcome)\n", len(r.PostgresCrashes), r.UnknownOutcome)
	}
	fmt.Printf("provider requests               %8d  (distinct keys %d, injected errors %d)\n", c.ProviderRequests, c.DistinctRequestKeys, c.InjectedFailures)
	fmt.Printf("duplicate copies absorbed       %8d  (stopped by consumers %d, by the provider %d)\n", c.IgnoredByConsum+c.SuppressedByProvider, c.IgnoredByConsum, c.SuppressedByProvider)
	fmt.Printf("provider side effects           %8d  (distinct keys %d)\n", c.Effects, c.DistinctEffectKeys)
	fmt.Printf("DUPLICATE SIDE EFFECTS          %8d\n", c.DuplicateSideEffects)
	if r.Verdict.Pass {
		fmt.Println("RESULT: PASS")
		return
	}
	fmt.Println("RESULT: FAIL")
	for _, f := range r.Verdict.Failures {
		fmt.Println("  -", f)
	}
}
