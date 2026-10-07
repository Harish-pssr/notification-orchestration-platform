// Command bench runs the benchmark suite (D13): it scales the stack, applies open-loop
// load through the HTTP API from inside the Compose network, and measures exact results
// from database timestamps.
//
//	bench ramp -relays 1 -consumers 1 -rates 50,100,200,400 -step 30s
//
// It is destructive: every step resets all data, so it requires -confirm.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/bench"
	"notifplatform/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "ramp":
		err = ramp(ctx, os.Args[2:])
	case "sustain":
		err = sustain(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench ramp|sustain -confirm [-relays R] [-consumers C] [-rate N | -start N] [-step 30s]")
	os.Exit(2)
}

// common holds the flags every subcommand shares.
type common struct {
	confirm      *bool
	compose      *string
	api          *string
	out          *string
	allowBattery *bool
}

func addCommon(fs *flag.FlagSet) common {
	return common{
		confirm:      fs.Bool("confirm", false, "required: every step deletes all data"),
		compose:      fs.String("compose", "docker-compose.yml,docker-compose.dev.yml", "comma-separated Compose files"),
		api:          fs.String("api", "http://localhost:8080", "API URL as seen from this machine"),
		out:          fs.String("out", "benchmark-results", "directory for raw results"),
		allowBattery: fs.Bool("allow-battery", false, "benchmark even though the machine is on battery (results will be unreliable)"),
	}
}

func (c common) open(ctx context.Context) (env, func(), error) {
	if !*c.confirm {
		return env{}, nil, fmt.Errorf("refusing to run without -confirm: every step deletes all data")
	}
	if err := os.MkdirAll(*c.out, 0o755); err != nil {
		return env{}, nil, err
	}
	machine := bench.CaptureEnvironment(ctx)
	fmt.Printf("environment: %s, %d logical CPUs, power %s (%s), clock %d/%d MHz, Docker VM %d CPUs %.1f GB\n",
		machine.CPUModel, machine.LogicalCPUs, machine.PowerSource, machine.PowerPlan, machine.ClockMHz, machine.MaxClockMHz, machine.DockerCPUs, machine.DockerMemGB)
	if w := machine.Warning(); w != "" {
		if !*c.allowBattery {
			return env{}, nil, fmt.Errorf("%s (override with -allow-battery)", w)
		}
		fmt.Println("WARNING:", w)
	}
	if raw, err := json.MarshalIndent(machine, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(*c.out, "environment.json"), raw, 0o644)
	}
	pool, err := pgxpool.New(ctx, config.Load().PlatformDSN)
	if err != nil {
		return env{}, nil, err
	}
	wd, _ := os.Getwd()
	return env{
		stack:  bench.Stack{ComposeFiles: strings.Split(*c.compose, ","), Dir: wd, APIURL: *c.api},
		pool:   pool,
		outDir: *c.out,
	}, pool.Close, nil
}

func parseRates(s string) ([]float64, error) {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("bad rate %q", f)
		}
		out = append(out, v)
	}
	return out, nil
}

// geometric returns start, start*factor, ... up to max, each rounded to a whole number.
func geometric(start, factor, max float64) []float64 {
	var out []float64
	for r := start; r <= max; r *= factor {
		rounded := float64(int(r + 0.5))
		if len(out) == 0 || rounded > out[len(out)-1] {
			out = append(out, rounded)
		}
	}
	return out
}

// ramp finds the capacity of one configuration: it raises the arrival rate in steps and
// reports the highest rate the system sustained (D13).
func ramp(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ramp", flag.ExitOnError)
	cm := addCommon(fs)
	relays := fs.Int("relays", 1, "relay instances")
	consumers := fs.Int("consumers", 1, "consumer instances per channel")
	rates := fs.String("rates", "", "explicit events per second, ascending (overrides -start and -factor)")
	start := fs.Float64("start", 50, "first rate of a geometric ramp, events per second")
	factor := fs.Float64("factor", 1.25, "each step's rate is the previous times this")
	top := fs.Float64("max-rate", 4000, "stop the geometric ramp at this rate")
	step := fs.Duration("step", 30*time.Second, "length of each step")
	warmup := fs.Duration("warmup", 5*time.Second, "discard events created in this period of a step")
	window := fs.Duration("window", 20*time.Second, "measurement window within a step")
	label := fs.String("label", "", "label for output files (default r<relays>c<consumers>)")
	maxFail := fs.Int("stop-after-failures", 2, "stop after this many consecutive unsustained steps")
	_ = fs.Parse(args)

	e, closeEnv, err := cm.open(ctx)
	if err != nil {
		return err
	}
	defer closeEnv()
	var rs []float64
	if *rates != "" {
		if rs, err = parseRates(*rates); err != nil {
			return err
		}
	} else {
		rs = geometric(*start, *factor, *top)
	}
	if *label == "" {
		*label = fmt.Sprintf("ramp-r%dc%d", *relays, *consumers)
	}

	fmt.Printf("configuration: %d relay(s), %d consumer(s) per channel\n", *relays, *consumers)
	if err := e.stack.Scale(ctx, *relays, *consumers); err != nil {
		return err
	}
	if err := e.stack.WaitReady(ctx, *relays, *consumers, 90*time.Second); err != nil {
		return err
	}

	jsonl, err := os.Create(filepath.Join(*cm.out, *label+".jsonl"))
	if err != nil {
		return err
	}
	defer jsonl.Close()

	var capacity float64
	failures := 0
	for _, rate := range rs {
		res, err := e.runStep(ctx, stepConfig{
			Label: *label, Relays: *relays, Consumers: *consumers, Rate: rate,
			Duration: *step, Warmup: *warmup, Window: *window, DrainTimeout: 120 * time.Second,
		})
		if err != nil {
			return err
		}
		fmt.Println(res.line())
		_ = json.NewEncoder(jsonl).Encode(res)

		if res.Verdict.Sustained {
			capacity, failures = rate, 0
		} else if failures++; failures >= *maxFail {
			break
		}
	}
	fmt.Printf("capacity of this configuration (highest sustained rate in the ramp): %.0f events/s\n", capacity)
	return nil
}

// sustain repeats one fixed-rate step (D13): the same configuration and arrival rate, a
// number of times, so run-to-run variation is visible instead of hidden.
func sustain(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("sustain", flag.ExitOnError)
	cm := addCommon(fs)
	relays := fs.Int("relays", 1, "relay instances")
	consumers := fs.Int("consumers", 1, "consumer instances per channel")
	rate := fs.Float64("rate", 100, "events per second")
	step := fs.Duration("step", 60*time.Second, "length of each run")
	warmup := fs.Duration("warmup", 10*time.Second, "discard events created in this period")
	window := fs.Duration("window", 40*time.Second, "measurement window")
	reps := fs.Int("reps", 3, "repetitions")
	label := fs.String("label", "", "label for output files")
	_ = fs.Parse(args)

	e, closeEnv, err := cm.open(ctx)
	if err != nil {
		return err
	}
	defer closeEnv()
	if *label == "" {
		*label = fmt.Sprintf("sustain-r%dc%d", *relays, *consumers)
	}
	if err := e.stack.Scale(ctx, *relays, *consumers); err != nil {
		return err
	}
	if err := e.stack.WaitReady(ctx, *relays, *consumers, 90*time.Second); err != nil {
		return err
	}
	jsonl, err := os.OpenFile(filepath.Join(*cm.out, *label+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer jsonl.Close()

	for i := 1; i <= *reps; i++ {
		res, err := e.runStep(ctx, stepConfig{
			Label: *label, Relays: *relays, Consumers: *consumers, Rate: *rate,
			Duration: *step, Warmup: *warmup, Window: *window, DrainTimeout: 120 * time.Second,
		})
		if err != nil {
			return err
		}
		fmt.Printf("rep %d: %s\n", i, res.line())
		_ = json.NewEncoder(jsonl).Encode(res)
	}
	return nil
}
