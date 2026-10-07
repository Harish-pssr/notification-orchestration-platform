package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"notifplatform/internal/bench"
)

// confirmCapacity finds the highest rate in an ascending ladder that the system sustains
// reliably, starting from the ramp's best guess (D13). passes(i) runs the repeated test
// at rate i and reports whether a majority of repetitions sustained it.
//
// If the starting rate is confirmed, it climbs until a rate fails; if not, it walks down
// until one passes. It returns the index of the confirmed rate, or -1 if none passed.
// Each rate is tested at most once, which keeps the total run time bounded.
func confirmCapacity(n, start int, passes func(i int) bool) int {
	if n == 0 {
		return -1
	}
	i := start
	if i < 0 {
		i = 0
	}
	if i >= n {
		i = n - 1
	}
	if passes(i) {
		best := i
		for j := i + 1; j < n; j++ {
			if !passes(j) {
				break
			}
			best = j
		}
		return best
	}
	for j := i - 1; j >= 0; j-- {
		if passes(j) {
			return j
		}
	}
	return -1
}

// confirmation is the repeated test at one rate.
type confirmation struct {
	Rate   float64      `json:"rate"`
	Passes int          `json:"passes"`
	Reps   []stepResult `json:"reps"`
	Passed bool         `json:"majority_sustained"`
}

// capacityResult is everything the capacity command measured for one configuration.
type capacityResult struct {
	Label         string            `json:"label"`
	Relays        int               `json:"relays"`
	Consumers     int               `json:"consumers"`
	RampCapacity  float64           `json:"ramp_best_rate"`
	Confirmed     float64           `json:"confirmed_capacity"` // 0 = none
	Ramp          []stepResult      `json:"ramp"`
	Confirmations []confirmation    `json:"confirmations"`
	LatencyRate   float64           `json:"latency_rate"` // 70% of the confirmed capacity
	Latency       []stepResult      `json:"latency"`
	Environment   bench.Environment `json:"environment"`
}

// majority runs up to three repetitions of a step and stops as soon as the outcome is
// decided (two passes or two failures), so a clear result costs two runs, not three.
func (e env) majority(ctx context.Context, c stepConfig) (confirmation, error) {
	out := confirmation{Rate: c.Rate}
	fails := 0
	for rep := 1; rep <= 3 && out.Passes < 2 && fails < 2; rep++ {
		res, err := e.runStep(ctx, c)
		if err != nil {
			return out, err
		}
		fmt.Printf("    confirm %.0f/s rep %d: %s\n", c.Rate, rep, res.line())
		out.Reps = append(out.Reps, res)
		if res.Verdict.Sustained {
			out.Passes++
		} else {
			fails++
		}
	}
	out.Passed = out.Passes >= 2
	return out, nil
}

// capacity runs the full protocol for one configuration: ramp, confirm, then latency at
// 70% of the confirmed capacity (D13).
func capacity(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("capacity", flag.ExitOnError)
	cm := addCommon(fs)
	relays := fs.Int("relays", 1, "relay instances")
	consumers := fs.Int("consumers", 1, "consumer instances per channel")
	start := fs.Float64("start", 50, "first rate of the geometric ramp")
	factor := fs.Float64("factor", 1.25, "ramp step factor")
	top := fs.Float64("max-rate", 4000, "highest rate to try")
	label := fs.String("label", "", "label for output files")
	_ = fs.Parse(args)

	e, closeEnv, err := cm.open(ctx)
	if err != nil {
		return err
	}
	defer closeEnv()
	if *label == "" {
		*label = fmt.Sprintf("cap-r%dc%d", *relays, *consumers)
	}
	res := capacityResult{Label: *label, Relays: *relays, Consumers: *consumers, Environment: bench.CaptureEnvironment(ctx)}

	fmt.Printf("=== %s: %d relay(s), %d consumer(s) per channel ===\n", *label, *relays, *consumers)
	if err := e.stack.Scale(ctx, *relays, *consumers); err != nil {
		return err
	}
	if err := e.stack.WaitReady(ctx, *relays, *consumers, 90*time.Second); err != nil {
		return err
	}

	rates := geometric(*start, *factor, *top)
	step := stepConfig{Label: *label, Relays: *relays, Consumers: *consumers, DrainTimeout: 120 * time.Second}

	// Stage 1: ramp, 30 s steps, to find the neighbourhood.
	best, failures := -1, 0
	for i, rate := range rates {
		s := step
		s.Rate, s.Duration, s.Warmup, s.Window = rate, 30*time.Second, 5*time.Second, 20*time.Second
		r, err := e.runStep(ctx, s)
		if err != nil {
			return err
		}
		fmt.Println("  ramp:", r.line())
		res.Ramp = append(res.Ramp, r)
		if r.Verdict.Sustained {
			best, failures = i, 0
			res.RampCapacity = rate
		} else if failures++; failures >= 2 {
			break
		}
	}

	// Stage 2: confirm with repeated 60 s steps and a majority vote.
	cache := map[int]bool{}
	confirmed := confirmCapacity(len(rates), best, func(i int) bool {
		if v, ok := cache[i]; ok {
			return v
		}
		s := step
		s.Rate, s.Duration, s.Warmup, s.Window = rates[i], 60*time.Second, 10*time.Second, 40*time.Second
		c, err := e.majority(ctx, s)
		if err != nil {
			fmt.Fprintln(os.Stderr, "confirmation error:", err)
			cache[i] = false
			return false
		}
		res.Confirmations = append(res.Confirmations, c)
		fmt.Printf("  confirmation at %.0f/s: %d of %d repetitions sustained => %v\n", rates[i], c.Passes, len(c.Reps), c.Passed)
		cache[i] = c.Passed
		return c.Passed
	})
	if confirmed >= 0 {
		res.Confirmed = rates[confirmed]
	}
	fmt.Printf("  confirmed capacity: %.0f events/s (ramp alone suggested %.0f)\n", res.Confirmed, res.RampCapacity)

	// Stage 3: latency at 70% of the confirmed capacity, three repetitions.
	if res.Confirmed > 0 {
		res.LatencyRate = math.Round(0.7 * res.Confirmed)
		for rep := 1; rep <= 3; rep++ {
			s := step
			s.Rate, s.Duration, s.Warmup, s.Window = res.LatencyRate, 60*time.Second, 10*time.Second, 40*time.Second
			r, err := e.runStep(ctx, s)
			if err != nil {
				return err
			}
			fmt.Printf("  latency at 70%% (%.0f/s) rep %d: %s\n", res.LatencyRate, rep, r.line())
			res.Latency = append(res.Latency, r)
		}
	}

	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*cm.out, *label+".capacity.json"), raw, 0o644)
}
