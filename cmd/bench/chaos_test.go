package main

import (
	"math/rand/v2"
	"testing"
	"time"
)

func TestPlanChaosHasTheRequestedCountsInTimeOrderInsideTheLoad(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	window := 20 * time.Minute
	plan := planChaos(rng, window, 15, 6, 2)

	counts := map[string]int{}
	for i, ev := range plan {
		counts[ev.Kind]++
		if ev.At < window/10 || ev.At > window*9/10 {
			t.Errorf("event %d at %s is outside the middle 80%% of the load", i, ev.At)
		}
		if i > 0 && ev.At < plan[i-1].At {
			t.Errorf("events %d and %d are out of time order", i-1, i)
		}
	}
	if counts[chaosRelay] != 15 || counts[chaosConsumer] != 6 || counts[chaosPostgres] != 2 {
		t.Errorf("counts = %v, want 15 relay, 6 consumer, 2 postgres", counts)
	}
}

func TestPlanChaosKeepsPostgresKillsFarApart(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	var last time.Duration
	first := true
	for _, ev := range planChaos(rng, 20*time.Minute, 0, 0, 3) {
		if !first && ev.At-last < 90*time.Second {
			t.Errorf("postgres kills only %s apart, want at least 90s", ev.At-last)
		}
		last, first = ev.At, false
	}
}

func TestPlanChaosWithNothingRequestedIsEmpty(t *testing.T) {
	if plan := planChaos(rand.New(rand.NewPCG(5, 6)), time.Minute, 0, 0, 0); len(plan) != 0 {
		t.Errorf("plan = %v, want none", plan)
	}
}
