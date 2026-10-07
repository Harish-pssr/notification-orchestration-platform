package bench

import (
	"math/rand/v2"
	"testing"
	"time"
)

func goodCounts() Counts {
	return Counts{
		Events: 100, Deliveries: 300, Delivered: 300, Effects: 300,
		ProviderRequests: 340, DistinctRequestKeys: 300, DistinctEffectKeys: 300,
		DuplicateDeliveries: 40, SuppressedByProvider: 40,
	}
}

func TestJudgeReliabilityPassesAcleanRun(t *testing.T) {
	if v := JudgeReliability(100, 3, goodCounts(), false); !v.Pass {
		t.Fatalf("a clean run failed: %v", v.Failures)
	}
}

func TestJudgeReliabilityCatchesEveryKindOfLoss(t *testing.T) {
	cases := map[string]func(c *Counts){
		"an event the API accepted is missing":   func(c *Counts) { c.Events = 99 },
		"a delivery is still unfinished":         func(c *Counts) { c.NotFinished, c.Delivered = 1, 299 },
		"a delivery was dead-lettered":           func(c *Counts) { c.DeadLettered, c.Delivered = 1, 299; c.Effects = 299; c.DistinctEffectKeys = 299 },
		"a duplicate side effect got through":    func(c *Counts) { c.Effects, c.DuplicateSideEffects = 301, 1 },
		"a delivered row has no provider effect": func(c *Counts) { c.Effects, c.DistinctEffectKeys = 299, 299 },
	}
	for name, mutate := range cases {
		c := goodCounts()
		mutate(&c)
		if v := JudgeReliability(100, 3, c, false); v.Pass {
			t.Errorf("%s, but the run passed", name)
		}
	}
}

func TestNegativeControlMustShowDuplicateSideEffects(t *testing.T) {
	c := goodCounts()
	if v := JudgeReliability(100, 3, c, true); v.Pass {
		t.Error("a control with no duplicate side effects passed: the counters were not shown to detect any")
	}
	c.Effects, c.DuplicateSideEffects = 320, 20
	if v := JudgeReliability(100, 3, c, true); !v.Pass {
		t.Errorf("a control that produced duplicates failed: %v", v.Failures)
	}
	c.NotFinished = 1
	if v := JudgeReliability(100, 3, c, true); v.Pass {
		t.Error("a control that lost a delivery passed")
	}
}

func TestCrashTimesAreSortedSpacedAndInsideTheWindow(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	window, gap := 10*time.Minute, time.Minute
	for i := 0; i < 200; i++ {
		ts := CrashTimes(rng, 3, window, gap)
		if len(ts) != 3 {
			t.Fatalf("got %d times", len(ts))
		}
		for j, v := range ts {
			if v < window/10 || v > window*9/10 {
				t.Fatalf("time %v outside the middle 80%% of %v", v, window)
			}
			if j > 0 && v-ts[j-1] < gap {
				t.Fatalf("times %v closer than %v", ts, gap)
			}
		}
	}
}

func TestCrashTimesFallBackToEvenSpacingWhenTheGapIsTight(t *testing.T) {
	ts := CrashTimes(rand.New(rand.NewPCG(3, 4)), 3, 10*time.Minute, 150*time.Second)
	if len(ts) != 3 || ts[1]-ts[0] < 150*time.Second {
		t.Fatalf("fallback gave %v", ts)
	}
}

func TestAnalyzeRecoveryMeasuresPeaksAndRecoveryTime(t *testing.T) {
	// 60 events/s with 3 deliveries each; delivered grows 180/s until the stop at t=10,
	// nothing is delivered during the outage 10..20, then it drains at 600/s.
	var s []Sample
	delivered, waiting := int64(0), int64(20)
	for t := 0; t <= 40; t++ {
		switch {
		case t < 10:
			delivered += 180
		case t < 20:
			waiting += 180
		case waiting > 20:
			drain := min(waiting-20, 420) // 600/s out, 180/s still arriving
			waiting -= drain
			delivered += drain + 180
		default:
			delivered += 180
		}
		s = append(s, Sample{T: float64(t), Delivered: delivered, DeliveriesWaiting: waiting, OutboxWaiting: waiting / 3})
	}
	r := AnalyzeRecovery(s, 10, 20, 60, 3)
	if r.OutageS != 10 {
		t.Errorf("outage = %v, want 10", r.OutageS)
	}
	if r.PeakDeliveriesWaiting != 20+10*180 {
		t.Errorf("peak deliveries waiting = %d, want 1,820", r.PeakDeliveriesWaiting)
	}
	if r.SteadyDeliveriesWait != 20 {
		t.Errorf("steady waiting = %v, want 20", r.SteadyDeliveriesWait)
	}
	if r.RecoveryS < 0 || r.RecoveryS > 10 {
		t.Errorf("recovery = %v s, want a few seconds (the backlog drains at 420/s net)", r.RecoveryS)
	}
	// 600/s while draining, averaged over 5 s windows that include the tail of the drain.
	if r.PeakDeliveryRatePerS < 400 || r.PeakDeliveryRatePerS > 600 {
		t.Errorf("peak delivery rate = %v, want between 400 and 600", r.PeakDeliveryRatePerS)
	}
}

func TestAnalyzeRecoveryReportsNeverRecovered(t *testing.T) {
	s := []Sample{{T: 0, DeliveriesWaiting: 0}, {T: 5, DeliveriesWaiting: 5000}, {T: 10, DeliveriesWaiting: 9000}, {T: 15, DeliveriesWaiting: 9000}}
	if r := AnalyzeRecovery(s, 5, 10, 50, 3); r.RecoveryS != -1 {
		t.Errorf("recovery = %v, want -1 (never recovered)", r.RecoveryS)
	}
}
