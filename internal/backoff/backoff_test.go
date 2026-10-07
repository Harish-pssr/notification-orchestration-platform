package backoff

import (
	"math/rand/v2"
	"testing"
	"time"
)

func zero() float64      { return 0 }
func almostOne() float64 { return 0.999999999 }

func TestDelayGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	base, max := time.Second, 30*time.Second
	for attempt, nominal := range map[int]time.Duration{
		1: 1 * time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
		4: 8 * time.Second, 5: 16 * time.Second,
	} {
		lo := Delay(attempt, base, max, zero)
		hi := Delay(attempt, base, max, almostOne)
		if lo != nominal/2 {
			t.Errorf("attempt %d: lowest delay %v, want %v", attempt, lo, nominal/2)
		}
		if hi < lo || hi > nominal {
			t.Errorf("attempt %d: highest delay %v outside [%v, %v]", attempt, hi, lo, nominal)
		}
	}
}

func TestDelayIsCapped(t *testing.T) {
	for _, attempt := range []int{6, 10, 100} {
		if got := Delay(attempt, time.Second, 30*time.Second, almostOne); got > 30*time.Second {
			t.Errorf("attempt %d: %v exceeds the cap", attempt, got)
		}
	}
}

func TestDelayNeverBelowHalfNominalForRandomDraws(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2)) // seeded: reproducible
	for i := 0; i < 1000; i++ {
		if got := Delay(3, time.Second, time.Minute, r.Float64); got < 2*time.Second || got > 4*time.Second {
			t.Fatalf("draw %d: %v outside [2s, 4s]", i, got)
		}
	}
}

func TestDelayTreatsNonPositiveAttemptAsFirst(t *testing.T) {
	if got := Delay(0, time.Second, time.Minute, zero); got != 500*time.Millisecond {
		t.Errorf("got %v", got)
	}
}
