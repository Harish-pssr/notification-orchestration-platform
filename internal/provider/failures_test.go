package provider

import (
	"testing"

	"notifplatform/internal/events"
)

func TestProbabilisticFailureAffectsOnlyTheChosenFraction(t *testing.T) {
	f := newFailures()
	draws := []float64{0.01, 0.50, 0.019, 0.99, 0.02} // against probability 0.02
	i := 0
	f.rnd = func() float64 { v := draws[i%len(draws)]; i++; return v }
	f.set(events.Webhook, failure{Status: 500, Probability: 0.02})

	var affected []bool
	for range draws {
		affected = append(affected, f.take(events.Webhook) != nil)
	}
	want := []bool{true, false, true, false, false} // draws below 0.02 are affected
	for k := range want {
		if affected[k] != want[k] {
			t.Errorf("request %d (draw %v): affected=%v, want %v", k, draws[k], affected[k], want[k])
		}
	}
}

func TestEscapedRequestsDoNotUseUpALimitedFailure(t *testing.T) {
	f := newFailures()
	f.rnd = func() float64 { return 0.9 } // always escapes a 0.5 probability
	two := 2
	f.set(events.Email, failure{Status: 503, Probability: 0.5, Remaining: &two})
	for i := 0; i < 10; i++ {
		if f.take(events.Email) != nil {
			t.Fatal("a request that escaped the probability was affected")
		}
	}
	if *f.byChan[events.Email].Remaining != 2 {
		t.Errorf("remaining = %d, want 2: escaped requests must not consume the count", *f.byChan[events.Email].Remaining)
	}
}

func TestDefaultProbabilityAffectsEveryRequest(t *testing.T) {
	f := newFailures()
	f.rnd = func() float64 { t.Fatal("no random draw is needed when every request is affected"); return 0 }
	f.set(events.Push, failure{Status: 500}) // probability unset
	for i := 0; i < 5; i++ {
		if f.take(events.Push) == nil {
			t.Fatalf("request %d unaffected, want every request affected by default", i)
		}
	}
}
