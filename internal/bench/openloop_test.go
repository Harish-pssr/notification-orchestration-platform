package bench

import (
	"math"
	"net/http"
	"testing"
	"time"
)

func near(a, b, tol float64) bool { return math.Abs(a-b) <= tol }

func TestIntendedTimesAreEvenlySpacedAndDoNotDrift(t *testing.T) {
	start := time.Unix(1000, 0)
	// 250 events per second: one every 4 ms, however far into the run we are.
	for _, i := range []int{0, 1, 2, 1000, 999999} {
		got := intendedAt(start, i, 250).Sub(start)
		want := time.Duration(i) * 4 * time.Millisecond
		if d := got - want; d < -time.Microsecond || d > time.Microsecond {
			t.Errorf("event %d intended %v after start, want %v", i, got, want)
		}
	}
	// A rate that does not divide a second evenly must still not accumulate error.
	got := intendedAt(start, 3_000_000, 3).Sub(start)
	if got != 1_000_000*time.Second {
		t.Errorf("3,000,000th event at 3/s intended at %v, want exactly 1,000,000s", got)
	}
}

func TestPercentileInterpolatesAndHandlesEdges(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("empty = %v, want 0", got)
	}
	if got := percentile([]float64{7}, 0.99); got != 7 {
		t.Errorf("single value = %v, want 7", got)
	}
	v := []float64{10, 20, 30, 40, 50}
	cases := map[float64]float64{0: 10, 0.5: 30, 1: 50, 0.25: 20, 0.9: 46}
	for p, want := range cases {
		if got := percentile(v, p); !near(got, want, 1e-9) {
			t.Errorf("p%.2f = %v, want %v", p, got, want)
		}
	}
}

func TestSummarizeCountsOutcomesAndMeasuresFromIntendedTime(t *testing.T) {
	ok := func(api, lag time.Duration) sample {
		return sample{apiLatency: api, lag: lag, status: http.StatusCreated, sent: true}
	}
	samples := []sample{
		ok(10*time.Millisecond, time.Millisecond),
		ok(20*time.Millisecond, time.Millisecond),
		ok(30*time.Millisecond, 2*time.Millisecond),
		{apiLatency: 5 * time.Millisecond, lag: time.Millisecond, status: http.StatusUnprocessableEntity, sent: true},
		{apiLatency: time.Second, lag: time.Millisecond, status: 0, sent: true}, // transport error
		{}, // never sent (skipped)
	}
	s := summarize("r1", 10, 2*time.Second, samples, 1, 4)

	if s.Planned != 6 || s.Sent != 5 || s.Created != 3 || s.Rejected != 1 || s.Errors != 1 || s.Skipped != 1 {
		t.Fatalf("counts wrong: %+v", s)
	}
	if !near(s.AchievedRate, 1.5, 1e-9) {
		t.Errorf("achieved rate = %v, want 3 created / 2s = 1.5", s.AchievedRate)
	}
	// A transport error has no meaningful latency: it must not pollute the percentiles.
	if s.APIMaxMs > 100 {
		t.Errorf("api max %v ms includes the failed request's 1000 ms", s.APIMaxMs)
	}
	if s.GeneratorOK {
		t.Error("a run that skipped a request must not claim the generator kept up")
	}
}

func TestGeneratorOKRequiresNoSkipsAndLowLag(t *testing.T) {
	fast := []sample{{apiLatency: time.Millisecond, lag: time.Millisecond, status: http.StatusCreated, sent: true}}
	if !summarize("r", 1, time.Second, fast, 0, 1).GeneratorOK {
		t.Error("a punctual, complete run should report the generator kept up")
	}
	late := []sample{{apiLatency: time.Millisecond, lag: 200 * time.Millisecond, status: http.StatusCreated, sent: true}}
	if summarize("r", 1, time.Second, late, 0, 1).GeneratorOK {
		t.Error("a run whose p99 schedule lag is 200 ms must not claim the generator kept up")
	}
}
