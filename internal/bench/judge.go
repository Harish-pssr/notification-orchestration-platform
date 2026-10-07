package bench

import "fmt"

// Verdict is whether the system sustained a given arrival rate during a step, and why.
type Verdict struct {
	Sustained bool   `json:"sustained"`
	Reason    string `json:"reason"` // empty when sustained
}

// Judge decides whether a step at a given rate was sustainable (D13). All of these must
// hold; the first that fails is reported:
//
//  1. the generator kept up (it must never be the bottleneck) and sent everything;
//  2. the API accepted every request (no errors, no rejections);
//  3. the API answered quickly: p99 from intended send time under one second;
//  4. the backlog at the end of the load is bounded: no more unfinished events than two
//     seconds' worth of arrivals. A system running below capacity keeps only a little
//     work in flight (arrival rate times latency); a system above capacity falls behind
//     by (rate - capacity) per second, so its backlog grows without bound;
//  5. at least 99% of the events in the measurement window were fully delivered.
func Judge(sum RunSummary, backlogAtEnd Backlog, a Analysis) Verdict {
	fail := func(format string, args ...any) Verdict {
		return Verdict{Sustained: false, Reason: fmt.Sprintf(format, args...)}
	}
	switch {
	case !sum.GeneratorOK:
		return fail("generator did not keep up (skipped %d, p99 schedule lag %.1f ms)", sum.Skipped, sum.LagP99Ms)
	case sum.Errors > 0 || sum.Rejected > 0:
		return fail("API refused or failed requests (%d errors, %d rejected)", sum.Errors, sum.Rejected)
	case sum.APIP99Ms >= 1000:
		return fail("API p99 latency %.0f ms is not under 1 s", sum.APIP99Ms)
	case float64(backlogAtEnd.Events) > 2*sum.TargetRate:
		return fail("backlog at end of load is %d events, above the bound of %.0f (2 s of arrivals)", backlogAtEnd.Events, 2*sum.TargetRate)
	case a.InWindow > 0 && float64(a.Completed) < 0.99*float64(a.InWindow):
		return fail("only %d of %d events in the window were fully delivered", a.Completed, a.InWindow)
	}
	return Verdict{Sustained: true}
}
