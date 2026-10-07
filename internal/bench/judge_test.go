package bench

import (
	"strings"
	"testing"
)

func healthy() (RunSummary, Backlog, Analysis) {
	return RunSummary{TargetRate: 200, GeneratorOK: true, APIP99Ms: 20},
		Backlog{Events: 40},
		Analysis{InWindow: 4000, Completed: 4000}
}

func TestJudgeAcceptsAHealthyStep(t *testing.T) {
	s, b, a := healthy()
	if v := Judge(s, b, a); !v.Sustained {
		t.Fatalf("a healthy step was rejected: %s", v.Reason)
	}
}

func TestJudgeRejectsEachFailureWithItsReason(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RunSummary, *Backlog, *Analysis)
		want   string
	}{
		{"generator lagged", func(s *RunSummary, _ *Backlog, _ *Analysis) { s.GeneratorOK = false; s.LagP99Ms = 90 }, "generator did not keep up"},
		{"API errors", func(s *RunSummary, _ *Backlog, _ *Analysis) { s.Errors = 3 }, "refused or failed"},
		{"API rejections", func(s *RunSummary, _ *Backlog, _ *Analysis) { s.Rejected = 1 }, "refused or failed"},
		{"slow API", func(s *RunSummary, _ *Backlog, _ *Analysis) { s.APIP99Ms = 1500 }, "API p99"},
		{"unbounded backlog", func(_ *RunSummary, b *Backlog, _ *Analysis) { b.Events = 401 }, "backlog at end"},
		{"events not delivered", func(_ *RunSummary, _ *Backlog, a *Analysis) { a.Completed = 3900 }, "fully delivered"},
	}
	for _, c := range cases {
		s, b, a := healthy()
		c.mutate(&s, &b, &a)
		v := Judge(s, b, a)
		if v.Sustained || !strings.Contains(v.Reason, c.want) {
			t.Errorf("%s: verdict %+v, want rejection mentioning %q", c.name, v, c.want)
		}
	}
}

// The backlog bound is two seconds of arrivals: exactly at the bound is still acceptable.
func TestJudgeBacklogBoundIsInclusive(t *testing.T) {
	s, b, a := healthy()
	b.Events = 400 // 2 * 200
	if v := Judge(s, b, a); !v.Sustained {
		t.Errorf("backlog exactly at the bound was rejected: %s", v.Reason)
	}
}

func TestJudgeIgnoresDeliveryCompletenessWhenTheWindowIsEmpty(t *testing.T) {
	s, b, _ := healthy()
	if v := Judge(s, b, Analysis{}); !v.Sustained {
		t.Errorf("an empty window must not fail the completeness rule: %s", v.Reason)
	}
}
