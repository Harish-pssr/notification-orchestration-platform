package health

import (
	"testing"
	"time"
)

func inst(name, role, state string, age float64) Instance {
	return Instance{Name: name, Role: role, State: state, AgeSeconds: age}
}

func statusOf(roles []Role, name string) Status {
	for _, r := range roles {
		if r.Name == name {
			return r.Status
		}
	}
	return ""
}

func TestSummarizeRoleStatus(t *testing.T) {
	stale := 10 * time.Second
	roles := Summarize([]Instance{
		inst("relay-a", "relay", StateRunning, 2),
		inst("relay-b", "relay", StateRunning, 400), // long gone: ignored for liveness
		inst("email-a", "consumer-email", StatePaused, 1),
		inst("email-b", "consumer-email", StatePaused, 3),
		inst("push-a", "consumer-push", StateRunning, 1),
		inst("push-b", "consumer-push", StatePaused, 1),      // mixed: still UP
		inst("hook-a", "consumer-webhook", StateRunning, 30), // only a stale instance
		inst("mystery", "billing", StateRunning, 1),          // not a known role
	}, stale)

	want := map[string]Status{
		"relay":            Up,
		"consumer-email":   Paused,
		"consumer-push":    Up,
		"consumer-webhook": Down,
	}
	for role, status := range want {
		if got := statusOf(roles, role); got != status {
			t.Errorf("%s = %s, want %s", role, got, status)
		}
	}
	if len(roles) != len(Roles) {
		t.Errorf("got %d roles, want the %d known ones", len(roles), len(Roles))
	}
}

func TestSummarizeWithNoInstancesMarksEveryRoleDown(t *testing.T) {
	for _, r := range Summarize(nil, 10*time.Second) {
		if r.Status != Down || r.Live != 0 {
			t.Errorf("%s = %s with %d live, want DOWN with none", r.Name, r.Status, r.Live)
		}
	}
}

func TestSummarizeCountsLiveInstancesAndKeepsStaleOnesListed(t *testing.T) {
	roles := Summarize([]Instance{
		inst("r1", "relay", StateRunning, 1),
		inst("r2", "relay", StateRunning, 2),
		inst("r3", "relay", StateRunning, 99),
	}, 10*time.Second)
	relay := roles[0]
	if relay.Live != 2 || len(relay.Instances) != 3 {
		t.Fatalf("live=%d listed=%d, want 2 live of 3 listed", relay.Live, len(relay.Instances))
	}
	if relay.Instances[2].Fresh {
		t.Error("the 99s-old instance must not be fresh")
	}
}

func TestInstanceExactlyAtTheThresholdIsStillAlive(t *testing.T) {
	roles := Summarize([]Instance{inst("r1", "relay", StateRunning, 10)}, 10*time.Second)
	if roles[0].Status != Up {
		t.Errorf("an instance exactly at the threshold should still count as alive, got %s", roles[0].Status)
	}
}
