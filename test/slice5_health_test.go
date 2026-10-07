package test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"notifplatform/internal/events"
	"notifplatform/internal/health"
)

type healthView struct {
	Roles []struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Live   int    `json:"live"`
	} `json:"roles"`
	Backlog struct {
		Outbox     int64            `json:"outbox"`
		Deliveries map[string]int64 `json:"deliveries"`
	} `json:"backlog"`
	Broker struct {
		Channels map[string]struct {
			Pending    uint64 `json:"pending"`
			AckPending int    `json:"ack_pending"`
		} `json:"channels"`
	} `json:"broker"`
}

func getHealth(t *testing.T) healthView {
	t.Helper()
	resp, err := http.Get(apiURL + "/api/health")
	if err != nil {
		t.Fatalf("GET /api/health: %v", err)
	}
	defer resp.Body.Close()
	var v healthView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	return v
}

func (v healthView) status(role string) string {
	for _, r := range v.Roles {
		if r.Name == role {
			return r.Status
		}
	}
	return ""
}

func waitRoleStatus(t *testing.T, role, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, role+" to be "+want, timeout, func() bool { return getHealth(t).status(role) == want })
}

func TestEveryRoleReportsUpWithItsInstances(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	for _, role := range health.Roles {
		waitRoleStatus(t, role, "UP", 20*time.Second)
	}
	v := getHealth(t)
	for _, r := range v.Roles {
		if r.Live < 1 {
			t.Errorf("role %s has %d live instances, want at least 1", r.Name, r.Live)
		}
	}
	for _, ch := range events.AllChannels {
		if _, ok := v.Broker.Channels[string(ch)]; !ok {
			t.Errorf("no broker backlog reported for channel %s", ch)
		}
	}
}

// Demo 2: with the relay paused, events commit but stay safely in the outbox; resuming
// publishes them and the system catches up on its own.
func TestPausedRelayHoldsEventsInTheOutboxThenRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	waitRoleStatus(t, "relay", "UP", 20*time.Second)

	setFault(t, "relay.pause", map[string]any{"action": "pause"})
	waitRoleStatus(t, "relay", "PAUSED", 15*time.Second)

	id := createPayment(t)
	time.Sleep(3 * time.Second) // plenty of time for a running relay to have published it

	var status string
	var attempts int
	if err := platformPool(t).QueryRow(context.Background(),
		`SELECT status, attempts FROM outbox_events WHERE id = $1`, id).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || attempts != 0 {
		t.Fatalf("while the relay is paused the event is %s with %d attempts, want PENDING with 0", status, attempts)
	}
	if v := getHealth(t); v.Backlog.Outbox < 1 {
		t.Errorf("outbox backlog = %d while an event is waiting, want at least 1", v.Backlog.Outbox)
	}

	clearFaults(t) // resume
	waitDelivered(t, id, 3)
	waitRoleStatus(t, "relay", "UP", 15*time.Second)
}

// With one consumer paused, its messages wait in the broker (visible as backlog) while
// the other channels carry on; resuming drains the backlog.
func TestPausedConsumerBuildsBrokerBacklogThenDrains(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	waitRoleStatus(t, "consumer-webhook", "UP", 20*time.Second)

	setFault(t, "consumer.webhook.pause", map[string]any{"action": "pause"})
	waitRoleStatus(t, "consumer-webhook", "PAUSED", 15*time.Second)

	id := createPayment(t)
	pool := platformPool(t)
	waitFor(t, "email and push to deliver while webhook is paused", 20*time.Second, func() bool {
		return readDelivery(t, pool, id, events.Email).status == "DELIVERED" &&
			readDelivery(t, pool, id, events.Push).status == "DELIVERED"
	})
	time.Sleep(2 * time.Second)
	if s := readDelivery(t, pool, id, events.Webhook).status; s != "PENDING" {
		t.Fatalf("webhook delivery = %s while its consumer is paused, want PENDING", s)
	}
	waitFor(t, "the broker to report a webhook backlog", 10*time.Second, func() bool {
		return getHealth(t).Broker.Channels["webhook"].Pending >= 1
	})

	clearFaults(t) // resume
	waitDeliveryStatus(t, id, events.Webhook, "DELIVERED", 30*time.Second)
	waitRoleStatus(t, "consumer-webhook", "UP", 15*time.Second)
}

// Age is measured with the database clock and a role with no fresh heartbeat is DOWN.
func TestRoleGoesDownWhenHeartbeatsStop(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, pool := isolatedStore(t)
	ctx := context.Background()
	stale := 10 * time.Second

	status := func() health.Status {
		list, err := health.List(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range health.Summarize(list, stale) {
			if r.Name == "relay" {
				return r.Status
			}
		}
		return ""
	}

	if got := status(); got != health.Down {
		t.Fatalf("relay with no instances = %s, want DOWN", got)
	}
	if err := health.Beat(ctx, pool, "relay-1", "relay", health.StateRunning); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != health.Up {
		t.Fatalf("relay after a heartbeat = %s, want UP", got)
	}
	if err := health.Beat(ctx, pool, "relay-1", "relay", health.StatePaused); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != health.Paused {
		t.Fatalf("relay reporting paused = %s, want PAUSED", got)
	}
	// The instance stops beating: its last heartbeat becomes 30 seconds old.
	if _, err := pool.Exec(ctx, `UPDATE workers SET last_seen = now() - interval '30 seconds' WHERE name = 'relay-1'`); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != health.Down {
		t.Fatalf("relay with a 30s-old heartbeat = %s, want DOWN", got)
	}
}
