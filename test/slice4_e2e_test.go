package test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"notifplatform/internal/events"
)

func setProviderFailure(t *testing.T, channel string, status, delayMs int, remaining *int) {
	t.Helper()
	body := map[string]any{"channel": channel, "status": status, "delay_ms": delayMs}
	if remaining != nil {
		body["remaining"] = *remaining
	}
	if code, data := adminCall(t, http.MethodPost, apiURL+"/api/provider/failure", body, adminToken); code != 200 {
		t.Fatalf("set provider failure: %d %s", code, data)
	}
}

func clearProviderFailure(t *testing.T) {
	t.Helper()
	if code, data := adminCall(t, http.MethodPost, apiURL+"/api/provider/failure/clear", map[string]any{}, adminToken); code != 200 {
		t.Fatalf("clear provider failure: %d %s", code, data)
	}
}

// resetProvider clears injected provider failures now and again when the test ends.
func resetProvider(t *testing.T) {
	t.Helper()
	clearProviderFailure(t)
	t.Cleanup(func() { clearProviderFailure(t) })
}

func waitDeliveryStatus(t *testing.T, eventID string, ch events.Channel, want string, timeout time.Duration) deliveryState {
	t.Helper()
	pool := platformPool(t)
	var last deliveryState
	waitFor(t, string(ch)+" delivery to reach "+want, timeout, func() bool {
		last = readDelivery(t, pool, eventID, ch)
		return last.status == want
	})
	return last
}

func providerKeyCounts(t *testing.T, eventID string, ch events.Channel) (requests, effects int) {
	t.Helper()
	pool := providerPool(t)
	key := eventID + ":" + string(ch)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM requests WHERE key = $1`, key).Scan(&requests)
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM effects WHERE key = $1`, key).Scan(&effects)
	return
}

func replayCall(t *testing.T, body map[string]any) (int, map[string]int) {
	t.Helper()
	code, data := adminCall(t, http.MethodPost, apiURL+"/api/replay", body, adminToken)
	var res map[string]int
	_ = json.Unmarshal(data, &res)
	return code, res
}

// Two transient provider errors are retried with backoff and the third attempt succeeds.
func TestTransientProviderErrorsAreRetriedUntilSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetProvider(t)
	two := 2
	setProviderFailure(t, "webhook", 500, 0, &two)

	id := createPayment(t)
	waitDelivered(t, id, 3)

	s := readDelivery(t, platformPool(t), id, events.Webhook)
	if s.status != "DELIVERED" || s.attempts != 3 || s.lastError != "" {
		t.Errorf("webhook delivery = %+v, want DELIVERED at attempt 3 with the error cleared", s)
	}
	requests, effects := providerKeyCounts(t, id, events.Webhook)
	if requests != 3 || effects != 1 {
		t.Errorf("webhook requests=%d effects=%d, want 3 requests (2 failed + 1 ok) and exactly 1 effect", requests, effects)
	}
}

// Demo 3: the webhook keeps failing, exhausts its retries and is dead-lettered while
// email and push are unaffected; once the webhook recovers, a replay delivers it, and
// only the webhook is sent again.
func TestRetriesExhaustedThenReplayDelivers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetProvider(t)
	setProviderFailure(t, "webhook", 500, 0, nil)

	id := createPayment(t)
	dead := waitDeliveryStatus(t, id, events.Webhook, "DLQ", 40*time.Second)
	if dead.attempts != 5 || !strings.Contains(dead.lastError, "retries exhausted") || !strings.Contains(dead.lastError, "500") {
		t.Errorf("dead webhook delivery = %+v, want 5 attempts and a 'retries exhausted ... 500' reason", dead)
	}
	pool := platformPool(t)
	for _, ch := range []events.Channel{events.Email, events.Push} {
		if s := readDelivery(t, pool, id, ch); s.status != "DELIVERED" {
			t.Errorf("%s = %s, want DELIVERED: one failing channel must not affect the others", ch, s.status)
		}
	}
	if requests, effects := providerKeyCounts(t, id, events.Webhook); requests != 5 || effects != 0 {
		t.Errorf("webhook requests=%d effects=%d, want 5 failed requests and no effect", requests, effects)
	}

	// The operator can see it.
	resp, err := http.Get(apiURL + "/api/dead-letters")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var dl struct {
		Deliveries []struct {
			EventID string `json:"event_id"`
			Channel string `json:"channel"`
		} `json:"deliveries"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&dl)
	listed := false
	for _, d := range dl.Deliveries {
		if d.EventID == id && d.Channel == "webhook" {
			listed = true
		}
	}
	if !listed {
		t.Error("the dead-lettered webhook delivery is not listed by /api/dead-letters")
	}

	// Restore the webhook, then replay only that delivery.
	clearProviderFailure(t)
	code, res := replayCall(t, map[string]any{"event_id": id, "channel": "webhook"})
	if code != 200 || res["deliveries"] != 1 {
		t.Fatalf("replay = %d %v, want 200 with one delivery", code, res)
	}
	waitDeliveryStatus(t, id, events.Webhook, "DELIVERED", 30*time.Second)

	var replays int
	_ = pool.QueryRow(context.Background(),
		`SELECT replay_count FROM channel_deliveries WHERE event_id = $1 AND channel = 'webhook'`, id).Scan(&replays)
	if replays != 1 {
		t.Errorf("replay_count = %d, want 1", replays)
	}
	if _, effects := providerKeyCounts(t, id, events.Webhook); effects != 1 {
		t.Errorf("webhook effects after replay = %d, want exactly 1", effects)
	}
	for _, ch := range []events.Channel{events.Email, events.Push} {
		if _, effects := providerKeyCounts(t, id, ch); effects != 1 {
			t.Errorf("%s effects = %d after the webhook replay, want 1: the replay must not resend other channels", ch, effects)
		}
	}
}

// A 4xx error means the request itself is wrong, so retrying cannot help.
func TestPermanentProviderErrorGoesStraightToDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetProvider(t)
	setProviderFailure(t, "webhook", 400, 0, nil)

	id := createPayment(t)
	dead := waitDeliveryStatus(t, id, events.Webhook, "DLQ", 20*time.Second)
	if dead.attempts != 1 || !strings.Contains(dead.lastError, "permanent") || !strings.Contains(dead.lastError, "400") {
		t.Errorf("dead delivery = %+v, want 1 attempt and a permanent-error reason", dead)
	}
	if requests, _ := providerKeyCounts(t, id, events.Webhook); requests != 1 {
		t.Errorf("provider saw %d requests, want 1 (no retries for a permanent error)", requests)
	}

	// Replay by channel picks it up once the provider is fixed.
	clearProviderFailure(t)
	if code, res := replayCall(t, map[string]any{"channel": "webhook"}); code != 200 || res["deliveries"] < 1 {
		t.Fatalf("replay by channel = %d %v", code, res)
	}
	waitDeliveryStatus(t, id, events.Webhook, "DELIVERED", 30*time.Second)
}

func TestReplayEndpointRules(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	url := apiURL + "/api/replay"
	if code, _ := adminCall(t, http.MethodPost, url, map[string]any{"all": true}, ""); code != 401 {
		t.Errorf("no token: status %d, want 401", code)
	}
	if code, _ := adminCall(t, http.MethodPost, url, map[string]any{}, adminToken); code != 400 {
		t.Errorf("empty selector: status %d, want 400 (an empty body must not replay everything)", code)
	}
	if code, _ := adminCall(t, http.MethodPost, url, map[string]any{"channel": "carrier-pigeon"}, adminToken); code != 400 {
		t.Errorf("unknown channel: status %d, want 400", code)
	}
	if code, _ := adminCall(t, http.MethodPost, url, map[string]any{"event_id": "not-a-uuid"}, adminToken); code != 400 {
		t.Errorf("bad event id: status %d, want 400", code)
	}
}
