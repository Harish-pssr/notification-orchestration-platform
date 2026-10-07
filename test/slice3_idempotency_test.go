package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifplatform/internal/faults"
)

const (
	adminToken  = "dev-token" // set by docker-compose.dev.yml
	providerURL = "http://localhost:9090"
	// Components poll fault_config about once a second; wait this long after
	// changing a fault before relying on it.
	faultPropagation = 2500 * time.Millisecond
)

func adminCall(t *testing.T, method, url string, body any, token string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func setFault(t *testing.T, point string, body map[string]any) {
	t.Helper()
	if code, data := adminCall(t, http.MethodPut, apiURL+"/api/faults/"+point, body, adminToken); code != 200 {
		t.Fatalf("set fault %s: %d %s", point, code, data)
	}
}

func clearFaults(t *testing.T) {
	t.Helper()
	if code, _ := adminCall(t, http.MethodDelete, apiURL+"/api/faults", nil, adminToken); code != 204 {
		t.Fatalf("clear faults: status %d", code)
	}
}

func setProviderDedupe(t *testing.T, enabled bool) {
	t.Helper()
	if code, data := adminCall(t, http.MethodPost, apiURL+"/api/provider/dedupe", map[string]bool{"enabled": enabled}, adminToken); code != 200 {
		t.Fatalf("set provider dedupe: %d %s", code, data)
	}
}

// resetFaults clears every fault and restores provider idempotency, now and again at
// the end of the test, so a failing test cannot leave the stack broken.
func resetFaults(t *testing.T) {
	t.Helper()
	restore := func() {
		clearFaults(t)
		setProviderDedupe(t, true)
		clearProviderFailure(t)
		time.Sleep(faultPropagation)
	}
	restore()
	t.Cleanup(restore)
}

type providerCounts struct{ requests, effects, distinctEffectKeys int }

func countProvider(t *testing.T, eventID string) providerCounts {
	t.Helper()
	var c providerCounts
	pool := providerPool(t)
	ctx := context.Background()
	like := eventID + ":%"
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM requests WHERE key LIKE $1`, like).Scan(&c.requests)
	_ = pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT key) FROM effects WHERE key LIKE $1`, like).Scan(&c.effects, &c.distinctEffectKeys)
	return c
}

func ignoredDuplicates(t *testing.T, eventID string) int {
	t.Helper()
	var n int
	_ = platformPool(t).QueryRow(context.Background(),
		`SELECT coalesce(sum(duplicate_count), 0) FROM channel_deliveries WHERE event_id = $1`, eventID).Scan(&n)
	return n
}

// waitFor polls cond until it is true or the timeout passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func createPayment(t *testing.T) string {
	t.Helper()
	r := postEvent(t, "/api/events/payment-succeeded", uuid.NewString(), payment())
	if r.Status != 201 {
		t.Fatalf("create event: %d %v", r.Status, r.Body)
	}
	return r.Body["event_id"].(string)
}

// Demo 4: the relay delivers every message twice. Each duplicate must be absorbed
// without a second side effect. Which layer absorbs it depends on timing since
// consumers process messages concurrently (D15): a copy that arrives after the first
// finished is ignored by the consumer; a copy handled while the first is still in
// progress reaches the provider, which suppresses it by its idempotency key.
func TestDuplicateDeliveryIsAbsorbedWithoutASecondSideEffect(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	one := 1
	setFault(t, "relay.after_publish", map[string]any{"action": "duplicate", "remaining": one})
	time.Sleep(faultPropagation)

	id := createPayment(t)
	waitDelivered(t, id, 3)
	waitFor(t, "all 3 duplicates to be absorbed", 20*time.Second, func() bool {
		c := countProvider(t, id)
		return ignoredDuplicates(t, id)+int(c.requests-c.effects) == 3
	})

	c := countProvider(t, id)
	if c.effects != 3 || c.distinctEffectKeys != 3 {
		t.Errorf("effects=%d distinct=%d, want 3 and 3 (no duplicate side effects)", c.effects, c.distinctEffectKeys)
	}
}

// The consumer shortcut is only an optimisation. With it switched off, duplicates reach
// the provider, which must suppress them by itself: 6 requests, still 3 effects.
func TestProviderAloneSuppressesDuplicateSideEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	setFault(t, "consumer.skip_delivered_check", map[string]any{"action": "disable"})
	setFault(t, "relay.after_publish", map[string]any{"action": "duplicate", "remaining": 1})
	time.Sleep(faultPropagation)

	id := createPayment(t)
	waitDelivered(t, id, 3)
	waitFor(t, "6 provider requests", 20*time.Second, func() bool { return countProvider(t, id).requests == 6 })

	c := countProvider(t, id)
	if c.effects != 3 || c.distinctEffectKeys != 3 {
		t.Errorf("effects=%d distinct=%d, want 3 and 3: a duplicate side effect got through", c.effects, c.distinctEffectKeys)
	}
	if n := ignoredDuplicates(t, id); n != 0 {
		t.Errorf("consumers ignored %d duplicates with the shortcut off, want 0", n)
	}
}

// Negative control (D5): with BOTH protections off the same duplicates must produce
// duplicate side effects. If this test failed, the zero in the other tests would mean
// nothing, because the counters could not detect duplicates at all.
func TestNegativeControlDuplicateEffectsAreDetectedWithoutProtection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	setProviderDedupe(t, false)
	setFault(t, "consumer.skip_delivered_check", map[string]any{"action": "disable"})
	setFault(t, "relay.after_publish", map[string]any{"action": "duplicate", "remaining": 1})
	time.Sleep(faultPropagation)

	id := createPayment(t)
	// Remove this experiment's rows afterwards so the dashboard's global counters
	// stay honest for the real runs.
	t.Cleanup(func() {
		pool := providerPool(t)
		for _, table := range []string{"effects", "requests", "idempotency_keys"} {
			_, _ = pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE key LIKE $1", id+":%")
		}
	})
	waitDelivered(t, id, 3)
	waitFor(t, "6 effects", 20*time.Second, func() bool { return countProvider(t, id).effects == 6 })

	c := countProvider(t, id)
	if c.effects != 6 || c.distinctEffectKeys != 3 {
		t.Errorf("effects=%d distinct=%d, want 6 and 3: the control failed to show duplicate effects", c.effects, c.distinctEffectKeys)
	}
}

// Demo 5 through the fault framework: the relay is killed (os.Exit) right after the
// broker accepted the publish. Docker restarts it, the lease expires, the event is
// published again and still produces exactly one effect per channel.
func TestInjectedRelayCrashAfterPublishIsRecovered(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	setFault(t, "relay.after_publish", map[string]any{"action": "crash", "remaining": 1})
	time.Sleep(faultPropagation)

	id := createPayment(t)
	waitDelivered(t, id, 3) // allows for the restart and the 5s dev lease

	var attempts int
	_ = platformPool(t).QueryRow(context.Background(), `SELECT attempts FROM outbox_events WHERE id = $1`, id).Scan(&attempts)
	if attempts < 2 {
		t.Errorf("attempts = %d, want at least 2 (the first relay died before finishing)", attempts)
	}
	if c := countProvider(t, id); c.effects != 3 || c.distinctEffectKeys != 3 {
		t.Errorf("effects=%d distinct=%d, want 3 and 3", c.effects, c.distinctEffectKeys)
	}
}

func TestAdminEndpointsRequireTheToken(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	url := apiURL + "/api/faults"
	if code, _ := adminCall(t, http.MethodGet, url, nil, ""); code != 401 {
		t.Errorf("no token: status %d, want 401", code)
	}
	if code, _ := adminCall(t, http.MethodGet, url, nil, "wrong"); code != 401 {
		t.Errorf("wrong token: status %d, want 401", code)
	}
	if code, _ := adminCall(t, http.MethodGet, url, nil, adminToken); code != 200 {
		t.Errorf("valid token: status %d, want 200", code)
	}
	// A typo in the injection point must be rejected, not silently ignored.
	if code, _ := adminCall(t, http.MethodPut, apiURL+"/api/faults/relay.after_publsh", map[string]any{"action": "crash"}, adminToken); code != 400 {
		t.Errorf("unknown point: status %d, want 400", code)
	}
	if code, _ := adminCall(t, http.MethodPut, apiURL+"/api/faults/relay.after_publish", map[string]any{"action": "disable"}, adminToken); code != 400 {
		t.Errorf("unsupported action: status %d, want 400", code)
	}
	if code, _ := adminCall(t, http.MethodPost, providerURL+"/admin/dedupe", map[string]bool{"enabled": false}, ""); code != 401 {
		t.Errorf("provider admin without token: status %d, want 401", code)
	}
}

// "Crash once" must fire in exactly one process even if many race for it (D8).
func TestLimitedFaultFiresExactlyItsCountAcrossConcurrentCallers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	_, pool := isolatedStore(t)
	ctx := context.Background()
	reg := faults.New(pool, true, time.Second, slog.Default())

	left := 3
	if err := reg.Set(ctx, faults.Fault{Point: "relay.after_publish", Action: faults.Duplicate, Remaining: &left, Probability: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	var fired atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := reg.Fire(ctx, "relay.after_publish"); ok {
				fired.Add(1)
			}
		}()
	}
	wg.Wait()
	if fired.Load() != 3 {
		t.Fatalf("fault fired %d times, want exactly 3", fired.Load())
	}
}

func TestDisabledRegistryNeverFiresAndCrashUsesTheInjectedExit(t *testing.T) {
	var nilReg *faults.Registry
	if _, ok := nilReg.Fire(context.Background(), "relay.after_publish"); ok {
		t.Error("a nil registry fired a fault")
	}
	if _, ok := nilReg.Active("consumer.skip_delivered_check"); ok {
		t.Error("a nil registry reported an active fault")
	}

	reg := faults.New(nil, false, time.Second, slog.Default())
	if reg.Enabled() {
		t.Error("registry should be disabled")
	}
	code := -1
	reg.SetExit(func(c int) { code = c })
	reg.Crash()
	if code != 137 {
		t.Errorf("crash exit code = %d, want 137", code)
	}
}
