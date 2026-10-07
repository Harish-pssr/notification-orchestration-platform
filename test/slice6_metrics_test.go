package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

const prometheusURL = "http://localhost:9091" // published by docker-compose.dev.yml

func scrape(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("scrape %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// metricSum adds up every sample of a metric whose label text contains labelPart
// ("" matches all), read from a Prometheus text exposition.
func metricSum(body, name, labelPart string) float64 {
	var total float64
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if !strings.HasPrefix(rest, " ") && !strings.HasPrefix(rest, "{") {
			continue // a different metric that merely shares the prefix
		}
		if labelPart != "" && !strings.Contains(rest, labelPart) {
			continue
		}
		fields := strings.Fields(line)
		if v, err := strconv.ParseFloat(fields[len(fields)-1], 64); err == nil {
			total += v
		}
	}
	return total
}

func TestPrometheusScrapesEveryProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	want := []string{"api", "provider", "relay", "consumer-email", "consumer-push", "consumer-webhook"}
	waitFor(t, "every scrape target to be up", 40*time.Second, func() bool {
		resp, err := http.Get(prometheusURL + "/api/v1/targets")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var out struct {
			Data struct {
				ActiveTargets []struct {
					Labels map[string]string `json:"labels"`
					Health string            `json:"health"`
				} `json:"activeTargets"`
			} `json:"data"`
		}
		if json.NewDecoder(resp.Body).Decode(&out) != nil {
			return false
		}
		up := map[string]bool{}
		for _, tg := range out.Data.ActiveTargets {
			if tg.Health == "up" {
				up[tg.Labels["job"]] = true
			}
		}
		for _, job := range want {
			if !up[job] {
				return false
			}
		}
		return true
	})
}

// Counters recorded by the API must match exactly what the test did.
func TestAPICountersMatchWhatWasDone(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	read := func(name, labels string) float64 { return metricSum(scrape(t, apiURL+"/metrics"), name, labels) }

	createdBefore := read("notif_events_created_total", `event_type="PaymentSucceeded"`)
	replaysBefore := read("notif_api_idempotent_replays_total", "")
	badBefore := read("notif_api_rejected_requests_total", `reason="bad_request"`)
	reuseBefore := read("notif_api_rejected_requests_total", `reason="key_reuse"`)

	key := uuid.NewString()
	for i := 0; i < 4; i++ {
		postEvent(t, "/api/events/payment-succeeded", uuid.NewString(), payment()) // 4 new events
	}
	postEvent(t, "/api/events/payment-succeeded", key, payment()) // 1 new event
	postEvent(t, "/api/events/payment-succeeded", key, payment()) // 1 replay
	postEvent(t, "/api/events/payment-succeeded", "", payment())  // 1 bad request (no key)
	other := payment()
	other["amount_cents"] = 1
	postEvent(t, "/api/events/payment-succeeded", key, other) // 1 key reuse

	if got := read("notif_events_created_total", `event_type="PaymentSucceeded"`) - createdBefore; got != 5 {
		t.Errorf("events created counter moved by %v, want 5", got)
	}
	if got := read("notif_api_idempotent_replays_total", "") - replaysBefore; got != 1 {
		t.Errorf("replays counter moved by %v, want 1", got)
	}
	if got := read("notif_api_rejected_requests_total", `reason="bad_request"`) - badBefore; got != 1 {
		t.Errorf("bad_request counter moved by %v, want 1", got)
	}
	if got := read("notif_api_rejected_requests_total", `reason="key_reuse"`) - reuseBefore; got != 1 {
		t.Errorf("key_reuse counter moved by %v, want 1", got)
	}
}

// The relay, consumers and provider counters flow through Prometheus: delivering N
// payment events must add at least 3N delivered outcomes (one per channel).
func TestDeliveryCountersReachPrometheus(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	query := func(expr string) float64 {
		resp, err := http.Get(prometheusURL + "/api/v1/query?query=" + urlEscape(expr))
		if err != nil {
			return -1
		}
		defer resp.Body.Close()
		var out struct {
			Data struct {
				Result []struct {
					Value [2]any `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if json.NewDecoder(resp.Body).Decode(&out) != nil || len(out.Data.Result) == 0 {
			return 0
		}
		v, _ := strconv.ParseFloat(fmt.Sprint(out.Data.Result[0].Value[1]), 64)
		return v
	}
	delivered := `sum(notif_consumer_outcomes_total{outcome="delivered"})`
	published := `sum(notif_relay_published_events_total)`
	before, pubBefore := query(delivered), query(published)

	const n = 6
	for i := 0; i < n; i++ {
		id := createPayment(t)
		waitDelivered(t, id, 3)
	}
	waitFor(t, "Prometheus to show the deliveries", 40*time.Second, func() bool {
		return query(delivered) >= before+3*n && query(published) >= pubBefore+n
	})
}

func TestBacklogGaugesFollowTheRealBacklog(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetFaults(t)
	waitRoleStatus(t, "relay", "UP", 20*time.Second)
	setFault(t, "relay.pause", map[string]any{"action": "pause"})
	waitRoleStatus(t, "relay", "PAUSED", 15*time.Second)

	gauge := func(name, labels string) float64 { return metricSum(scrape(t, apiURL+"/metrics"), name, labels) }
	base := gauge("notif_outbox_waiting_events", "")
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		ids = append(ids, createPayment(t))
	}
	if got := gauge("notif_outbox_waiting_events", ""); got != base+3 {
		t.Errorf("outbox backlog gauge = %v, want %v (3 events created while the relay is paused)", got, base+3)
	}
	if got := gauge("notif_role_paused", `role="relay"`); got != 1 {
		t.Errorf("relay paused gauge = %v, want 1", got)
	}

	clearFaults(t)
	for _, id := range ids {
		waitDelivered(t, id, 3)
	}
	waitFor(t, "the outbox backlog gauge to drain", 20*time.Second, func() bool {
		return gauge("notif_outbox_waiting_events", "") == 0 && gauge("notif_role_paused", `role="relay"`) == 0
	})
}

// D9's promise: no metric carries an unbounded label such as an event id.
func TestScrapedMetricsCarryNoUnboundedLabels(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	createPayment(t) // make sure series exist
	uuidLike := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	for name, url := range map[string]string{"api": apiURL + "/metrics", "provider": providerURL + "/metrics"} {
		body := scrape(t, url)
		if strings.Contains(body, "event_id=") {
			t.Errorf("%s exposes an event_id label", name)
		}
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "notif_") && uuidLike.MatchString(line) {
				t.Errorf("%s: a notif_ series contains an id: %s", name, line)
			}
		}
		if !strings.Contains(body, "process_cpu_seconds_total") || !strings.Contains(body, "go_goroutines") {
			t.Errorf("%s is missing the Go and process metrics", name)
		}
	}
}

func urlEscape(s string) string {
	r := strings.NewReplacer(" ", "%20", `"`, "%22", "{", "%7B", "}", "%7D", "(", "%28", ")", "%29", "=", "%3D", "+", "%2B")
	return r.Replace(s)
}
