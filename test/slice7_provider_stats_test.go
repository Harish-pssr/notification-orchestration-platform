package test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"notifplatform/internal/events"
)

// A request that the provider answered with an injected failure is a retry waiting to
// happen, not a duplicate the provider suppressed. The dashboard's "suppressed by the
// provider" figure (requests minus effects) must not count it.
func TestInjectedFailuresAreNotCountedAsSuppressedDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resetProvider(t)
	two := 2
	setProviderFailure(t, "webhook", 500, 0, &two) // the first two webhook calls fail, then it recovers
	id := createPayment(t)
	waitDeliveryStatus(t, id, events.Webhook, "DELIVERED", 30*time.Second)

	resp, err := http.Get(apiURL + "/api/idempotency?event=" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Suppressed int64 `json:"duplicate_requests_suppressed_by_provider"`
		Provider   struct {
			Requests         int64 `json:"requests"`
			Effects          int64 `json:"effects"`
			InjectedFailures int64 `json:"injected_failures"`
		} `json:"provider"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Provider.InjectedFailures != 2 {
		t.Errorf("injected failures = %d, want 2", out.Provider.InjectedFailures)
	}
	if out.Provider.Requests != out.Provider.Effects+2 {
		t.Errorf("requests = %d, effects = %d: want requests = effects + 2 failed attempts", out.Provider.Requests, out.Provider.Effects)
	}
	if out.Suppressed != 0 {
		t.Errorf("suppressed by provider = %d, want 0: the two failed requests are retries, not suppressed duplicates", out.Suppressed)
	}
}
