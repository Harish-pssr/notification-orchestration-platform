package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notifplatform/internal/faults"
)

func get(t *testing.T, checks map[string]Check) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	NewHandler(Deps{Checks: checks}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return rec.Code, body
}

func TestHealthzAllHealthy(t *testing.T) {
	code, body := get(t, map[string]Check{
		"postgres": func(context.Context) error { return nil },
	})
	if code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("got %d %v, want 200 ok", code, body)
	}
}

func TestHealthzReportsFailingDependency(t *testing.T) {
	code, body := get(t, map[string]Check{
		"postgres": func(context.Context) error { return nil },
		"nats":     func(context.Context) error { return errors.New("not connected") },
	})
	if code != http.StatusServiceUnavailable || body["status"] != "unhealthy" {
		t.Fatalf("got %d %v, want 503 unhealthy", code, body)
	}
	checks := body["checks"].(map[string]any)
	if checks["nats"] != "not connected" || checks["postgres"] != "ok" {
		t.Fatalf("unexpected checks: %v", checks)
	}
}

// The dashboard's calls to the provider are quick (3 s), but sending a real test email takes several seconds. That
// one route must wait longer, or the button reports "provider unreachable" while the email goes out.
func TestTestEmailRouteWaitsLongerThanTheQuickProviderCalls(t *testing.T) {
	oldQuick, oldSlow := providerTimeout, providerSlowTimeout
	providerTimeout, providerSlowTimeout = 100*time.Millisecond, 2*time.Second
	defer func() { providerTimeout, providerSlowTimeout = oldQuick, oldSlow }()

	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond) // slower than a quick call, like a real SMTP send
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accepted":true,"reply":"250 OK"}`))
	}))
	defer provider.Close()

	h := NewHandler(Deps{
		Faults:        faults.New(nil, true, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))),
		FaultsEnabled: true, AdminToken: "tok", ProviderURL: provider.URL,
	})
	call := func(path string) int {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer tok")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := call("/api/provider/dedupe"); code != http.StatusBadGateway {
		t.Fatalf("a quick call to a slow provider: %d, want 502 (the quick timeout)", code)
	}
	if code := call("/api/provider/email/test"); code != http.StatusOK {
		t.Fatalf("the test email route: %d, want 200 (it must wait for the slow send)", code)
	}
}
