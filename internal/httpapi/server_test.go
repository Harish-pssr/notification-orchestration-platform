package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
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
