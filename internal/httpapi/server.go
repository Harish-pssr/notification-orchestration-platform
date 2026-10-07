// Package httpapi builds the HTTP handler served by cmd/api.
package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"notifplatform/internal/admin"
	"notifplatform/internal/faults"
	"notifplatform/internal/health"
	"notifplatform/internal/outbox"
	"notifplatform/web"
)

// Check reports whether one dependency is healthy; nil means healthy.
type Check func(ctx context.Context) error

// Deps are the handler's collaborators. Passing them in keeps the handler
// testable without a database or broker.
type Deps struct {
	Checks map[string]Check
	Store  *outbox.Store

	// Health (D9). Each is optional: a missing one is simply left out of the response.
	Workers    func(ctx context.Context) ([]health.Instance, error)
	Broker     func(ctx context.Context) (BrokerStats, error)
	StaleAfter time.Duration // a heartbeat older than this means the instance is gone

	Faults        *faults.Registry
	FaultsEnabled bool   // FAULTS_ENABLED: the admin endpoints exist only when true
	AdminToken    string // ADMIN_TOKEN: shared secret for the admin endpoints
	ProviderURL   string // base URL of the mock provider
}

// NewHandler returns the API's routes.
func NewHandler(d Deps) http.Handler {
	mux := http.NewServeMux()
	// Go 1.22+ lets a route pattern name the HTTP method and capture path parts.
	mux.HandleFunc("GET /healthz", healthz(d.Checks))

	e := &eventsHandler{store: d.Store}
	mux.HandleFunc("POST /api/events/payment-succeeded", e.create(paymentSucceeded))
	mux.HandleFunc("POST /api/events/booking-confirmed", e.create(bookingConfirmed))
	mux.HandleFunc("POST /api/events/password-changed", e.create(passwordChanged))
	mux.HandleFunc("GET /api/summary", e.summary)
	mux.HandleFunc("GET /api/events", e.list)
	mux.HandleFunc("GET /api/events/{id}", e.get)

	staleAfter := d.StaleAfter
	if staleAfter <= 0 {
		staleAfter = 10 * time.Second
	}
	hh := &healthHandler{store: d.Store, workers: d.Workers, broker: d.Broker, staleAfter: staleAfter}
	mux.HandleFunc("GET /api/health", hh.get)

	mux.HandleFunc("GET /api/dead-letters", e.deadLetters)
	// Replay is an operator action, not fault injection, so it needs the token but
	// not FAULTS_ENABLED: it must work on a deployment where faults are off.
	mux.Handle("POST /api/replay", admin.RequireToken(d.AdminToken, http.HandlerFunc(e.replay)))

	client := newHTTPClient()
	mux.HandleFunc("GET /api/idempotency", e.idempotency(d.ProviderURL, client))
	// The UI uses this to know whether to offer the failure controls.
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"faults_enabled": d.FaultsEnabled})
	})

	if d.Faults != nil {
		f := &faultsHandler{registry: d.Faults, providerURL: d.ProviderURL, client: client}
		adm := func(h http.HandlerFunc) http.Handler { return admin.Require(d.FaultsEnabled, d.AdminToken, h) }
		mux.Handle("GET /api/faults", adm(f.list))
		mux.Handle("PUT /api/faults/{point}", adm(f.set))
		mux.Handle("DELETE /api/faults/{point}", adm(f.delete))
		mux.Handle("DELETE /api/faults", adm(f.clear))
		mux.Handle("POST /api/provider/dedupe", adm(f.forwardToProvider("/admin/dedupe")))
		mux.Handle("POST /api/provider/forget", adm(f.forwardToProvider("/admin/forget")))
		mux.Handle("POST /api/provider/failure", adm(f.forwardToProvider("/admin/failures")))
		mux.Handle("POST /api/provider/failure/clear", adm(f.forwardToProvider("/admin/failures/clear")))
		mux.HandleFunc("GET /api/provider/failures", f.providerFailures) // read-only
	}

	mux.Handle("GET /", http.FileServerFS(web.FS)) // the dashboard, embedded in the binary
	return mux
}

func healthz(checks map[string]Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Bound the whole check so a hung dependency cannot hang the probe.
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		results := make(map[string]string, len(checks))
		status := http.StatusOK
		for name, check := range checks {
			if err := check(ctx); err != nil {
				results[name] = err.Error()
				status = http.StatusServiceUnavailable
			} else {
				results[name] = "ok"
			}
		}

		body := map[string]any{"status": "ok", "checks": results}
		if status != http.StatusOK {
			body["status"] = "unhealthy"
		}
		writeJSON(w, status, body)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
