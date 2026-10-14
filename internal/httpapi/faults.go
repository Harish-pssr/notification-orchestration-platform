package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"notifplatform/internal/faults"
)

type faultsHandler struct {
	registry    *faults.Registry
	providerURL string
	client      *http.Client
	slowClient  *http.Client // for calls that send a real email
}

func (h *faultsHandler) list(w http.ResponseWriter, r *http.Request) {
	list, err := h.registry.List(r.Context())
	if err != nil {
		slog.Error("list faults failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list faults")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"faults": list, "known": faults.Known})
}

// set creates or replaces the fault at {point}.
func (h *faultsHandler) set(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Action      faults.Action  `json:"action"`
		Params      map[string]any `json:"params"`
		Remaining   *int           `json:"remaining"`
		Probability float64        `json:"probability"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if in.Probability == 0 {
		in.Probability = 1 // omitted means "always"
	}
	f := faults.Fault{Point: r.PathValue("point"), Action: in.Action, Params: in.Params, Remaining: in.Remaining, Probability: in.Probability}
	if err := f.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.registry.Set(r.Context(), f); err != nil {
		slog.Error("set fault failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not save fault")
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (h *faultsHandler) delete(w http.ResponseWriter, r *http.Request) {
	if err := h.registry.Delete(r.Context(), r.PathValue("point")); err != nil {
		writeError(w, http.StatusInternalServerError, "could not delete fault")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *faultsHandler) clear(w http.ResponseWriter, r *http.Request) {
	if err := h.registry.Clear(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "could not clear faults")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// forwardToProvider returns a handler that forwards its body to one of the provider's
// own admin endpoints. The provider is a separate "external" system with its own
// control surface (D8); the shared admin token is passed through.
func (h *faultsHandler) forwardToProvider(path string) http.HandlerFunc {
	return h.forward(path, nil)
}

// forwardToProviderSlow is forwardToProvider with the long timeout, for calls that send a real email.
func (h *faultsHandler) forwardToProviderSlow(path string) http.HandlerFunc {
	return h.forward(path, h.slowClient)
}

func (h *faultsHandler) forward(path string, client *http.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := client
		if c == nil {
			c = h.client
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<10))
		if err != nil {
			writeError(w, http.StatusBadRequest, "could not read body")
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, h.providerURL+path, bytes.NewReader(body))
		req.Header.Set("Authorization", r.Header.Get("Authorization"))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.Do(req)
		if err != nil {
			writeError(w, http.StatusBadGateway, "provider unreachable")
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
}

// providerFailures shows which failures are currently injected into the provider.
func (h *faultsHandler) providerFailures(w http.ResponseWriter, r *http.Request) {
	resp, err := h.client.Get(h.providerURL + "/failures")
	if err != nil {
		writeError(w, http.StatusBadGateway, "provider unreachable")
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// forwardGet returns a handler that relays a read-only GET to the provider.
func (h *faultsHandler) forwardGet(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp, err := h.client.Get(h.providerURL + path)
		if err != nil {
			writeError(w, http.StatusBadGateway, "provider unreachable")
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
}

// providerStats is the shape of the provider's GET /stats.
type providerStats struct {
	Requests         int64 `json:"requests"`
	DistinctKeys     int64 `json:"distinct_keys"`
	Effects          int64 `json:"effects"`
	DuplicateEffects int64 `json:"duplicate_effects"`
	InjectedFailures int64 `json:"injected_failures"`
	Dedupe           bool  `json:"dedupe"`
}

// idempotency combines the platform's counters with the provider's (D5): how many
// duplicate deliveries happened, where they were stopped, and whether any
// duplicate side effect got through.
func (h *eventsHandler) idempotency(providerURL string, client *http.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// ?event=<id> narrows every counter to one event (used by the demo).
		var eventID *uuid.UUID
		statsURL := providerURL + "/stats"
		if v := r.URL.Query().Get("event"); v != "" {
			id, err := uuid.Parse(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid event id")
				return
			}
			eventID = &id
			statsURL += "?prefix=" + id.String() + ":"
		}
		delivered, ignored, err := h.store.DuplicateSummary(r.Context(), eventID)
		if err != nil {
			slog.Error("duplicate summary failed", "error", err)
			writeError(w, http.StatusInternalServerError, "could not read delivery counters")
			return
		}
		out := map[string]any{
			"deliveries_delivered":            delivered,
			"duplicates_ignored_by_consumers": ignored,
		}
		resp, err := client.Get(statsURL)
		if err != nil {
			out["provider_error"] = "provider unreachable"
			writeJSON(w, http.StatusOK, out)
			return
		}
		defer resp.Body.Close()
		var ps providerStats
		if err := json.NewDecoder(resp.Body).Decode(&ps); err != nil {
			out["provider_error"] = "unreadable provider response"
			writeJSON(w, http.StatusOK, out)
			return
		}
		// A request answered with an injected error ran no effect, but it is not a duplicate
		// the provider suppressed either, so it is subtracted (it is a retry, not a repeat).
		suppressed := ps.Requests - ps.InjectedFailures - ps.Effects
		if suppressed < 0 {
			suppressed = 0
		}
		out["provider"] = ps
		out["duplicate_requests_suppressed_by_provider"] = suppressed
		writeJSON(w, http.StatusOK, out)
	}
}

// providerTimeout bounds the dashboard's quick calls to the provider. providerSlowTimeout is for the one call that
// really sends an email (the test email): an SMTP conversation takes several seconds (Gmail about 5), longer than a
// quick call is allowed, so it would otherwise be reported as "provider unreachable" while the email goes out.
var (
	providerTimeout     = 3 * time.Second
	providerSlowTimeout = 30 * time.Second
)

func newHTTPClient() *http.Client { return &http.Client{Timeout: providerTimeout} }
