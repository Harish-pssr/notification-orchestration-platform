package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"notifplatform/internal/health"
)

// BrokerChannel is one channel's consumer as the broker sees it.
type BrokerChannel struct {
	Pending     uint64 `json:"pending"`     // messages waiting to be delivered
	AckPending  int    `json:"ack_pending"` // delivered, not yet acknowledged
	Redelivered int    `json:"redelivered"` // messages delivered more than once
}

// BrokerStats is the broker's view of the backlog.
type BrokerStats struct {
	StreamMessages uint64                   `json:"stream_messages"`
	Channels       map[string]BrokerChannel `json:"channels"`
}

type healthHandler struct {
	store interface {
		Backlog(ctx context.Context) (int64, map[string]int64, error)
	}
	workers    func(ctx context.Context) ([]health.Instance, error)
	broker     func(ctx context.Context) (BrokerStats, error)
	staleAfter time.Duration
}

// health combines worker heartbeats (role status), the outbox and delivery backlog
// from the database, and the broker's own backlog (D9). Each part degrades on its
// own: if the broker is unreachable the rest is still reported.
func (h *healthHandler) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	out := map[string]any{"stale_after_s": h.staleAfter.Seconds()}

	if h.workers != nil {
		instances, err := h.workers(ctx)
		if err != nil {
			slog.Error("list workers failed", "error", err)
			out["roles_error"] = "could not read worker heartbeats"
		} else {
			out["roles"] = health.Summarize(instances, h.staleAfter)
		}
	}

	if outboxWaiting, deliveries, err := h.store.Backlog(ctx); err != nil {
		slog.Error("backlog failed", "error", err)
		out["backlog_error"] = "could not count the backlog"
	} else {
		out["backlog"] = map[string]any{"outbox": outboxWaiting, "deliveries": deliveries}
	}

	if h.broker != nil {
		if stats, err := h.broker(ctx); err != nil {
			out["broker_error"] = err.Error()
		} else {
			out["broker"] = stats
		}
	}
	writeJSON(w, http.StatusOK, out)
}
