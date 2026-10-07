package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
)

// deadLetters lists everything an operator can replay.
func (h *eventsHandler) deadLetters(w http.ResponseWriter, r *http.Request) {
	dl, err := h.store.ListDeadLetters(r.Context(), 200)
	if err != nil {
		slog.Error("list dead letters failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list dead letters")
		return
	}
	writeJSON(w, http.StatusOK, dl)
}

// replay puts dead-lettered deliveries back through the relay (D7). The caller must
// name an event, a channel, or pass {"all": true}; an empty body is refused.
func (h *eventsHandler) replay(w http.ResponseWriter, r *http.Request) {
	var in struct {
		EventID string `json:"event_id"`
		Channel string `json:"channel"`
		All     bool   `json:"all"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req := outbox.ReplayInput{Channel: in.Channel, All: in.All}
	if in.EventID != "" {
		id, err := uuid.Parse(in.EventID)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid event_id")
			return
		}
		req.EventID = &id
	}
	if in.Channel != "" && !events.Channel(in.Channel).Valid() {
		writeError(w, http.StatusBadRequest, "channel must be email, push or webhook")
		return
	}
	if req.EventID == nil && req.Channel == "" && !req.All {
		writeError(w, http.StatusBadRequest, "give an event_id, a channel, or set all to true")
		return
	}

	res, err := h.store.Replay(r.Context(), req)
	if err != nil {
		slog.Error("replay failed", "error", err)
		writeError(w, http.StatusInternalServerError, "replay failed")
		return
	}
	slog.Info("replay requested", "event_id", in.EventID, "channel", in.Channel, "all", in.All,
		"deliveries", res.Deliveries, "events", res.Events, "skipped", res.Skipped)
	writeJSON(w, http.StatusOK, res)
}
