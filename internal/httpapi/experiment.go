package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"notifplatform/internal/experiment"
)

// experimentHandler serves the latest benchmark run's markers (D16). Without a source
// it reports "no experiment", so the dashboard simply hides the Experiment page.
func experimentHandler(source func(ctx context.Context) (experiment.State, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if source == nil {
			writeJSON(w, http.StatusOK, experiment.State{Markers: []experiment.Marker{}})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		st, err := source(ctx)
		if err != nil {
			slog.Error("read experiment failed", "error", err)
			writeError(w, http.StatusInternalServerError, "could not read the experiment")
			return
		}
		writeJSON(w, http.StatusOK, st)
	}
}
