// Package experiment records and reads the markers a benchmark run leaves in the
// database (D16). The benchmark tool writes them directly, so there is no write
// endpoint to secure; the API only reads them for the dashboard.
package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Marker kinds.
const (
	Start = "start"
	Phase = "phase"
	End   = "end"
)

// activeWindow: a run with no end marker is shown as active only while its latest
// marker is this recent, so a benchmark that was killed does not look live forever.
const activeWindow = 15 * time.Minute

// Marker is one recorded point in a run.
type Marker struct {
	Kind      string         `json:"kind"`
	Label     string         `json:"label"`
	Params    map[string]any `json:"params"`
	CreatedAt time.Time      `json:"created_at"`
}

// State is the latest run as the dashboard shows it.
type State struct {
	Run     string   `json:"run"`
	Active  bool     `json:"active"`
	Markers []Marker `json:"markers"`
}

// Mark records a marker. params describe what is being varied.
func Mark(ctx context.Context, pool *pgxpool.Pool, run, kind, label string, params map[string]any) error {
	if params == nil {
		params = map[string]any{}
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `INSERT INTO experiment_markers (run, kind, label, params) VALUES ($1, $2, $3, $4)`,
		run, kind, label, raw)
	return err
}

// Current returns the most recent run and its markers, or a zero State (empty Run)
// when no experiment has ever been recorded.
func Current(ctx context.Context, pool *pgxpool.Pool) (State, error) {
	var st State
	var lastAge float64
	err := pool.QueryRow(ctx, `
		SELECT run, extract(epoch FROM (now() - created_at))
		FROM experiment_markers ORDER BY id DESC LIMIT 1`).Scan(&st.Run, &lastAge)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return State{Markers: []Marker{}}, nil
		}
		return st, fmt.Errorf("latest experiment: %w", err)
	}
	rows, err := pool.Query(ctx, `
		SELECT kind, label, params, created_at FROM experiment_markers WHERE run = $1 ORDER BY id`, st.Run)
	if err != nil {
		return st, fmt.Errorf("experiment markers: %w", err)
	}
	defer rows.Close()
	st.Markers = []Marker{}
	ended := false
	for rows.Next() {
		var m Marker
		var raw []byte
		if err := rows.Scan(&m.Kind, &m.Label, &raw, &m.CreatedAt); err != nil {
			return st, err
		}
		_ = json.Unmarshal(raw, &m.Params)
		if m.Kind == End {
			ended = true
		}
		st.Markers = append(st.Markers, m)
	}
	st.Active = !ended && lastAge < activeWindow.Seconds()
	return st, rows.Err()
}
