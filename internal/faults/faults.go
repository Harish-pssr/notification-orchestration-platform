// Package faults implements fault injection (D8): named hooks in the code whose
// behaviour is controlled by rows in the fault_config table. Hooks are no-ops
// unless FAULTS_ENABLED is true, and every method is safe on a nil *Registry, so
// components work unchanged when no registry is configured.
package faults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Action is what a fault does when it fires.
type Action string

const (
	Crash     Action = "crash"     // exit the process abruptly (os.Exit(137), no cleanup)
	Duplicate Action = "duplicate" // publish the messages a second time, bypassing broker dedup
	Disable   Action = "disable"   // a state fault: switch an optimisation off while active
	Pause     Action = "pause"     // a state fault: a worker role stops taking new work while active
)

// Known lists every injection point and the actions it supports. The admin API
// rejects anything else, so a typo cannot silently do nothing.
var Known = map[string][]Action{
	"relay.after_publish":           {Crash, Duplicate},
	"consumer.skip_delivered_check": {Disable},
	// Pausing stops a role from claiming or fetching. Nothing is lost: work simply
	// waits in the outbox (relay) or in the broker (consumers) until it is resumed.
	"relay.pause":            {Pause},
	"consumer.email.pause":   {Pause},
	"consumer.push.pause":    {Pause},
	"consumer.webhook.pause": {Pause},
}

// Fault is one row of fault_config.
type Fault struct {
	Point       string         `json:"point"`
	Action      Action         `json:"action"`
	Params      map[string]any `json:"params,omitempty"`
	Remaining   *int           `json:"remaining"` // nil = unlimited
	Probability float64        `json:"probability"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// Validate checks the point, action and numeric ranges.
func (f Fault) Validate() error {
	actions, ok := Known[f.Point]
	if !ok {
		return fmt.Errorf("unknown injection point %q", f.Point)
	}
	if !slices.Contains(actions, f.Action) {
		return fmt.Errorf("point %q does not support action %q (supported: %v)", f.Point, f.Action, actions)
	}
	if f.Probability <= 0 || f.Probability > 1 {
		return errors.New("probability must be in (0, 1]")
	}
	if f.Remaining != nil && *f.Remaining < 0 {
		return errors.New("remaining must be zero or more")
	}
	return nil
}

// Registry caches fault_config and answers the hooks' questions.
type Registry struct {
	enabled  bool
	pool     *pgxpool.Pool
	interval time.Duration
	log      *slog.Logger
	cache    atomic.Pointer[map[string]Fault]
	rnd      func() float64
	exit     func(int) // injectable so tests do not kill the test runner
}

// New returns a Registry. With enabled=false every hook is a no-op.
func New(pool *pgxpool.Pool, enabled bool, interval time.Duration, log *slog.Logger) *Registry {
	r := &Registry{enabled: enabled, pool: pool, interval: interval, log: log, rnd: rand.Float64, exit: os.Exit}
	empty := map[string]Fault{}
	r.cache.Store(&empty)
	return r
}

// Enabled reports whether fault injection is switched on for this process.
func (r *Registry) Enabled() bool { return r != nil && r.enabled }

// SetExit replaces the function Crash calls (tests only).
func (r *Registry) SetExit(fn func(int)) { r.exit = fn }

// Run refreshes the cache every interval until ctx is cancelled.
func (r *Registry) Run(ctx context.Context) {
	if !r.Enabled() {
		return
	}
	for {
		if err := r.Refresh(ctx); err != nil && ctx.Err() == nil {
			// Keep serving the last known configuration if the database is unreachable.
			r.log.Warn("fault refresh failed; keeping last known faults", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval):
		}
	}
}

// Refresh reloads fault_config into the cache.
func (r *Registry) Refresh(ctx context.Context) error {
	list, err := r.List(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]Fault, len(list))
	for _, f := range list {
		m[f.Point] = f
	}
	r.cache.Store(&m)
	return nil
}

// Active returns the fault configured at point without consuming it. It is for
// state faults such as Disable, which stay in force while the row exists.
func (r *Registry) Active(point string) (Fault, bool) {
	if !r.Enabled() {
		return Fault{}, false
	}
	f, ok := (*r.cache.Load())[point]
	if !ok || (f.Remaining != nil && *f.Remaining == 0) {
		return Fault{}, false
	}
	return f, true
}

// Fire asks whether the fault at point should happen now. A limited fault is
// consumed with one atomic UPDATE, so across any number of processes it fires at
// most `remaining` times. Any error means "do not fire": faults fail safe.
func (r *Registry) Fire(ctx context.Context, point string) (Fault, bool) {
	f, ok := r.Active(point)
	if !ok {
		return Fault{}, false
	}
	if f.Probability < 1 && r.rnd() >= f.Probability {
		return Fault{}, false
	}
	if f.Remaining != nil {
		var left int
		err := r.pool.QueryRow(ctx,
			`UPDATE fault_config SET remaining = remaining - 1 WHERE point = $1 AND remaining > 0 RETURNING remaining`,
			point).Scan(&left)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				r.log.Warn("could not consume fault", "point", point, "error", err)
			}
			return Fault{}, false
		}
	}
	r.log.Warn("fault fired", "point", point, "action", f.Action)
	return f, true
}

// Crash terminates the process the way a kill would: no deferred cleanup runs.
func (r *Registry) Crash() {
	r.log.Error("injected crash: exiting now", "status", "crash")
	r.exit(137)
}

// Set creates or replaces the fault at f.Point.
func (r *Registry) Set(ctx context.Context, f Fault) error {
	if err := f.Validate(); err != nil {
		return err
	}
	params, err := json.Marshal(f.Params)
	if err != nil {
		return err
	}
	if f.Params == nil {
		params = []byte("{}")
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO fault_config (point, action, params, remaining, probability)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (point) DO UPDATE
		SET action = $2, params = $3, remaining = $4, probability = $5, updated_at = now()`,
		f.Point, string(f.Action), params, f.Remaining, f.Probability)
	return err
}

// List returns every configured fault.
func (r *Registry) List(ctx context.Context) ([]Fault, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT point, action, params, remaining, probability, updated_at FROM fault_config ORDER BY point`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Fault{}
	for rows.Next() {
		var f Fault
		var action string
		var params []byte
		if err := rows.Scan(&f.Point, &action, &params, &f.Remaining, &f.Probability, &f.UpdatedAt); err != nil {
			return nil, err
		}
		f.Action = Action(action)
		_ = json.Unmarshal(params, &f.Params)
		out = append(out, f)
	}
	return out, rows.Err()
}

// Delete removes the fault at point (a no-op if there is none).
func (r *Registry) Delete(ctx context.Context, point string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM fault_config WHERE point = $1`, point)
	return err
}

// Clear removes every fault.
func (r *Registry) Clear(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM fault_config`)
	return err
}
