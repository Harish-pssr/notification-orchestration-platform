// Package health implements worker heartbeats and role status (D9). Workers are
// ephemeral, so health is defined per role: a role is UP when at least one instance
// has sent a heartbeat recently, DOWN when none has, and PAUSED when every live
// instance reports that it was paused by a fault.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Roles are the worker roles shown on the dashboard, in display order.
var Roles = []string{"relay", "consumer-email", "consumer-push", "consumer-webhook"}

// Worker states reported in a heartbeat.
const (
	StateRunning = "RUNNING"
	StatePaused  = "PAUSED"
)

// Beat records one heartbeat for an instance (an upsert keyed by its name).
func Beat(ctx context.Context, pool *pgxpool.Pool, name, role, state string) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO workers (name, role, state) VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET role = $2, state = $3, last_seen = now()`,
		name, role, state)
	return err
}

// Run sends a heartbeat every interval until ctx is cancelled. state reports the
// instance's current state (RUNNING or PAUSED). Instances that vanished long ago are
// removed once at start-up so the table does not grow without bound.
func Run(ctx context.Context, pool *pgxpool.Pool, name, role string, interval time.Duration, state func() string, log *slog.Logger) {
	if _, err := pool.Exec(ctx, `DELETE FROM workers WHERE last_seen < now() - interval '1 hour'`); err != nil {
		log.Warn("could not purge old workers", "error", err)
	}
	for {
		if err := Beat(ctx, pool, name, role, state()); err != nil && ctx.Err() == nil {
			log.Warn("heartbeat failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// Instance is one worker as last seen. Age is measured with the database clock, so
// it does not depend on this machine's clock agreeing with the workers'.
type Instance struct {
	Name       string    `json:"name"`
	Role       string    `json:"role"`
	State      string    `json:"state"`
	AgeSeconds float64   `json:"age_s"`
	StartedAt  time.Time `json:"started_at"`
}

// List returns every recorded instance, most recently seen first.
func List(ctx context.Context, pool *pgxpool.Pool) ([]Instance, error) {
	rows, err := pool.Query(ctx, `
		SELECT name, role, state, extract(epoch FROM (now() - last_seen)), started_at
		FROM workers ORDER BY last_seen DESC, name`)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()
	out := []Instance{}
	for rows.Next() {
		var i Instance
		if err := rows.Scan(&i.Name, &i.Role, &i.State, &i.AgeSeconds, &i.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// Status is a role's overall health.
type Status string

const (
	Up     Status = "UP"
	Down   Status = "DOWN"
	Paused Status = "PAUSED"
)

// InstanceView is an instance with its freshness decided.
type InstanceView struct {
	Instance
	Fresh bool `json:"fresh"` // heartbeat is recent enough to count as alive
}

// Role is one worker role and its instances.
type Role struct {
	Name      string         `json:"name"`
	Status    Status         `json:"status"`
	Live      int            `json:"live"`
	Instances []InstanceView `json:"instances"`
}

// Summarize groups instances by role and decides each role's status. An instance is
// live when its last heartbeat is no older than staleAfter. Every known role appears,
// even with no instances (then it is DOWN).
func Summarize(instances []Instance, staleAfter time.Duration) []Role {
	byRole := map[string]*Role{}
	for _, name := range Roles {
		byRole[name] = &Role{Name: name, Status: Down, Instances: []InstanceView{}}
	}
	livePaused := map[string]int{}
	for _, in := range instances {
		r, ok := byRole[in.Role]
		if !ok {
			continue // an unknown role is ignored rather than invented
		}
		fresh := in.AgeSeconds <= staleAfter.Seconds()
		r.Instances = append(r.Instances, InstanceView{Instance: in, Fresh: fresh})
		if fresh {
			r.Live++
			if in.State == StatePaused {
				livePaused[in.Role]++
			}
		}
	}
	out := make([]Role, 0, len(Roles))
	for _, name := range Roles {
		r := byRole[name]
		switch {
		case r.Live == 0:
			r.Status = Down
		case livePaused[name] == r.Live:
			r.Status = Paused
		default:
			r.Status = Up
		}
		out = append(out, *r)
	}
	return out
}
