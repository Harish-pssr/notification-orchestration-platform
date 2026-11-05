package relay

import (
	"context"
	"log/slog"
	"time"

	"notifplatform/internal/metrics"
	"notifplatform/internal/outbox"
)

// ReconcilerConfig tunes the reconciler (D22). A zero Every disables it.
type ReconcilerConfig struct {
	Every      time.Duration // how often to look
	After      time.Duration // how long an open delivery may sit untouched before it counts as lost
	MaxRepairs int           // republishes per delivery before it is dead-lettered
	Batch      int           // events handled per pass
}

// Reconciler closes the gap the outbox cannot: the broker accepted a message (so the
// event is PUBLISHED) and then lost it. It only changes rows; the relay republishes
// them through the normal lease-and-fence path.
type Reconciler struct {
	cfg     ReconcilerConfig
	store   *outbox.Store
	log     *slog.Logger
	metrics *metrics.Relay // nil means no metrics
}

// NewReconciler builds a reconciler.
func NewReconciler(cfg ReconcilerConfig, store *outbox.Store, log *slog.Logger) *Reconciler {
	return &Reconciler{cfg: cfg, store: store, log: log.With("component", "reconciler")}
}

// SetMetrics attaches the relay's metrics. Call it before Run.
func (r *Reconciler) SetMetrics(m *metrics.Relay) { r.metrics = m }

// Run does one pass every cfg.Every until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	if r.cfg.Every <= 0 {
		r.log.Info("reconciler disabled")
		return
	}
	r.log.Info("reconciler started", "every", r.cfg.Every, "after", r.cfg.After, "max_repairs", r.cfg.MaxRepairs)
	t := time.NewTicker(r.cfg.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce does a single pass. It is exported so tests can drive it.
func (r *Reconciler) RunOnce(ctx context.Context) outbox.ReconcileResult {
	res, err := r.store.Reconcile(ctx, outbox.ReconcileInput{
		After: r.cfg.After, MaxRepairs: r.cfg.MaxRepairs, Batch: r.cfg.Batch,
	})
	if err != nil {
		r.metrics.ReconcileFailed()
		r.log.Error("reconcile pass failed", "error", err)
		return res
	}
	r.metrics.Reconciled(res.Repaired, res.DeadLettered)
	if res.Repaired > 0 || res.DeadLettered > 0 {
		// This is the signal worth alerting on: in normal operation it never happens.
		r.log.Warn("found deliveries the broker seems to have lost",
			"status", "reconciled", "repaired", res.Repaired, "events", res.Events, "dead_lettered", res.DeadLettered)
	}
	return res
}
