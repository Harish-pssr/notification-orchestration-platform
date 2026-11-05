// Command relay publishes outbox events to the broker. Run several for concurrency.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/faults"
	"notifplatform/internal/health"
	"notifplatform/internal/metrics"
	"notifplatform/internal/outbox"
	"notifplatform/internal/relay"
)

func main() {
	if err := run(); err != nil {
		slog.Error("relay failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PlatformDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("relay"),
		nats.MaxReconnects(-1),    // keep trying for as long as the broker is away (D18)
		nats.ReconnectBufSize(-1)) // fail a publish now instead of queueing it behind our back
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}
	if err := broker.EnsureStream(ctx, js); err != nil {
		return err
	}

	// The container hostname is unique per replica, so it works as the worker name.
	name, _ := os.Hostname()
	r := relay.New(relay.Config{
		Name:           config.Getenv("RELAY_NAME", name),
		Lease:          config.GetenvDuration("RELAY_LEASE", 30*time.Second),
		Batch:          config.GetenvInt("RELAY_BATCH", 10),
		Poll:           config.GetenvDuration("RELAY_POLL", 200*time.Millisecond),
		MaxAttempts:    config.GetenvInt("MAX_ATTEMPTS", 5),
		BackoffBase:    config.GetenvDuration("BACKOFF_BASE", time.Second),
		BackoffCap:     config.GetenvDuration("BACKOFF_CAP", 30*time.Second),
		PublishTimeout: config.GetenvDuration("PUBLISH_TIMEOUT", 5*time.Second),
		BrokerBackoff:  config.GetenvDuration("RELAY_BROKER_BACKOFF", time.Second),
	}, outbox.NewStore(pool), js, log)

	// Metrics (D9): served on their own port because the relay has no HTTP server.
	reg := metrics.NewRegistry()
	rm := metrics.NewRelay(reg)
	r.SetMetrics(rm)
	go metrics.Serve(ctx, config.Getenv("METRICS_ADDR", ":9100"), reg, log)

	// Reconciler (D22): finds deliveries the broker seems to have lost and sends them
	// back through the relay. RECONCILE_EVERY=0 turns it off.
	rec := relay.NewReconciler(relay.ReconcilerConfig{
		Every:      config.GetenvDuration("RECONCILE_EVERY", 30*time.Second),
		After:      config.GetenvDuration("RECONCILE_AFTER", 10*time.Minute),
		MaxRepairs: config.GetenvInt("RECONCILE_MAX_REPAIRS", 3),
		Batch:      config.GetenvInt("RECONCILE_BATCH", 100),
	}, outbox.NewStore(pool), log)
	rec.SetMetrics(rm)
	go rec.Run(ctx)

	// Fault injection (D8): a no-op unless FAULTS_ENABLED=true.
	fr := faults.New(pool, config.Getenv("FAULTS_ENABLED", "false") == "true",
		config.GetenvDuration("FAULT_POLL_INTERVAL", time.Second), log)
	go fr.Run(ctx)
	r.SetFaults(fr)

	// Heartbeats feed the dashboard's health panel (D9).
	go health.Run(ctx, pool, config.Getenv("RELAY_NAME", name), "relay",
		config.GetenvDuration("HEARTBEAT_INTERVAL", 3*time.Second), func() string {
			if r.Paused() {
				return health.StatePaused
			}
			return health.StateRunning
		}, map[string]any{
			"lease":        config.Getenv("RELAY_LEASE", "30s"),
			"batch":        config.GetenvInt("RELAY_BATCH", 10),
			"poll":         config.Getenv("RELAY_POLL", "200ms"),
			"max_attempts": config.GetenvInt("MAX_ATTEMPTS", 5),
		}, log)

	return r.Run(ctx)
}
