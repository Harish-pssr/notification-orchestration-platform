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

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("relay"))
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
	}, outbox.NewStore(pool), js, log)

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
		}, log)

	return r.Run(ctx)
}
