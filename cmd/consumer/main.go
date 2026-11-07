// Command consumer delivers one channel's notifications. The channel comes from
// the CHANNEL environment variable (email, push or webhook); run replicas freely.
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
	"notifplatform/internal/consumer"
	"notifplatform/internal/events"
	"notifplatform/internal/faults"
	"notifplatform/internal/health"
	"notifplatform/internal/metrics"
)

func main() {
	if err := run(); err != nil {
		slog.Error("consumer failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	cfg := config.Load()

	channel := events.Channel(os.Getenv("CHANNEL"))
	if !channel.Valid() {
		return fmt.Errorf("CHANNEL must be one of email, push, webhook (got %q)", channel)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	concurrency := config.GetenvInt("CONSUMER_CONCURRENCY", 8)
	poolCfg, err := pgxpool.ParseConfig(cfg.PlatformDSN)
	if err != nil {
		return fmt.Errorf("parse platform dsn: %w", err)
	}
	// Every message in progress may hold a connection, so the pool must not be the limit.
	poolCfg.MaxConns = int32(max(int(poolCfg.MaxConns), concurrency+2))
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("consumer-"+string(channel)), nats.MaxReconnects(-1))
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
	maxAttempts := config.GetenvInt("MAX_ATTEMPTS", 5)
	maxDeliver := maxAttempts + 3 // backstop above our own limit (D11)
	cons, err := broker.EnsureConsumer(ctx, js, channel, maxDeliver)
	if err != nil {
		return err
	}

	name, _ := os.Hostname()
	c := consumer.New(consumer.Config{
		Name:        config.Getenv("CONSUMER_NAME", name),
		Channel:     channel,
		ProviderURL: config.Getenv("PROVIDER_URL", "http://localhost:9090"),
		Batch:       config.GetenvInt("CONSUMER_BATCH", 10),
		Concurrency: concurrency,
		FetchWait:   config.GetenvDuration("CONSUMER_FETCH_WAIT", 2*time.Second),
		CallTimeout: config.GetenvDuration("CALL_TIMEOUT", 5*time.Second),
		RetryDelay:  config.GetenvDuration("RETRY_DELAY", time.Second),
		RetryCap:    config.GetenvDuration("RETRY_CAP", 30*time.Second), // database-error delays double up to this
		DBWait:      config.GetenvDuration("DB_WAIT", 2*time.Minute),    // hold a message this long while the database is down
		MaxAttempts: maxAttempts,
		BackoffBase: config.GetenvDuration("BACKOFF_BASE", time.Second),
		BackoffCap:  config.GetenvDuration("BACKOFF_CAP", 30*time.Second),
	}, pool, cons, log)

	// Metrics (D9): served on their own port because the consumer has no HTTP server.
	reg := metrics.NewRegistry()
	c.SetMetrics(metrics.NewConsumer(reg, string(channel)))
	go metrics.Serve(ctx, config.Getenv("METRICS_ADDR", ":9100"), reg, log)

	// Fault injection (D8): a no-op unless FAULTS_ENABLED=true.
	fr := faults.New(pool, config.Getenv("FAULTS_ENABLED", "false") == "true",
		config.GetenvDuration("FAULT_POLL_INTERVAL", time.Second), log)
	go fr.Run(ctx)
	c.SetFaults(fr)

	// Heartbeats feed the dashboard's health panel (D9).
	go health.Run(ctx, pool, config.Getenv("CONSUMER_NAME", name), "consumer-"+string(channel),
		config.GetenvDuration("HEARTBEAT_INTERVAL", 3*time.Second), func() string {
			if c.Paused() {
				return health.StatePaused
			}
			return health.StateRunning
		}, map[string]any{
			"concurrency":  concurrency,
			"batch":        config.GetenvInt("CONSUMER_BATCH", 10),
			"max_attempts": maxAttempts,
			"call_timeout": config.Getenv("CALL_TIMEOUT", "5s"),
		}, log)

	return c.Run(ctx)
}
