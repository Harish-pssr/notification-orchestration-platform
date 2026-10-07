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

	pool, err := pgxpool.New(ctx, cfg.PlatformDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("consumer-"+string(channel)))
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
		FetchWait:   config.GetenvDuration("CONSUMER_FETCH_WAIT", 2*time.Second),
		CallTimeout: config.GetenvDuration("CALL_TIMEOUT", 5*time.Second),
		RetryDelay:  config.GetenvDuration("RETRY_DELAY", time.Second),
		MaxAttempts: maxAttempts,
		BackoffBase: config.GetenvDuration("BACKOFF_BASE", time.Second),
		BackoffCap:  config.GetenvDuration("BACKOFF_CAP", 30*time.Second),
	}, pool, cons, log)

	// Fault injection (D8): a no-op unless FAULTS_ENABLED=true.
	fr := faults.New(pool, config.Getenv("FAULTS_ENABLED", "false") == "true",
		config.GetenvDuration("FAULT_POLL_INTERVAL", time.Second), log)
	go fr.Run(ctx)
	c.SetFaults(fr)

	return c.Run(ctx)
}
