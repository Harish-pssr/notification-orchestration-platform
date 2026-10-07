// Command api serves the REST API and the dashboard.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/events"
	"notifplatform/internal/experiment"
	"notifplatform/internal/faults"
	"notifplatform/internal/health"
	"notifplatform/internal/httpapi"
	"notifplatform/internal/metrics"
	"notifplatform/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api failed", "error", err)
		os.Exit(1)
	}
}

// brokerStats reads the stream and each channel's durable consumer from JetStream:
// how many messages are waiting, in flight, or have been redelivered.
func brokerStats(ctx context.Context, js jetstream.JetStream) (httpapi.BrokerStats, error) {
	stats := httpapi.BrokerStats{Channels: map[string]httpapi.BrokerChannel{}}
	stream, err := js.Stream(ctx, broker.StreamName)
	if err != nil {
		return stats, errors.New("the broker stream does not exist yet")
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return stats, errors.New("could not read the broker stream")
	}
	stats.StreamMessages = info.State.Msgs
	for _, ch := range events.AllChannels {
		cons, err := stream.Consumer(ctx, string(ch))
		if err != nil {
			continue // that channel's consumer has not started yet
		}
		ci, err := cons.Info(ctx)
		if err != nil {
			continue
		}
		stats.Channels[string(ch)] = httpapi.BrokerChannel{
			Pending: ci.NumPending, AckPending: ci.NumAckPending, Redelivered: ci.NumRedelivered,
		}
	}
	return stats, nil
}

// snapshot reads the backlog and role health for the metrics gauges. A broker failure
// leaves only the broker gauges absent: the database-derived ones are still reported.
func snapshot(ctx context.Context, store *outbox.Store, pool *pgxpool.Pool, js jetstream.JetStream, staleAfter time.Duration) (metrics.Snapshot, error) {
	s := metrics.Snapshot{
		BrokerPending: map[string]uint64{}, BrokerInflight: map[string]int{},
		RoleLive: map[string]int{}, RoleUp: map[string]bool{}, RolePaused: map[string]bool{},
	}
	var err error
	if s.OutboxWaiting, s.DeliveriesWaiting, err = store.Backlog(ctx); err != nil {
		return s, err
	}
	if s.DeliveriesDead, err = store.DeadCounts(ctx); err != nil {
		return s, err
	}
	instances, err := health.List(ctx, pool)
	if err != nil {
		return s, err
	}
	for _, role := range health.Summarize(instances, staleAfter) {
		s.RoleLive[role.Name] = role.Live
		s.RoleUp[role.Name] = role.Status == health.Up
		s.RolePaused[role.Name] = role.Status == health.Paused
	}
	if bs, err := brokerStats(ctx, js); err == nil {
		s.BrokerStream = bs.StreamMessages
		for ch, c := range bs.Channels {
			s.BrokerPending[ch] = c.Pending
			s.BrokerInflight[ch] = c.AckPending
		}
	}
	return s, nil
}

func run() error {
	// slog's JSON handler writes one structured log line per call to stdout (D9).
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PlatformDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("api"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("jetstream: %w", err)
	}

	store := outbox.NewStore(pool)
	staleAfter := config.GetenvDuration("HEARTBEAT_STALE_AFTER", 10*time.Second)

	// Metrics (D9): counters and histograms are recorded by the processes doing the work;
	// the backlog and health gauges are computed here, once per scrape, so replicas never
	// double-count them.
	reg := metrics.NewRegistry()
	apiMetrics := metrics.NewAPI(reg)
	reg.MustRegister(metrics.NewStateCollector(func(ctx context.Context) (metrics.Snapshot, error) {
		return snapshot(ctx, store, pool, js, staleAfter)
	}, slog.Default()))

	faultsEnabled := config.Getenv("FAULTS_ENABLED", "false") == "true"
	handler := httpapi.NewHandler(httpapi.Deps{
		Store:          store,
		Metrics:        apiMetrics,
		MetricsHandler: metrics.Handler(reg),
		Workers:        func(ctx context.Context) ([]health.Instance, error) { return health.List(ctx, pool) },
		Experiment:     func(ctx context.Context) (experiment.State, error) { return experiment.Current(ctx, pool) },
		Broker:         func(ctx context.Context) (httpapi.BrokerStats, error) { return brokerStats(ctx, js) },
		StaleAfter:     staleAfter,
		Faults:         faults.New(pool, faultsEnabled, time.Second, slog.Default()),
		FaultsEnabled:  faultsEnabled,
		AdminToken:     os.Getenv("ADMIN_TOKEN"),
		// On a public deployment every write uses the one admin token (D19).
		CreateRequiresToken: config.Getenv("CREATE_REQUIRES_TOKEN", "false") == "true",
		ProviderURL:         config.Getenv("PROVIDER_URL", "http://localhost:9090"),
		Checks: map[string]httpapi.Check{
			"postgres": func(ctx context.Context) error { return pool.Ping(ctx) },
			"nats": func(context.Context) error {
				if !nc.IsConnected() {
					return errors.New("not connected")
				}
				return nil
			},
		},
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}

	// ListenAndServe blocks, so it runs in a goroutine; we then wait for either a
	// server error or a shutdown signal on the main goroutine.
	errCh := make(chan error, 1)
	go func() {
		slog.Info("api listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx) // lets in-flight requests finish
	}
}
