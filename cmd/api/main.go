// Command api serves the REST API and, later, the dashboard.
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

	"notifplatform/internal/config"
	"notifplatform/internal/faults"
	"notifplatform/internal/httpapi"
	"notifplatform/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api failed", "error", err)
		os.Exit(1)
	}
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

	nc, err := nats.Connect(cfg.NATSURL, nats.Name("api"))
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()

	faultsEnabled := config.Getenv("FAULTS_ENABLED", "false") == "true"
	handler := httpapi.NewHandler(httpapi.Deps{
		Store:         outbox.NewStore(pool),
		Faults:        faults.New(pool, faultsEnabled, time.Second, slog.Default()),
		FaultsEnabled: faultsEnabled,
		AdminToken:    os.Getenv("ADMIN_TOKEN"),
		ProviderURL:   config.Getenv("PROVIDER_URL", "http://localhost:9090"),
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
