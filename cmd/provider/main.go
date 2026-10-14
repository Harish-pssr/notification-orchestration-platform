// Command provider runs the mock external email/push/webhook system.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/metrics"
	"notifplatform/internal/provider"
)

func main() {
	if err := run(); err != nil {
		slog.Error("provider failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.ProviderDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	// PROVIDER_DEDUPE=false is the negative-control mode (D5): never use it normally.
	dedupe := config.Getenv("PROVIDER_DEDUPE", "true") != "false"
	if !dedupe {
		log.Warn("idempotency DISABLED (negative-control mode)")
	}

	addr := config.Getenv("PROVIDER_ADDR", ":9090")
	p := provider.New(pool, dedupe, log)
	reg := metrics.NewRegistry()
	p.SetMetrics(metrics.NewProvider(reg), metrics.Handler(reg))
	p.SetAdmin(config.Getenv("FAULTS_ENABLED", "false") == "true", os.Getenv("ADMIN_TOKEN"))
	// Real email sending (D21): available only when an SMTP account is configured, and always off until an
	// operator switches it on from the dashboard.
	emailCfg := provider.EmailConfigFromEnv()
	p.SetEmail(ctx, emailCfg, nil)
	if emailCfg.Configured() {
		log.Info("real email sending is available but switched off", "smtp_host", emailCfg.Host, "max_per_hour", emailCfg.MaxPerHour)
	}
	srv := &http.Server{Addr: addr, Handler: p.Handler(), ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		log.Info("provider listening", "addr", addr, "dedupe", dedupe)
		errCh <- srv.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
