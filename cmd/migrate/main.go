// Command migrate applies SQL migrations to one database.
//
// Usage: migrate platform|provider
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/db"
	"notifplatform/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: migrate platform|provider")
	}
	target := os.Args[1]

	cfg := config.Load()
	var dsn string
	switch target {
	case "platform":
		dsn = cfg.PlatformDSN
	case "provider":
		dsn = cfg.ProviderDSN
	default:
		return fmt.Errorf("unknown target %q (want platform or provider)", target)
	}

	// The context is cancelled on Ctrl-C or `docker stop`, which aborts the run cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	applied, err := db.Migrate(ctx, pool, migrations.FS, target)
	if err != nil {
		return err
	}
	slog.Info("migrations complete", "target", target, "applied", applied)
	return nil
}
