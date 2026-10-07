package test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/config"
	"notifplatform/internal/db"
	"notifplatform/internal/events"
	"notifplatform/internal/outbox"
	"notifplatform/migrations"
)

// isolatedStore creates a throwaway database on the dev Postgres, migrates it, and
// returns a Store on it. Store-level tests use it so the running relay container
// cannot claim their rows. The database is dropped when the test ends.
func isolatedStore(t *testing.T) (*outbox.Store, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(config.Load().PlatformDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	adminCfg := cfg.Copy()
	adminCfg.ConnConfig.Database = "postgres"
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}

	name := "t_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatalf("create database: %v", err)
	}

	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	if _, err := db.Migrate(ctx, pool, migrations.FS, "platform"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close()
	})
	return outbox.NewStore(pool), pool
}

// seedOutbox creates n events through the real CreateEvent path (no business row).
func seedOutbox(t *testing.T, store *outbox.Store, n int) []uuid.UUID {
	t.Helper()
	ids := make([]uuid.UUID, 0, n)
	for i := 0; i < n; i++ {
		res, err := store.CreateEvent(context.Background(), outbox.CreateInput{
			Key: uuid.NewString(), RequestHash: "h", EventType: events.PaymentSucceeded,
			Write: func(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (outbox.Aggregate, error) {
				return outbox.Aggregate{Type: "payment", ID: uuid.NewString(), Payload: map[string]any{"n": 1}}, nil
			},
		})
		if err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
		ids = append(ids, res.EventID)
	}
	return ids
}

func outboxStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) (status string, attempts int) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT status, attempts FROM outbox_events WHERE id = $1`, id).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	return
}
