// Package db holds database helpers shared by the binaries.
package db

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is an arbitrary constant. Postgres advisory locks are keyed by
// a number we choose; every migrator uses the same one so only one runs at a time.
const migrationLockID = 727274

// Migrate applies, in file-name order, every *.sql file in dir of fsys that has
// not been applied yet. It returns the names it applied in this call.
//
// Each file runs in its own transaction together with the bookkeeping insert, so
// a failing file leaves nothing half-applied.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, dir string) ([]string, error) {
	// A session-level advisory lock belongs to one connection, so we pin one
	// connection from the pool for the whole run instead of using pool.Exec.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return nil, fmt.Errorf("take migration lock: %w", err)
	}
	// Use a fresh context for the unlock: ctx may already be cancelled.
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(fsys, dir) // ReadDir returns entries sorted by name
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %q: %w", dir, err)
	}

	var applied []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version := e.Name()

		var done bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&done); err != nil {
			return applied, fmt.Errorf("check %s: %w", version, err)
		}
		if done {
			continue
		}

		sqlBytes, err := fs.ReadFile(fsys, path.Join(dir, version))
		if err != nil {
			return applied, fmt.Errorf("read %s: %w", version, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return applied, fmt.Errorf("begin %s: %w", version, err)
		}
		// With no arguments pgx sends the text as-is, so a file may hold many statements.
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("apply %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("record %s: %w", version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return applied, fmt.Errorf("commit %s: %w", version, err)
		}
		applied = append(applied, version)
	}
	return applied, nil
}
