package test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	"notifplatform/internal/config"
)

// Integration tests need the stack running: scripts/up.sh.
// Run `go test -short ./...` to skip them.

func TestMigrationsApplied(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cases := []struct {
		name, dsn, table string
	}{
		{"platform", cfg.PlatformDSN, "workers"},
		{"provider", cfg.ProviderDSN, "idempotency_keys"},
	}
	for _, c := range cases {
		pool, err := pgxpool.New(ctx, c.dsn)
		if err != nil {
			t.Fatalf("%s: connect: %v", c.name, err)
		}
		var exists bool
		err = pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, c.table).Scan(&exists)
		pool.Close()
		if err != nil || !exists {
			t.Errorf("%s: table %s missing (err=%v)", c.name, c.table, err)
		}
	}
}

func TestNATSJetStreamEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	nc, err := nats.Connect(config.Load().NATSURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("JetStream not enabled on the server: %v", err)
	}
}

func TestAPIHealthz(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	resp, err := http.Get("http://localhost:8080/healthz")
	if err != nil {
		t.Fatalf("api unreachable: %v", err)
	}
	defer resp.Body.Close()

	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || body.Status != "ok" {
		t.Fatalf("got %d %+v, want 200 ok", resp.StatusCode, body)
	}
	if body.Checks["postgres"] != "ok" || body.Checks["nats"] != "ok" {
		t.Fatalf("unexpected checks: %+v", body.Checks)
	}
}
