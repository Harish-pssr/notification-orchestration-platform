// Package provider is the mock external system the consumers call (D5). It
// honors Idempotency-Key: the key and the effect are written in one transaction
// under a unique constraint, so a repeated key cannot run the effect twice.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"notifplatform/internal/admin"
	"notifplatform/internal/events"
	"notifplatform/internal/metrics"
)

// keyPrefix matches the only prefixes stats accepts: hex digits, dashes and colons.
var keyPrefix = regexp.MustCompile(`^[0-9a-f:-]+$`)

// Provider serves the mock email, push and webhook endpoints.
type Provider struct {
	pool *pgxpool.Pool
	log  *slog.Logger
	// dedupe is true in normal operation. The negative-control run turns it off to
	// prove duplicates would be detected if the mechanism were missing (D5). It is
	// atomic because the admin endpoint flips it while requests are being served.
	dedupe atomic.Bool

	adminEnabled bool
	adminToken   string

	fails          *failures         // injected misbehaviour per channel (D8)
	metrics        *metrics.Provider // nil means no metrics
	metricsHandler http.Handler      // serves /metrics when set
}

// SetMetrics attaches the provider's metrics and the handler that serves them.
func (p *Provider) SetMetrics(m *metrics.Provider, h http.Handler) {
	p.metrics, p.metricsHandler = m, h
}

// New returns a Provider. Pass dedupe=false only for the negative-control run.
func New(pool *pgxpool.Pool, dedupe bool, log *slog.Logger) *Provider {
	p := &Provider{pool: pool, log: log, fails: newFailures()}
	p.dedupe.Store(dedupe)
	return p
}

// SetAdmin configures the protected /admin endpoints (D8).
func (p *Provider) SetAdmin(enabled bool, token string) {
	p.adminEnabled, p.adminToken = enabled, token
}

// Handler returns the provider's routes.
func (p *Provider) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/{channel}", p.send)
	mux.HandleFunc("GET /stats", p.stats)
	if p.metricsHandler != nil {
		mux.Handle("GET /metrics", p.metricsHandler)
	}
	mux.HandleFunc("GET /failures", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.fails.snapshot())
	})
	mux.Handle("POST /admin/dedupe", admin.Require(p.adminEnabled, p.adminToken, http.HandlerFunc(p.setDedupe)))
	mux.Handle("POST /admin/forget", admin.Require(p.adminEnabled, p.adminToken, http.HandlerFunc(p.forget)))
	mux.Handle("POST /admin/failures", admin.Require(p.adminEnabled, p.adminToken, http.HandlerFunc(p.setFailure)))
	mux.Handle("POST /admin/failures/clear", admin.Require(p.adminEnabled, p.adminToken, http.HandlerFunc(p.clearFailures)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := p.pool.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (p *Provider) send(w http.ResponseWriter, r *http.Request) {
	channel := events.Channel(r.PathValue("channel"))
	if !channel.Valid() {
		http.Error(w, "unknown channel", http.StatusNotFound)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		http.Error(w, "Idempotency-Key header required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || !json.Valid(body) {
		http.Error(w, "body must be valid JSON", http.StatusBadRequest)
		return
	}

	// Record the request in its own statement, before anything can fail, so
	// requests that error out are still counted (D5).
	if _, err := p.pool.Exec(r.Context(),
		`INSERT INTO requests (key, channel) VALUES ($1, $2)`, key, string(channel)); err != nil {
		p.fail(w, "record request", err)
		return
	}

	// Time from here on: this is the work a real provider would do for the request.
	start := time.Now()

	// Injected failure (D8): answered after the request was counted, before any effect.
	if p.applyFailure(w, r, channel) {
		p.metrics.Request(string(channel), metrics.ResultInjected, time.Since(start))
		return
	}

	response, replayed, err := p.execute(r.Context(), key, channel, body)
	if err != nil {
		p.metrics.Request(string(channel), metrics.ResultError, time.Since(start))
		p.fail(w, "execute", err)
		return
	}
	result := metrics.ResultExecuted
	if replayed {
		result = metrics.ResultReplayed
	}
	p.metrics.Request(string(channel), result, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	_, _ = w.Write(response)
}

// execute runs the effect at most once per key.
func (p *Provider) execute(ctx context.Context, key string, ch events.Channel, payload []byte) (resp []byte, replayed bool, err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	dedupe := p.dedupe.Load() // read once so one request is consistent
	if dedupe {
		// A concurrent request with the same key blocks here until the first
		// transaction ends, then sees 0 rows affected.
		tag, err := tx.Exec(ctx,
			`INSERT INTO idempotency_keys (key, channel, response) VALUES ($1, $2, '{}') ON CONFLICT (key) DO NOTHING`,
			key, string(ch))
		if err != nil {
			return nil, false, err
		}
		if tag.RowsAffected() == 0 {
			err := tx.QueryRow(ctx, `SELECT response FROM idempotency_keys WHERE key = $1`, key).Scan(&resp)
			return resp, true, err
		}
	}

	var effectID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO effects (key, channel, payload) VALUES ($1, $2, $3) RETURNING id`,
		key, string(ch), payload).Scan(&effectID); err != nil {
		return nil, false, err
	}
	resp, _ = json.Marshal(map[string]any{"status": "sent", "channel": ch, "effect_id": effectID})

	if dedupe {
		if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET response = $2 WHERE key = $1`, key, resp); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return resp, false, nil
}

// setDedupe switches idempotency on or off at runtime: {"enabled": false} is the
// negative-control mode. It is protected by the admin middleware.
func (p *Provider) setDedupe(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in); err != nil || in.Enabled == nil {
		http.Error(w, `body must be {"enabled": true|false}`, http.StatusBadRequest)
		return
	}
	p.dedupe.Store(*in.Enabled)
	p.log.Warn("provider idempotency switched", "enabled", *in.Enabled)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"dedupe": *in.Enabled})
}

// forget deletes the request, effect and key rows whose key starts with the given
// prefix ({"prefix": "<event id>:"}). The dashboard calls it after a negative-control
// experiment so its duplicate effects do not pollute the real counters.
func (p *Provider) forget(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in); err != nil || len(in.Prefix) < 36 || !keyPrefix.MatchString(in.Prefix) {
		http.Error(w, `body must be {"prefix": "<full event id>:"}`, http.StatusBadRequest)
		return
	}
	like := in.Prefix + "%"
	var removed int64
	for _, table := range []string{"effects", "requests", "idempotency_keys"} {
		tag, err := p.pool.Exec(r.Context(), "DELETE FROM "+table+" WHERE key LIKE $1", like)
		if err != nil {
			p.fail(w, "forget", err)
			return
		}
		removed += tag.RowsAffected()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int64{"removed": removed})
}

// Stats are the provider's honest counters (D5).
type Stats struct {
	Requests         int64 `json:"requests"`
	DistinctKeys     int64 `json:"distinct_keys"`
	Effects          int64 `json:"effects"`
	DuplicateEffects int64 `json:"duplicate_effects"`
	Dedupe           bool  `json:"dedupe"` // whether idempotency is currently on
}

// stats returns the counters. ?prefix=<event id>: restricts them to one event's keys.
func (p *Provider) stats(w http.ResponseWriter, r *http.Request) {
	like := "%"
	if prefix := r.URL.Query().Get("prefix"); prefix != "" {
		if !keyPrefix.MatchString(prefix) { // keys are "<uuid>:<channel>"; refuse LIKE wildcards
			http.Error(w, "invalid prefix", http.StatusBadRequest)
			return
		}
		like = prefix + "%"
	}
	var s Stats
	err := p.pool.QueryRow(r.Context(), `
		SELECT (SELECT count(*) FROM requests WHERE key LIKE $1),
		       (SELECT count(DISTINCT key) FROM requests WHERE key LIKE $1),
		       (SELECT count(*) FROM effects WHERE key LIKE $1),
		       (SELECT count(*) - count(DISTINCT key) FROM effects WHERE key LIKE $1)`, like).
		Scan(&s.Requests, &s.DistinctKeys, &s.Effects, &s.DuplicateEffects)
	s.Dedupe = p.dedupe.Load()
	if err != nil {
		p.fail(w, "stats", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}

func (p *Provider) fail(w http.ResponseWriter, what string, err error) {
	p.log.Error("provider error", "op", what, "error", fmt.Sprint(err))
	http.Error(w, "internal error", http.StatusInternalServerError)
}
