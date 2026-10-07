// Command loadgen prepares and reads benchmark runs.
//
//	loadgen reset -confirm        clear all events, deliveries, provider data and the stream
//	loadgen seed -n 20000 [-delay 5s]  bulk-insert n PaymentSucceeded events straight into the outbox
//	loadgen report [-json]        print exact counts and timings read from the database
//
// Seeding bypasses the HTTP API on purpose: relay-scaling runs measure the relay,
// not the API's insert path. Every timing in the report comes from database
// timestamps, never from this tool's own clock (D9).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
	"notifplatform/internal/events"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "reset":
		err = reset(os.Args[2:])
	case "seed":
		err = seed(os.Args[2:])
	case "report":
		err = report(os.Args[2:])
	case "wait":
		err = wait(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: loadgen reset -confirm | seed -n N | wait -n N [-delivered] | report [-json|-csv]")
	os.Exit(2)
}

// wait blocks until n events are PUBLISHED (or, with -delivered, until 3n deliveries
// are DELIVERED), and fails if the timeout passes first.
func wait(args []string) error {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	n := fs.Int64("n", 0, "number of events to wait for")
	delivered := fs.Bool("delivered", false, "wait for every delivery to be DELIVERED instead")
	timeout := fs.Duration("timeout", 5*time.Minute, "give up after this long")
	_ = fs.Parse(args)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, config.Load().PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	query := `SELECT count(*) FROM outbox_events WHERE status = 'PUBLISHED'`
	want := *n
	if *delivered {
		query = `SELECT count(*) FROM channel_deliveries WHERE status = 'DELIVERED'`
		want = *n * 3
	}
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		var got int64
		if err := pool.QueryRow(ctx, query).Scan(&got); err != nil {
			return err
		}
		if got >= want {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s waiting for %d", *timeout, want)
}

func reset(args []string) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	confirm := fs.Bool("confirm", false, "required: this deletes all data")
	_ = fs.Parse(args)
	if !*confirm {
		return fmt.Errorf("refusing to delete data without -confirm")
	}
	ctx := context.Background()
	cfg := config.Load()

	platform, err := pgxpool.New(ctx, cfg.PlatformDSN)
	if err != nil {
		return err
	}
	defer platform.Close()
	if _, err := platform.Exec(ctx, `TRUNCATE channel_deliveries, outbox_events, api_idempotency, payments, bookings, password_changes`); err != nil {
		return fmt.Errorf("truncate platform: %w", err)
	}

	provider, err := pgxpool.New(ctx, cfg.ProviderDSN)
	if err != nil {
		return err
	}
	defer provider.Close()
	if _, err := provider.Exec(ctx, `TRUNCATE requests, effects, idempotency_keys`); err != nil {
		return fmt.Errorf("truncate provider: %w", err)
	}

	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if err := broker.EnsureStream(ctx, js); err != nil {
		return err
	}
	stream, err := js.Stream(ctx, broker.StreamName)
	if err != nil {
		return err
	}
	if err := stream.Purge(ctx); err != nil {
		return fmt.Errorf("purge stream: %w", err)
	}
	fmt.Println("reset: platform and provider tables truncated, stream purged")
	return nil
}

func seed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	n := fs.Int("n", 10000, "number of events")
	delay := fs.Duration("delay", 0, "make events due only after this long, so idle relays all start together")
	_ = fs.Parse(args)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, config.Load().PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	channels := make([]string, 0, 3)
	for _, ch := range events.RoutesFor(events.PaymentSucceeded) {
		channels = append(channels, string(ch))
	}

	const batch = 5000
	started := time.Now()
	for done := 0; done < *n; done += batch {
		size := min(batch, *n-done)
		ids := make([]string, size)
		aggregates := make([]string, size)
		for i := range ids {
			id, err := uuid.NewV7()
			if err != nil {
				return err
			}
			ids[i], aggregates[i] = id.String(), uuid.NewString()
		}
		// One statement per table inserts a whole batch (unnest turns arrays into rows).
		// The due time is computed by the database (now() + delay), so no clock other
		// than the database's is involved.
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (id, event_type, aggregate_type, aggregate_id, payload, next_attempt_at)
			SELECT i, $2, 'payment', a, '{"seed":true}', now() + make_interval(secs => $4)
			FROM unnest($1::uuid[], $3::text[]) AS t(i, a)`,
			ids, string(events.PaymentSucceeded), aggregates, delay.Seconds()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("insert outbox: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO channel_deliveries (event_id, channel)
			SELECT i, c FROM unnest($1::uuid[]) AS i CROSS JOIN unnest($2::text[]) AS c`,
			ids, channels); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("insert deliveries: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	fmt.Printf("seeded %d events (%d deliveries) in %s, due in %s\n", *n, *n*len(channels), time.Since(started).Round(time.Millisecond), *delay)
	return nil
}

// Report is the exact state of a run, read from the database.
type Report struct {
	Events          int64   `json:"events"`
	Published       int64   `json:"published"`
	DLQ             int64   `json:"dlq"`
	Deliveries      int64   `json:"deliveries"`
	Delivered       int64   `json:"delivered"`
	PublishWindowS  float64 `json:"publish_window_s"`  // first to last published_at
	PublishRate     float64 `json:"publish_events_s"`  // published events / window
	DeliverWindowS  float64 `json:"deliver_window_s"`  // first to last delivered_at
	DeliverRate     float64 `json:"deliver_per_s"`     // delivered notifications / window
	DrainFromSeedS  float64 `json:"drain_from_seed_s"` // seed time to last published_at
	MaxAttempts     int64   `json:"max_attempts"`
	ProviderEffects int64   `json:"provider_effects"`
	ProviderDupes   int64   `json:"provider_duplicate_effects"`
}

func report(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable output")
	asCSV := fs.Bool("csv", false, "one line: published,window_s,events_per_s,max_attempts,dlq,deliveries_delivered")
	_ = fs.Parse(args)
	ctx := context.Background()
	cfg := config.Load()

	pool, err := pgxpool.New(ctx, cfg.PlatformDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	var r Report
	var pubFirst, pubLast, delFirst, delLast, created *time.Time
	err = pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status = 'PUBLISHED'),
		       count(*) FILTER (WHERE status = 'DLQ'),
		       coalesce(max(attempts), 0),
		       min(published_at), max(published_at), min(created_at)
		FROM outbox_events`).Scan(&r.Events, &r.Published, &r.DLQ, &r.MaxAttempts, &pubFirst, &pubLast, &created)
	if err != nil {
		return fmt.Errorf("event counts: %w", err)
	}
	err = pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE status = 'DELIVERED'), min(delivered_at), max(delivered_at)
		FROM channel_deliveries`).Scan(&r.Deliveries, &r.Delivered, &delFirst, &delLast)
	if err != nil {
		return fmt.Errorf("delivery counts: %w", err)
	}

	if pubFirst != nil && pubLast != nil {
		r.PublishWindowS = pubLast.Sub(*pubFirst).Seconds()
		if r.PublishWindowS > 0 {
			r.PublishRate = float64(r.Published) / r.PublishWindowS
		}
		if created != nil {
			r.DrainFromSeedS = pubLast.Sub(*created).Seconds()
		}
	}
	if delFirst != nil && delLast != nil {
		r.DeliverWindowS = delLast.Sub(*delFirst).Seconds()
		if r.DeliverWindowS > 0 {
			r.DeliverRate = float64(r.Delivered) / r.DeliverWindowS
		}
	}

	if prov, err := pgxpool.New(ctx, cfg.ProviderDSN); err == nil {
		defer prov.Close()
		_ = prov.QueryRow(ctx, `SELECT count(*), count(*) - count(DISTINCT key) FROM effects`).Scan(&r.ProviderEffects, &r.ProviderDupes)
	}

	if *asCSV {
		fmt.Printf("%d,%.3f,%.0f,%d,%d,%d\n", r.Published, r.PublishWindowS, r.PublishRate, r.MaxAttempts, r.DLQ, r.Delivered)
		return nil
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	fmt.Printf("events          %d (published %d, dlq %d, max attempts %d)\n", r.Events, r.Published, r.DLQ, r.MaxAttempts)
	fmt.Printf("publish window  %.2fs  ->  %.0f events/s\n", r.PublishWindowS, r.PublishRate)
	fmt.Printf("deliveries      %d of %d delivered, window %.2fs -> %.0f/s\n", r.Delivered, r.Deliveries, r.DeliverWindowS, r.DeliverRate)
	fmt.Printf("provider        %d effects, %d duplicate effects\n", r.ProviderEffects, r.ProviderDupes)
	return nil
}
