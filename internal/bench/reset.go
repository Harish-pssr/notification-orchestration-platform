package bench

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/broker"
	"notifplatform/internal/config"
)

// Reset deletes all events, deliveries and provider data and purges the broker stream,
// so every benchmark step starts from the same empty state. It is destructive and only
// ever called by the benchmark tools, which require an explicit -confirm.
func Reset(ctx context.Context) error {
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
	return nil
}
