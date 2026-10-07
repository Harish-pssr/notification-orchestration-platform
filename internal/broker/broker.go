// Package broker creates the JetStream stream and consumers (D11).
package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"notifplatform/internal/events"
)

// StreamName is the single stream holding all channel subjects.
const StreamName = "NOTIFY"

// EnsureStream creates the stream if missing and updates it otherwise. It is safe
// to call from every process at start-up.
func EnsureStream(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{"notify.*"},
		// WorkQueue: a message is deleted once acknowledged. Valid because every
		// consumer filters on its own subject and none overlap.
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
		Replicas:  1,
		// When full, reject new publishes instead of dropping old messages. The
		// relay then keeps the event in the outbox and retries: backpressure, no loss.
		Discard:    jetstream.DiscardNew,
		MaxBytes:   1 << 30, // 1 GiB
		MaxAge:     7 * 24 * time.Hour,
		Duplicates: 2 * time.Minute, // dedup window; an optimization only (D1)
	})
	if err != nil {
		return fmt.Errorf("ensure stream: %w", err)
	}
	return nil
}

// EnsureConsumer creates or updates the durable pull consumer for a channel.
// maxDeliver is only a backstop above our own max-attempt logic (D11).
func EnsureConsumer(ctx context.Context, js jetstream.JetStream, ch events.Channel, maxDeliver int) (jetstream.Consumer, error) {
	cons, err := js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:       string(ch),
		FilterSubject: ch.Subject(),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    maxDeliver,
		MaxAckPending: 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %s: %w", ch, err)
	}
	return cons, nil
}
