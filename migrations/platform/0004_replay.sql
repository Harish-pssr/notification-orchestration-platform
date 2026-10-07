-- Replay support (D7).
-- replay_count goes into the NATS message ID of a replayed delivery, so the broker's
-- dedup window cannot silently drop a replay that follows the original publish
-- within two minutes. The provider's idempotency key does NOT include it, so a
-- replay of an effect that already ran is still a harmless repeat (D5).
ALTER TABLE channel_deliveries ADD COLUMN replay_count int NOT NULL DEFAULT 0;

-- Events the relay itself gave up on (outbox status DLQ) are listed on the dead-letter page.
CREATE INDEX outbox_dlq_idx ON outbox_events (id) WHERE status = 'DLQ';
