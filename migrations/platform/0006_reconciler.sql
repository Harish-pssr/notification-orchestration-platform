-- Reconciler support (D22).
-- reconcile_count is how many times the reconciler republished this delivery because
-- the broker seemed to have lost it. It (a) caps the repairs so a delivery that never
-- completes cannot loop forever, (b) stretches the wait before each further repair,
-- and (c) goes into the NATS message ID of the republished message, so the broker's
-- dedup window cannot drop it. The provider's idempotency key does NOT include it,
-- so a repair of a delivery whose effect already ran is a harmless repeat (D5).
ALTER TABLE channel_deliveries ADD COLUMN reconcile_count int NOT NULL DEFAULT 0;
