-- Fault injection state (D8). One row per injection point; polled and cached by
-- every process. `remaining` limits how many times a fault fires (NULL = unlimited);
-- it is consumed with an atomic UPDATE so "crash once" fires in exactly one process.
CREATE TABLE fault_config (
    point       text             PRIMARY KEY,             -- e.g. relay.after_publish
    action      text             NOT NULL,                -- crash | duplicate | disable ...
    params      jsonb            NOT NULL DEFAULT '{}',
    remaining   int              CHECK (remaining >= 0),
    probability double precision NOT NULL DEFAULT 1 CHECK (probability > 0 AND probability <= 1),
    updated_at  timestamptz      NOT NULL DEFAULT now()
);

-- Consumer-side duplicate counter (D5): how many messages arrived for a delivery
-- that was already DELIVERED and were ignored.
ALTER TABLE channel_deliveries ADD COLUMN duplicate_count int NOT NULL DEFAULT 0;
