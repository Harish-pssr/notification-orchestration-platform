-- Worker heartbeats (D9). Each relay/consumer instance upserts its row every few
-- seconds; a role is UP when at least one instance has a fresh last_seen.
CREATE TABLE workers (
    name       text        PRIMARY KEY,                 -- e.g. relay-1, consumer-email-2
    role       text        NOT NULL,                    -- relay | consumer-email | consumer-push | consumer-webhook
    state      text        NOT NULL DEFAULT 'RUNNING',  -- RUNNING | PAUSED (set by fault hooks)
    started_at timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now()
);
