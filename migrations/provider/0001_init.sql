-- Mock external provider (D5). The idempotency key and the effect are written in
-- one transaction under this primary key, so a repeated key cannot run the effect
-- twice. Request/effect counting tables arrive with the idempotency slice.
CREATE TABLE idempotency_keys (
    key        text        PRIMARY KEY,   -- "<event_id>:<channel>"
    channel    text        NOT NULL,
    response   jsonb       NOT NULL,      -- stored result returned on a repeat
    created_at timestamptz NOT NULL DEFAULT now()
);
