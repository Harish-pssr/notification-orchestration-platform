-- Honest counting (D5). Requests and effects are recorded separately so duplicate
-- deliveries and duplicate side effects can be measured independently.
--   duplicate deliveries    = requests - distinct request keys
--   duplicate side effects  = effects  - distinct effect keys
--   duplicates suppressed   = requests - effects
-- Neither table has a unique constraint: a constraint here would hide duplicates.
CREATE TABLE requests (
    id          bigserial   PRIMARY KEY,
    key         text        NOT NULL,
    channel     text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE effects (
    id          bigserial   PRIMARY KEY,
    key         text        NOT NULL,
    channel     text        NOT NULL,
    payload     jsonb       NOT NULL,
    executed_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX effects_key_idx ON effects (key);
CREATE INDEX requests_key_idx ON requests (key);
