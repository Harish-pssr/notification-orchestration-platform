-- Requests that were answered with an injected failure (D8) before any effect ran. They
-- are counted in `requests`, so without this table a retry after an injected failure
-- looks like a duplicate delivery. Reports subtract these rows.
CREATE TABLE injected_failures (
    id          bigserial   PRIMARY KEY,
    key         text        NOT NULL,
    channel     text        NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);
