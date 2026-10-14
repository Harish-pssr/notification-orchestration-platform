-- Real email sending (D21). With the mock, the key and the effect are written in one transaction, so a key
-- row always means "done". A real email cannot be sent inside a database transaction, so a key now has a
-- state: IN_PROGRESS while the provider is sending, DONE once the effect and the response are recorded.
-- Existing rows, and every row written by the mock, are DONE.
ALTER TABLE idempotency_keys
    ADD COLUMN state      text        NOT NULL DEFAULT 'DONE' CHECK (state IN ('IN_PROGRESS', 'DONE')),
    ADD COLUMN started_at timestamptz NOT NULL DEFAULT now();

-- Settings the operator changes from the dashboard and that should survive a restart. The real-send switch is
-- deliberately NOT stored here: it always starts off after a restart.
CREATE TABLE provider_settings (
    key   text PRIMARY KEY,   -- e.g. email_to
    value text NOT NULL
);
