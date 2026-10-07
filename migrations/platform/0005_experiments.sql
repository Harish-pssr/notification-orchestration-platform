-- What a worker is configured with, so the dashboard can show what is being varied
-- (D16): replica counts come from the heartbeats, the settings from this column.
ALTER TABLE workers ADD COLUMN config jsonb NOT NULL DEFAULT '{}';

-- Markers written by the benchmark tool while an experiment runs (D16), so the
-- dashboard can show which run and phase is in progress and when each phase began.
-- They are not business data: the benchmark's reset never touches this table.
CREATE TABLE experiment_markers (
    id         bigserial   PRIMARY KEY,
    run        text        NOT NULL,                 -- e.g. reliability-2026-10-07T16:40:00Z
    kind       text        NOT NULL,                 -- start | phase | end
    label      text        NOT NULL,                 -- human text: "relay crash 2 of 3"
    params     jsonb       NOT NULL DEFAULT '{}',    -- what is being varied: rate, relays, ...
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX experiment_markers_run_idx ON experiment_markers (run, id);
