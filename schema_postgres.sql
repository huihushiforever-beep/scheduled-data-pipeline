-- Same model on Postgres / Supabase (Supabase runs Postgres).
-- Differences from the SQLite file are the identity columns only.

CREATE TABLE IF NOT EXISTS records (
    id              BIGSERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    name            TEXT NOT NULL,
    city            TEXT NOT NULL DEFAULT '',
    category        TEXT NOT NULL DEFAULT '',
    website         TEXT NOT NULL DEFAULT '',
    canonical_key   TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    merged_into_id  BIGINT REFERENCES records(id),
    UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_records_canonical ON records (canonical_key);

CREATE TABLE IF NOT EXISTS runs (
    run_id             TEXT PRIMARY KEY,
    started_at         TIMESTAMPTZ NOT NULL,
    finished_at        TIMESTAMPTZ,
    status             TEXT NOT NULL,
    feeds_total        INTEGER NOT NULL DEFAULT 0,
    feeds_ok           INTEGER NOT NULL DEFAULT 0,
    feeds_failed       INTEGER NOT NULL DEFAULT 0,
    records_seen       INTEGER NOT NULL DEFAULT 0,
    records_new        INTEGER NOT NULL DEFAULT 0,
    records_updated    INTEGER NOT NULL DEFAULT 0,
    records_unchanged  INTEGER NOT NULL DEFAULT 0,
    records_invalid    INTEGER NOT NULL DEFAULT 0,
    records_merged     INTEGER NOT NULL DEFAULT 0,
    message            TEXT
);

CREATE TABLE IF NOT EXISTS feed_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    feed_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    records_seen    INTEGER NOT NULL DEFAULT 0,
    records_invalid INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS merges (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    record_id       BIGINT NOT NULL REFERENCES records(id),
    canonical_key   TEXT NOT NULL,
    kept_record_id  BIGINT NOT NULL REFERENCES records(id),
    created_at      TIMESTAMPTZ NOT NULL
);
