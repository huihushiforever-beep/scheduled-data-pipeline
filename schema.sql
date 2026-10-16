-- SQLite schema used by the rehearsal run (db/pipeline.db).
-- Every write is idempotent: re-running the pipeline never duplicates a record.

CREATE TABLE IF NOT EXISTS records (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    name            TEXT NOT NULL,
    city            TEXT NOT NULL DEFAULT '',
    category        TEXT NOT NULL DEFAULT '',
    website         TEXT NOT NULL DEFAULT '',
    canonical_key   TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT NOT NULL,
    merged_into_id  INTEGER REFERENCES records(id),
    UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_records_canonical ON records (canonical_key);

CREATE TABLE IF NOT EXISTS runs (
    run_id             TEXT PRIMARY KEY,
    started_at         TEXT NOT NULL,
    finished_at        TEXT,
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
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL REFERENCES runs(run_id),
    feed_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    records_seen    INTEGER NOT NULL DEFAULT 0,
    records_invalid INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_feed_runs_run ON feed_runs (run_id);

-- Cross-source duplicates are recorded here instead of being merged silently,
-- so a wrong merge is visible and reversible.
CREATE TABLE IF NOT EXISTS merges (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    record_id       INTEGER NOT NULL REFERENCES records(id),
    canonical_key   TEXT NOT NULL,
    kept_record_id  INTEGER NOT NULL REFERENCES records(id),
    created_at      TEXT NOT NULL
);
