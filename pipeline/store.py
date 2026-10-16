"""Storage layer: idempotent upserts, one row per run, and a visible record of duplicates.

The whole point of this file is that running the pipeline twice changes nothing the second time,
and that a cross-source duplicate is written down instead of being merged behind your back.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"

LEGAL_SUFFIXES = {"ltd", "limited", "co", "company", "inc", "plc", "llp", "group", "services", "and", "the"}


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def canonical_key(name: str, city: str) -> str:
    tokens = [token for token in normalize_text(name).split() if token not in LEGAL_SUFFIXES]
    return f"{' '.join(tokens)}|{normalize_text(city)}"


def payload_hash(record: dict) -> str:
    joined = "|".join(
        normalize_text(str(record.get(field, ""))) for field in ("name", "city", "category", "website")
    )
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]


class Store:
    def __init__(self, db_path: Path | str, schema_path: Path | str = SCHEMA_PATH) -> None:
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.db_path))
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def init_schema(self) -> None:
        self.connection.executescript(self.schema_path.read_text(encoding="utf-8"))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    # -- runs ---------------------------------------------------------------
    def start_run(self, run_id: str, started_at: str) -> None:
        self.connection.execute(
            "INSERT INTO runs (run_id, started_at, status) VALUES (?, ?, 'running')",
            (run_id, started_at),
        )
        self.connection.commit()

    def finish_run(self, run_id: str, **fields) -> None:
        columns = ", ".join(f"{key} = ?" for key in fields)
        self.connection.execute(
            f"UPDATE runs SET {columns} WHERE run_id = ?", (*fields.values(), run_id)
        )
        self.connection.commit()

    def record_feed_run(
        self,
        run_id: str,
        feed_id: str,
        status: str,
        attempts: int,
        records_seen: int,
        records_invalid: int,
        error: str | None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO feed_runs (run_id, feed_id, status, attempts, records_seen, records_invalid, error)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, feed_id, status, attempts, records_seen, records_invalid, error),
        )
        self.connection.commit()

    # -- records ------------------------------------------------------------
    def upsert_records(self, run_id: str, source_id: str, records: list[dict], now: str) -> dict:
        counters = {"new": 0, "updated": 0, "unchanged": 0, "merged": 0}
        for record in records:
            digest = payload_hash(record)
            key = canonical_key(record["name"], record.get("city", ""))
            existing = self.connection.execute(
                "SELECT id, payload_hash FROM records WHERE source_id = ? AND external_id = ?",
                (source_id, record["external_id"]),
            ).fetchone()

            if existing is None:
                cursor = self.connection.execute(
                    "INSERT INTO records (source_id, external_id, name, city, category, website,"
                    " canonical_key, payload_hash, first_seen_at, last_seen_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        source_id,
                        record["external_id"],
                        record["name"],
                        record.get("city", ""),
                        record.get("category", ""),
                        record.get("website", ""),
                        key,
                        digest,
                        now,
                        now,
                    ),
                )
                counters["new"] += 1
                twin = self.connection.execute(
                    "SELECT id FROM records WHERE canonical_key = ? AND id <> ? LIMIT 1",
                    (key, cursor.lastrowid),
                ).fetchone()
                if twin is not None:
                    self.connection.execute(
                        "INSERT INTO merges (run_id, record_id, canonical_key, kept_record_id, created_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (run_id, cursor.lastrowid, key, twin["id"], now),
                    )
                    self.connection.execute(
                        "UPDATE records SET merged_into_id = ? WHERE id = ?", (twin["id"], cursor.lastrowid)
                    )
                    counters["merged"] += 1
            elif existing["payload_hash"] != digest:
                self.connection.execute(
                    "UPDATE records SET name = ?, city = ?, category = ?, website = ?, canonical_key = ?,"
                    " payload_hash = ?, last_seen_at = ? WHERE id = ?",
                    (
                        record["name"],
                        record.get("city", ""),
                        record.get("category", ""),
                        record.get("website", ""),
                        key,
                        digest,
                        now,
                        existing["id"],
                    ),
                )
                counters["updated"] += 1
            else:
                counters["unchanged"] += 1
        self.connection.commit()
        return counters

    # -- reads used by the report and the reconciliation --------------------
    def recent_runs(self, limit: int = 10) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            )
        )

    def counts_by_source(self) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                "SELECT source_id, COUNT(*) AS records, SUM(merged_into_id IS NOT NULL) AS duplicates"
                " FROM records GROUP BY source_id ORDER BY source_id"
            )
        )

    def scalar(self, sql: str, params: tuple = ()) -> int:
        row = self.connection.execute(sql, params).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
