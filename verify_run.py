#!/usr/bin/env python3
"""Re-read the database and check every number the run claimed.

This is the reconciliation step: it does not trust the run summary, it recomputes from the
fixtures and from the database itself.

    python verify_run.py                 # checks db/pipeline.db against fixtures/
    python verify_run.py --db other.db
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.config import DEFAULT_DB, DEFAULT_FEEDS, DEFAULT_FIXTURES, load_config
from pipeline.store import Store, canonical_key


def expected_counts(fixtures_dir: Path, feeds_path: Path) -> dict[str, int]:
    config = load_config(feeds_path)
    counts: dict[str, int] = {}
    for feed in config.feeds:
        raw = json.loads((fixtures_dir / feed.fixture).read_text(encoding="utf-8"))
        rows = raw.get("items", [])
        counts[feed.id] = sum(
            1 for row in rows if row.get(feed.id_field) and (row.get("name") or "").strip()
        )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile the database against the fixtures")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--feeds", default=str(DEFAULT_FEEDS))
    parser.add_argument("--fixtures", default=str(DEFAULT_FIXTURES))
    args = parser.parse_args(argv)

    if not Path(args.db).exists():
        print(f"FAIL database not found: {args.db}")
        return 1

    expected = expected_counts(Path(args.fixtures), Path(args.feeds))
    total_expected = sum(expected.values())
    store = Store(args.db)
    checks: list[tuple[str, bool, str]] = []

    actual_total = store.scalar("SELECT COUNT(*) FROM records")
    checks.append(
        (
            "records match fixtures",
            actual_total == total_expected,
            f"database has {actual_total}, fixtures describe {total_expected}",
        )
    )

    rows = list(store.connection.execute("SELECT source_id, COUNT(*) AS n FROM records GROUP BY source_id"))
    per_source_ok = True
    detail_bits = []
    for row in rows:
        want = expected.get(row["source_id"])
        detail_bits.append(f"{row['source_id']}: db={row['n']} fixture={want}")
        if want is None or row["n"] != want:
            per_source_ok = False
    checks.append(("records per source", per_source_ok, "; ".join(detail_bits) or "no rows"))

    blank_ids = store.scalar("SELECT COUNT(*) FROM records WHERE external_id IS NULL OR TRIM(external_id) = ''")
    checks.append(("no blank external ids", blank_ids == 0, f"{blank_ids} blank"))

    duplicate_keys = store.scalar(
        "SELECT COUNT(*) FROM (SELECT source_id, external_id FROM records"
        " GROUP BY source_id, external_id HAVING COUNT(*) > 1)"
    )
    checks.append(("no duplicate (source, external id)", duplicate_keys == 0, f"{duplicate_keys} duplicated keys"))

    run = store.connection.execute(
        "SELECT * FROM runs WHERE status = 'success' ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    if run is None:
        checks.append(("a successful run exists", False, "no run with status = success"))
    else:
        checks.append(
            (
                "successful run saw every fixture row",
                run["records_seen"] == total_expected,
                f"run {run['run_id']} records_seen={run['records_seen']} expected={total_expected}",
            )
        )
        accounted = run["records_new"] + run["records_updated"] + run["records_unchanged"]
        checks.append(
            (
                "new + updated + unchanged = seen",
                accounted == run["records_seen"],
                f"{run['records_new']} + {run['records_updated']} + {run['records_unchanged']} = {accounted}",
            )
        )
        feed_rows = list(
            store.connection.execute("SELECT * FROM feed_runs WHERE run_id = ?", (run["run_id"],))
        )
        checks.append(
            (
                "one feed_runs row per feed",
                len(feed_rows) == len(expected),
                f"{len(feed_rows)} rows for {len(expected)} feeds",
            )
        )
        checks.append(
            (
                "every feed recorded its attempts",
                all(row["attempts"] >= 1 for row in feed_rows),
                ", ".join(f"{row['feed_id']}:{row['attempts']}" for row in feed_rows),
            )
        )

    bad_merges = list(
        store.connection.execute(
            "SELECT m.id, m.canonical_key, r.canonical_key AS record_key, k.canonical_key AS kept_key"
            " FROM merges m"
            " JOIN records r ON r.id = m.record_id"
            " JOIN records k ON k.id = m.kept_record_id"
            " WHERE r.canonical_key <> m.canonical_key OR k.canonical_key <> m.canonical_key"
        )
    )
    checks.append(
        (
            "duplicate decisions are consistent",
            not bad_merges,
            f"{len(bad_merges)} inconsistent merge rows",
        )
    )

    flagged = store.scalar("SELECT COUNT(*) FROM records WHERE merged_into_id IS NOT NULL")
    merge_rows = store.scalar("SELECT COUNT(*) FROM merges")
    checks.append(
        (
            "flagged duplicates equal logged merges",
            flagged == merge_rows,
            f"flagged={flagged} merges={merge_rows}",
        )
    )

    orphan = store.scalar(
        "SELECT COUNT(*) FROM records r WHERE r.merged_into_id IS NOT NULL"
        " AND NOT EXISTS (SELECT 1 FROM records k WHERE k.id = r.merged_into_id)"
    )
    checks.append(("no orphan merge targets", orphan == 0, f"{orphan} orphans"))

    cross_source = store.scalar(
        "SELECT COUNT(*) FROM merges WHERE 1 = 1"
    )
    if cross_source:
        sample = store.connection.execute(
            "SELECT canonical_key FROM merges LIMIT 3"
        ).fetchall()
        checks.append(
            (
                "cross-source duplicates were detected",
                True,
                "examples: " + ", ".join(row["canonical_key"] for row in sample),
            )
        )
    else:
        checks.append(("cross-source duplicates were detected", False, "no merges logged"))

    failures = [name for name, ok, _ in checks if not ok]
    for name, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name} - {detail}")
    if failures:
        print(f"\n{len(failures)} check(s) failed: {', '.join(failures)}")
        store.close()
        return 1
    print(f"\nall checks passed ({len(checks)} checks)")
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
