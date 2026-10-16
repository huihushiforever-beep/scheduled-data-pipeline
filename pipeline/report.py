"""The human-readable status page: what ran, what it produced, what needs attention."""

from __future__ import annotations

import time
from pathlib import Path

from .store import Store


def _fmt(value) -> str:
    return "-" if value is None else str(value)


def write_status_report(store: Store, path: Path, limit: int = 10) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    runs = store.recent_runs(limit)
    by_source = store.counts_by_source()
    total_records = store.scalar("SELECT COUNT(*) FROM records")
    duplicate_records = store.scalar("SELECT COUNT(*) FROM records WHERE merged_into_id IS NOT NULL")
    merge_rows = store.scalar("SELECT COUNT(*) FROM merges")

    lines = [
        "# Pipeline status",
        "",
        f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"Records held: **{total_records}**  ",
        f"Cross-source duplicates flagged: **{duplicate_records}** (merge decisions logged: {merge_rows})",
        "",
        "## Recent runs",
        "",
        "| run | started | status | feeds ok/failed | new | updated | unchanged | invalid | alert |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for run in runs:
        lines.append(
            "| {run_id} | {started} | {status} | {ok}/{failed} | {new} | {updated} | {unchanged} | {invalid} | {message} |".format(
                run_id=run["run_id"],
                started=run["started_at"],
                status=run["status"],
                ok=run["feeds_ok"],
                failed=run["feeds_failed"],
                new=run["records_new"],
                updated=run["records_updated"],
                unchanged=run["records_unchanged"],
                invalid=run["records_invalid"],
                message=_fmt(run["message"]),
            )
        )
    lines += ["", "## Records by source", "", "| source | records | flagged as duplicate |", "|---|---|---|"]
    for row in by_source:
        lines.append(f"| {row['source_id']} | {row['records']} | {row['duplicates']} |")
    lines += [
        "",
        "## If something looks wrong",
        "",
        "1. A run marked `partial` means at least one source failed after its retries - read the alert line",
        "   for that run and check the source before the next scheduled run.",
        "2. A source that keeps returning fewer records than usual usually means its page or feed changed;",
        "   that is what the per-source counts above are for.",
        "3. A record flagged as a duplicate keeps both rows and a merge entry, so a wrong match can be undone",
        "   without losing the original.",
    ]
    text = "\n".join(lines) + "\n"
    path.write_text(text, encoding="utf-8")
    return path
