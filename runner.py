#!/usr/bin/env python3
"""One scheduled run of the pipeline.

    python runner.py --offline                              # rehearsal against local fixtures
    python runner.py --online                               # fetch the configured URLs
    python runner.py --offline --inject-failure feed_b      # rehearse the alert path

Exit codes: 0 = every source ok, 2 = at least one source failed, 3 = every source failed.
The non-zero exit code is what makes a scheduler (n8n, cron, Task Scheduler) notify a human.
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import DEFAULT_DB, DEFAULT_FEEDS, DEFAULT_FIXTURES, DEFAULT_OUTPUT, load_config
from pipeline.report import write_status_report
from pipeline.sources import Fetcher
from pipeline.store import Store


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_run_id() -> str:
    return f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(2)}"


def append_alert(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="One scheduled run of the pipeline")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline", action="store_true", help="read local fixtures (default)")
    mode.add_argument("--online", action="store_true", help="fetch the configured URLs")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--feeds", default=str(DEFAULT_FEEDS))
    parser.add_argument("--fixtures", default=str(DEFAULT_FIXTURES))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--inject-failure", action="append", default=[], metavar="FEED_ID")
    parser.add_argument("--retries", type=int, default=None, help="override retries for every feed")
    args = parser.parse_args(argv)

    config = load_config(args.feeds)
    feeds = config.feeds
    if args.retries is not None:
        feeds = [replace(feed, max_retries=args.retries) for feed in feeds]

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    alerts_path = output_dir / "alerts.log"
    summary_path = output_dir / "run_summary.json"
    report_path = output_dir / "status_report.md"

    store = Store(args.db)
    store.init_schema()
    fetcher = Fetcher(
        fixtures_dir=Path(args.fixtures),
        offline=not args.online,
        inject_failures=tuple(args.inject_failure),
        backoff_seconds=config.backoff_seconds,
        invalid_row_tolerance=config.invalid_row_tolerance,
    )

    run_id = new_run_id()
    started_at = utc_now()
    store.start_run(run_id, started_at)

    totals = {"seen": 0, "new": 0, "updated": 0, "unchanged": 0, "invalid": 0, "merged": 0}
    feed_status: list[dict] = []

    for feed in feeds:
        result = fetcher.fetch(feed)
        if result.ok:
            counters = store.upsert_records(run_id, feed.id, result.records, utc_now())
            totals["seen"] += len(result.records)
            totals["invalid"] += result.invalid
            for key in ("new", "updated", "unchanged", "merged"):
                totals[key] += counters[key]
            store.record_feed_run(run_id, feed.id, "ok", result.attempts, len(result.records), result.invalid, None)
            feed_status.append(
                {
                    "feed_id": feed.id,
                    "status": "ok",
                    "attempts": result.attempts,
                    "records": len(result.records),
                    "invalid": result.invalid,
                }
            )
        else:
            message = f"source {feed.id} failed after {result.attempts} attempts: {result.error}"
            append_alert(
                alerts_path,
                {
                    "run_id": run_id,
                    "feed_id": feed.id,
                    "level": "error",
                    "message": message,
                    "at": utc_now(),
                },
            )
            store.record_feed_run(run_id, feed.id, "failed", result.attempts, 0, 0, result.error)
            feed_status.append(
                {"feed_id": feed.id, "status": "failed", "attempts": result.attempts, "error": result.error}
            )

    feeds_ok = sum(1 for item in feed_status if item["status"] == "ok")
    feeds_failed = len(feed_status) - feeds_ok
    if feeds_failed == 0:
        status, exit_code = "success", 0
    elif feeds_ok == 0:
        status, exit_code = "failed", 3
    else:
        status, exit_code = "partial", 2

    message = "all sources ok" if status == "success" else f"{feeds_failed} source(s) failed - alerter fired"
    store.finish_run(
        run_id,
        finished_at=utc_now(),
        status=status,
        feeds_total=len(feed_status),
        feeds_ok=feeds_ok,
        feeds_failed=feeds_failed,
        records_seen=totals["seen"],
        records_new=totals["new"],
        records_updated=totals["updated"],
        records_unchanged=totals["unchanged"],
        records_invalid=totals["invalid"],
        records_merged=totals["merged"],
        message=message,
    )
    write_status_report(store, report_path)

    summary = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "status": status,
        "feeds_total": len(feed_status),
        "feeds_ok": feeds_ok,
        "feeds_failed": feeds_failed,
        "records_seen": totals["seen"],
        "records_new": totals["new"],
        "records_updated": totals["updated"],
        "records_unchanged": totals["unchanged"],
        "records_invalid": totals["invalid"],
        "records_merged": totals["merged"],
        "feeds": feed_status,
        "database": Path(args.db).name,
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    store.close()

    if status == "success":
        print(
            "run complete: {run} feeds_ok={ok} seen={seen} new={new} updated={updated} "
            "unchanged={unchanged} invalid={invalid} duplicates={merged}".format(
                run=run_id, ok=feeds_ok, seen=totals["seen"], new=totals["new"],
                updated=totals["updated"], unchanged=totals["unchanged"],
                invalid=totals["invalid"], merged=totals["merged"],
            )
        )
    else:
        for item in feed_status:
            if item["status"] != "ok":
                print(f"alert: {item['feed_id']} failed - {item.get('error')}")
        print(
            f"run incomplete: {run_id} status={status} feeds_ok={feeds_ok}/{len(feed_status)} "
            f"alert_file={alerts_path.name} exit={exit_code}"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
