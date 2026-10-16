"""Fetching a feed, with retries and backoff, and a deterministic way to rehearse failure.

Two modes:
  * offline rehearsal (default) - reads a local fixture, so the pipeline can be run and verified
    anywhere without hitting someone else's server;
  * online - fetches the configured URL with a timeout.

`--inject-failure <feed_id>` makes that feed fail every attempt, which is how the retry and the
alert path are demonstrated end to end.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from .config import Feed


@dataclass
class FetchResult:
    feed_id: str
    ok: bool
    records: list[dict] = field(default_factory=list)
    invalid: int = 0
    attempts: int = 0
    error: str | None = None


class Fetcher:
    def __init__(
        self,
        fixtures_dir: Path,
        offline: bool = True,
        inject_failures: tuple[str, ...] = (),
        backoff_seconds: float = 0.2,
        invalid_row_tolerance: float = 0.34,
        timeout_seconds: float = 15.0,
        sleep=time.sleep,
    ) -> None:
        self.fixtures_dir = Path(fixtures_dir)
        self.offline = offline
        self.inject_failures = set(inject_failures)
        self.backoff_seconds = backoff_seconds
        self.invalid_row_tolerance = invalid_row_tolerance
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep

    # -- loading ------------------------------------------------------------
    def _load_raw(self, feed: Feed) -> dict:
        if feed.id in self.inject_failures:
            raise RuntimeError("injected failure (rehearsal)")
        if self.offline:
            path = self.fixtures_dir / feed.fixture
            if not path.exists():
                raise FileNotFoundError(f"fixture not found: {path.name}")
            return json.loads(path.read_text(encoding="utf-8"))
        if not feed.url:
            raise ValueError(f"feed {feed.id} has no url configured")
        request = urllib.request.Request(feed.url, headers={"User-Agent": "pipeline-rehearsal/1.0"})
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    # -- validation ---------------------------------------------------------
    def _validate(self, feed: Feed, items: list[dict]) -> tuple[list[dict], int]:
        good: list[dict] = []
        invalid = 0
        for item in items:
            external_id = item.get(feed.id_field)
            name = (item.get("name") or "").strip()
            if not external_id or not name:
                invalid += 1
                continue
            good.append(
                {
                    "external_id": str(external_id).strip(),
                    "name": name,
                    "city": (item.get("city") or "").strip(),
                    "category": (item.get("category") or "").strip(),
                    "website": (item.get("website") or "").strip(),
                }
            )
        return good, invalid

    # -- public -------------------------------------------------------------
    def fetch(self, feed: Feed) -> FetchResult:
        last_error: str | None = None
        for attempt in range(1, feed.max_retries + 1):
            try:
                raw = self._load_raw(feed)
                items = raw.get("items") if isinstance(raw, dict) else raw
                if not isinstance(items, list):
                    raise ValueError("payload has no 'items' list")
                records, invalid = self._validate(feed, items)
                if items and invalid / len(items) > self.invalid_row_tolerance:
                    raise ValueError(f"too many unusable rows: {invalid}/{len(items)}")
                return FetchResult(feed.id, True, records, invalid, attempt, None)
            except (RuntimeError, ValueError, FileNotFoundError, urllib.error.URLError, OSError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < feed.max_retries:
                    self.sleep(self.backoff_seconds * (2 ** (attempt - 1)))
        return FetchResult(feed.id, False, [], 0, feed.max_retries, last_error)
