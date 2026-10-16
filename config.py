"""Configuration loading. Everything is relative to the project folder, never absolute."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DEFAULT_FEEDS = ROOT / "sources" / "feeds.json"
DEFAULT_DB = ROOT / "db" / "pipeline.db"
DEFAULT_FIXTURES = ROOT / "fixtures"
DEFAULT_OUTPUT = ROOT / "output"


@dataclass(frozen=True)
class Feed:
    id: str
    name: str
    fixture: str
    url: str | None
    id_field: str
    max_retries: int


@dataclass(frozen=True)
class PipelineConfig:
    feeds: list[Feed]
    backoff_seconds: float
    invalid_row_tolerance: float


def load_config(path: Path | str = DEFAULT_FEEDS) -> PipelineConfig:
    path = Path(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    feeds = [
        Feed(
            id=item["id"],
            name=item.get("name", item["id"]),
            fixture=item["fixture"],
            url=item.get("url"),
            id_field=item.get("id_field", "external_id"),
            max_retries=int(item.get("max_retries", raw.get("default_max_retries", 3))),
        )
        for item in raw["feeds"]
        if item.get("enabled", True)
    ]
    return PipelineConfig(
        feeds=feeds,
        backoff_seconds=float(raw.get("backoff_seconds", 0.2)),
        invalid_row_tolerance=float(raw.get("invalid_row_tolerance", 0.34)),
    )
