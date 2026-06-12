"""Load curated employer registry from BigQuery (preferred) or shipped CSV."""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from config import Settings
from discovery.resolve import parse_ats_from_text
from storage.bq_repository import JobBigQuery

logger = logging.getLogger(__name__)

CURATED_CSV_FIELDS = ("company_name", "job_board_url", "mission_category", "discovery_source")


def load_curated_csv(path: Path, *, limit: int | None = None) -> list[dict[str, str]]:
    """Read registry/curated_companies.csv (or custom path)."""
    if not path.is_file():
        logger.warning("Curated CSV missing: %s", path)
        return []
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        for raw in csv.DictReader(f):
            name = (raw.get("company_name") or "").strip()
            url = (raw.get("job_board_url") or "").strip()
            if not name or not url:
                continue
            rows.append(
                {
                    "company_name": name,
                    "job_board_url": url,
                    "mission_category": (raw.get("mission_category") or "mission").strip(),
                    "discovery_source": (raw.get("discovery_source") or "csv").strip(),
                }
            )
            if limit is not None and limit > 0 and len(rows) >= limit:
                break
    logger.info("Curated registry: %s companies from %s", len(rows), path.name)
    return rows


def load_curated_records(
    settings: Settings,
    bq: JobBigQuery | None = None,
    *,
    limit: int | None = None,
) -> list[dict[str, str]]:
    """BigQuery curated_companies when populated; otherwise registry CSV."""
    if bq is not None:
        try:
            rows = bq.fetch_curated_companies(limit=limit)
            if rows:
                logger.info("Curated registry: %s companies from BigQuery", len(rows))
                return rows
            logger.info("BigQuery curated_companies is empty — using CSV fallback")
        except Exception as exc:  # noqa: BLE001
            logger.warning("BigQuery curated_companies unavailable (%s) — using CSV fallback", exc)
    return load_curated_csv(settings.CURATED_COMPANIES_PATH, limit=limit)


def load_curated_board_keys(
    settings: Settings,
    bq: JobBigQuery | None = None,
    *,
    limit: int | None = None,
    allowed: frozenset[str] | None = None,
) -> set[tuple[str, str]]:
    """Return (ats_type, ats_slug) pairs from the active curated registry."""
    allowed = allowed or frozenset({"greenhouse", "lever", "smartrecruiters"})
    keys: set[tuple[str, str]] = set()
    for item in load_curated_records(settings, bq, limit=limit):
        url = (item.get("job_board_url") or "").strip()
        if not url:
            continue
        parsed = parse_ats_from_text(url)
        if not parsed:
            continue
        ats_type, ats_slug = parsed
        if ats_type in allowed:
            keys.add((ats_type.lower(), ats_slug.lower()))
    return keys
