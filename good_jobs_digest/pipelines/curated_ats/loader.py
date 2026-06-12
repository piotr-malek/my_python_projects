"""Load employers from curated registry (BigQuery or CSV) for ATS polling."""

from __future__ import annotations

import logging

from config import Settings
from core.curated_registry import load_curated_records
from core.models import CompanyRow
from discovery.resolve import parse_ats_from_text
from storage.bq_repository import JobBigQuery

logger = logging.getLogger(__name__)

_ALLOWED = frozenset({"greenhouse", "lever", "smartrecruiters"})


def load_curated_companies(
    settings: Settings,
    bq: JobBigQuery | None = None,
    *,
    limit: int | None = None,
) -> list[CompanyRow]:
    """Parse curated registry rows into ATS ingest rows."""
    rows: list[CompanyRow] = []
    skipped = 0
    for item in load_curated_records(settings, bq, limit=limit):
        name = (item.get("company_name") or "").strip()
        url = (item.get("job_board_url") or "").strip()
        mission = (item.get("mission_category") or "mission").strip() or "mission"
        if not name or not url:
            skipped += 1
            continue
        parsed = parse_ats_from_text(url)
        if not parsed:
            logger.debug("Skip unparseable ATS URL for %s: %s", name, url)
            skipped += 1
            continue
        ats_type, ats_slug = parsed
        if ats_type not in _ALLOWED:
            skipped += 1
            continue
        rows.append(
            CompanyRow(
                company_name=name,
                ats_type=ats_type,
                ats_slug=ats_slug,
                careers_url=url,
                mission_category=mission,
            )
        )
    if skipped:
        logger.info("Curated registry: skipped %s rows (missing or unparseable URL)", skipped)
    return rows
