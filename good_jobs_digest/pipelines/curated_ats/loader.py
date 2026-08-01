"""Load employers from curated registry (BigQuery or CSV) for ATS polling."""

from __future__ import annotations

import logging

from config import Settings
from core.curated_registry import load_curated_records
from core.models import CompanyRow
from discovery.ats_registry import CURATED_ATS_TYPES, parse_ats_from_text
from storage.bq_repository import JobBigQuery

logger = logging.getLogger(__name__)


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
        url = (item.get("careers_url") or item.get("job_board_url") or "").strip()
        mission = (item.get("mission_category") or "mission").strip() or "mission"
        if not name or not url:
            skipped += 1
            continue
        ats_type = (item.get("ats_type") or "").strip().lower()
        ats_slug = (item.get("ats_slug") or "").strip()
        ats_region = (item.get("ats_region") or "global").strip() or "global"
        if not ats_type or not ats_slug:
            parsed = parse_ats_from_text(url)
            if not parsed:
                logger.debug("Skip unparseable ATS URL for %s: %s", name, url)
                skipped += 1
                continue
            ats_type, ats_slug = parsed
        if ats_type not in CURATED_ATS_TYPES:
            skipped += 1
            continue
        rows.append(
            CompanyRow(
                company_name=name,
                ats_type=ats_type,
                ats_slug=ats_slug,
                ats_region=ats_region,
                careers_url=url,
                mission_category=mission,
                last_validated_at=(item.get("last_validated_at") or "").strip() or None,
            )
        )
    if skipped:
        logger.info("Curated registry: skipped %s rows (missing or unsupported ATS)", skipped)
    return rows
