#!/usr/bin/env python3
"""Probe mined employer candidates and promote matches to curated_companies."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from discovery.employer_candidates import (  # noqa: E402
    DEFAULT_POOL_PATH,
    load_unprobed_candidates,
    mark_probed,
)
from discovery.ats_registry import set_bulk_probe  # noqa: E402
from discovery.mission_filter import EmployerMissionFilter  # noqa: E402
from discovery.resolve import careers_url, resolve_candidate  # noqa: E402
from discovery.validate import validate_registry_entry  # noqa: E402
from storage.bq_repository import JobBigQuery  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("discover_candidates")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_discover_candidates(
    *,
    pool_path: Path = DEFAULT_POOL_PATH,
    limit: int = 100,
    delay_ms: int = 300,
    dry_run: bool = False,
    skip_llm: bool = False,
    bulk: bool = False,
) -> int:
    if bulk:
        set_bulk_probe(True)
        delay_ms = 0
    candidates = load_unprobed_candidates(pool_path, limit=limit)
    if not candidates:
        logger.info("No unprobed candidates in %s", pool_path)
        return 0

    matched: list[dict[str, str]] = []
    probed_names: list[str] = []

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for cand in candidates:
            probed_names.append(cand.company_name)
            match = resolve_candidate(
                client,
                cand,
                max_slug_attempts=3 if bulk else 6,
                website_first=not bulk,
                batch_ats=bulk,
            )
            if not match:
                time.sleep(max(0, delay_ms) / 1000.0)
                continue
            if not bulk:
                verified = validate_registry_entry(
                    client,
                    company_name=cand.company_name,
                    ats_type=match.ats_type,
                    ats_slug=match.ats_slug,
                    ats_region=match.ats_region,
                )
                if not verified.ok:
                    time.sleep(max(0, delay_ms) / 1000.0)
                    continue
            board_url = match.careers_url or careers_url(match.ats_type, match.ats_slug)
            matched.append(
                {
                    "company_name": cand.company_name,
                    "job_board_url": board_url,
                    "careers_url": board_url,
                    "mission_category": cand.mission_category or "mission",
                    "discovery_source": f"{cand.discovery_source}_mining",
                    "ats_type": match.ats_type,
                    "ats_slug": match.ats_slug,
                    "ats_region": match.ats_region,
                }
            )
            logger.info("MINED_MATCH %s → %s", cand.company_name, board_url)
            time.sleep(max(0, delay_ms) / 1000.0)

    mark_probed(probed_names, pool_path)

    if not matched:
        logger.info("No ATS matches from mined pool")
        return 0
    if dry_run:
        logger.info("Dry run — would insert %s rows", len(matched))
        return 0

    if settings.REGISTRY_LLM_FILTER and not skip_llm:
        mission_filter = EmployerMissionFilter(settings)
        matched = mission_filter.filter_employers(matched)

    if not settings.BQ_ENABLED:
        logger.error("BQ_ENABLED is false — cannot insert curated_companies")
        return 1
    bq = JobBigQuery(settings)
    bq.ensure_tables()
    inserted = bq.insert_curated_companies(matched, added_at=_utc_now_iso())
    logger.info("Inserted %s mined employers into curated_companies", inserted)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe employer_candidates pool → curated_companies")
    parser.add_argument("--pool-path", type=Path, default=DEFAULT_POOL_PATH)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--delay-ms", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--bulk", action="store_true", help="Fast full-ATS probing (batched waves)")
    args = parser.parse_args()
    return run_discover_candidates(
        pool_path=args.pool_path,
        limit=args.limit,
        delay_ms=args.delay_ms,
        dry_run=args.dry_run,
        skip_llm=args.skip_llm,
        bulk=args.bulk,
    )


if __name__ == "__main__":
    raise SystemExit(main())
