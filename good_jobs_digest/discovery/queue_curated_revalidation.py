#!/usr/bin/env python3
"""Queue existing curated_companies for website-first ATS revalidation."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from discovery.employer_candidates import DEFAULT_POOL_PATH, append_employer_candidates  # noqa: E402
from discovery.sources_curated import collect_curated_revalidate  # noqa: E402
from storage.bq_repository import JobBigQuery  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("queue_curated_revalidation")


def _connect_bq() -> JobBigQuery | None:
    if not settings.BQ_ENABLED:
        return None
    try:
        bq = JobBigQuery(settings)
        bq.ensure_tables()
        return bq
    except Exception as exc:  # noqa: BLE001
        logger.warning("BigQuery unavailable (%s) — using CSV fallback", exc)
        return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Queue curated_companies for website-first ATS revalidation.",
    )
    parser.add_argument(
        "--pool-path",
        type=Path,
        default=DEFAULT_POOL_PATH,
        help="employer_candidates.jsonl path",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        default=ROOT / "data" / "curated_revalidate_queued.json",
    )
    parser.add_argument(
        "--no-greenhouse-lookup",
        action="store_true",
        help="Skip Greenhouse API website lookup (faster; less website coverage)",
    )
    args = parser.parse_args()

    bq = _connect_bq()
    candidates = collect_curated_revalidate(
        settings,
        bq,
        resolve_greenhouse_websites=not args.no_greenhouse_lookup,
    )
    with_website = sum(1 for c in candidates if c.website.strip())
    added = append_employer_candidates(candidates, pool_path=args.pool_path)

    summary = {
        "total_registry_rows": len(candidates),
        "with_website": with_website,
        "new_in_pool": added,
        "discovery_source": "curated_revalidate",
        "run_command": (
            "python discovery/build_registry.py --sources curated_revalidate "
            "--skip-llm --progress-path data/build_registry_run_curated_revalidate.json"
        ),
    }
    args.summary_path.parent.mkdir(parents=True, exist_ok=True)
    args.summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    logger.info("Queued %s new / %s total (websites known: %s)", added, len(candidates), with_website)
    logger.info("Summary: %s", args.summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
