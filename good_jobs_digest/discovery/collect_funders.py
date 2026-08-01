#!/usr/bin/env python3
"""Phase 4: scrape funder portfolio org lists into employer_candidates pool."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from discovery.employer_candidates import DEFAULT_POOL_PATH, append_employer_candidates  # noqa: E402
from discovery.sources_funders import collect_funder_sources  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("collect_funders")

FUNDER_SOURCES = ("openphil", "echoing_green", "ashoka_fellows", "fast_forward")


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect funder portfolio org names (Phase 4).")
    parser.add_argument(
        "--sources",
        default=",".join(FUNDER_SOURCES),
        help="Comma-separated funder sources",
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
        default=ROOT / "data" / "funder_candidates_collected.json",
        help="Write collection summary JSON",
    )
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    logger.info("Collecting funder portfolios: %s", ", ".join(sources))
    candidates = collect_funder_sources(sources)
    logger.info("Collected %s unique org names", len(candidates))

    by_source: dict[str, int] = {}
    for cand in candidates:
        src = cand.discovery_source or "unknown"
        by_source[src] = by_source.get(src, 0) + 1

    added = append_employer_candidates(candidates, pool_path=args.pool_path)
    summary = {
        "sources": sources,
        "total_collected": len(candidates),
        "new_in_pool": added,
        "by_source": by_source,
        "sample": [c.company_name for c in candidates[:20]],
    }
    args.summary_path.parent.mkdir(parents=True, exist_ok=True)
    args.summary_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    logger.info("Summary written to %s", args.summary_path)
    for src, count in sorted(by_source.items()):
        logger.info("  %s: %s", src, count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
