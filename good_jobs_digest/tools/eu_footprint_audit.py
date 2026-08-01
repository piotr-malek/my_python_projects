#!/usr/bin/env python3
"""Audit curated employers for EU/remote footprint; optionally prune + rewrite CSV."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.curated_registry import CURATED_CSV_FIELDS, load_curated_csv  # noqa: E402
from discovery.eu_footprint import evaluate_employer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("eu_footprint_audit")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        default=settings.CURATED_COMPANIES_PATH,
        help="Input curated_companies.csv",
    )
    parser.add_argument(
        "--out-report",
        type=Path,
        default=ROOT / "data" / "eu_footprint_report.json",
        help="JSON report path",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="If set, write pruned CSV keeping only EU/remote-ok employers",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max employers to audit (0 = all)")
    parser.add_argument("--delay-ms", type=int, default=200)
    args = parser.parse_args()

    rows = load_curated_csv(args.csv)
    if args.limit > 0:
        rows = rows[: args.limit]
    logger.info("Auditing %s employers from %s", len(rows), args.csv)

    verdicts = []
    keep: list[dict[str, str]] = []
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for i, row in enumerate(rows, 1):
            v = evaluate_employer(
                client,
                company_name=row.get("company_name") or "",
                ats_type=row.get("ats_type") or "",
                ats_slug=row.get("ats_slug") or "",
                ats_region=row.get("ats_region") or "global",
                job_board_url=row.get("job_board_url") or row.get("careers_url") or "",
            )
            payload = {
                "company_name": v.company_name,
                "ats_type": v.ats_type,
                "ats_slug": v.ats_slug,
                "ok": v.ok,
                "verdict": v.verdict,
                "has_eu_or_remote": v.has_eu_or_remote,
                "posting_count": v.posting_count,
                "eu_or_remote_count": v.eu_or_remote_count,
                "sample_locations": v.sample_locations,
                "reason": v.reason,
                "http_ok": v.http_ok,
                "job_board_url": row.get("job_board_url") or "",
                "discovery_source": row.get("discovery_source") or "",
            }
            verdicts.append(payload)
            if v.should_keep:
                keep.append(row)
            if i % 25 == 0 or i == len(rows):
                logger.info(
                    "Progress %s/%s — keep=%s demote=%s",
                    i,
                    len(rows),
                    sum(1 for x in verdicts if x["verdict"] != "demote"),
                    sum(1 for x in verdicts if x["verdict"] == "demote"),
                )
            time.sleep(max(0, args.delay_ms) / 1000.0)

    args.out_report.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "input": str(args.csv),
        "total": len(verdicts),
        "eu_or_remote_ok": sum(1 for v in verdicts if v["verdict"] == "eu_ok"),
        "undecided_kept": sum(1 for v in verdicts if v["verdict"] == "undecided"),
        "demoted": sum(1 for v in verdicts if v["verdict"] == "demote"),
        "fetch_errors": sum(1 for v in verdicts if not v["http_ok"]),
        "verdicts": verdicts,
    }
    args.out_report.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    logger.info(
        "Wrote report %s (ok=%s demoted=%s errors=%s)",
        args.out_report,
        report["eu_or_remote_ok"],
        report["demoted"],
        report["fetch_errors"],
    )

    if args.out_csv is not None:
        args.out_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.out_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(CURATED_CSV_FIELDS))
            w.writeheader()
            for row in keep:
                w.writerow({k: row.get(k, "") for k in CURATED_CSV_FIELDS})
        logger.info("Wrote pruned CSV %s (%s rows)", args.out_csv, len(keep))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
