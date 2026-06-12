#!/usr/bin/env python3
"""Probe job-board sources and print available fields (read-only)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from pipelines.job_boards.sources import (
    fetch_80000hours,
    fetch_climatebase,
    fetch_climatebase_detail,
    fetch_escapethecity,
    fetch_reliefweb,
    fetch_techjobsforgood,
)
from pipelines.job_boards.sources.types import JobBoardFetchResult

PROBES = {
    "80000hours": fetch_80000hours,
    "climatebase": fetch_climatebase,
    "escapethecity": fetch_escapethecity,
    "techjobsforgood": fetch_techjobsforgood,
    "reliefweb": fetch_reliefweb,
}


def _print_result(result: JobBoardFetchResult, *, verbose: bool) -> None:
    print(result.summary_line())
    if verbose and result.sample_job:
        print(json.dumps(result.sample_job, indent=2, default=str)[:4000])
        if len(json.dumps(result.sample_job, default=str)) > 4000:
            print("… (sample truncated)")
    print()


def main() -> int:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Test fetching from mission job boards.")
    parser.add_argument(
        "--source",
        choices=[*PROBES.keys(), "all"],
        default="all",
        help="Which source to probe (default: all)",
    )
    parser.add_argument("--limit", type=int, default=3, help="Max jobs per source sample")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print sample job JSON")
    parser.add_argument(
        "--climatebase-detail",
        action="store_true",
        help="Also fetch one Climatebase job detail page",
    )
    parser.add_argument(
        "--tjfg-wayback",
        action="store_true",
        help="If live Tech Jobs for Good is blocked, use an Internet Archive snapshot",
    )
    args = parser.parse_args()

    names = list(PROBES.keys()) if args.source == "all" else [args.source]
    failures = 0

    for name in names:
        fn = PROBES[name]
        kwargs: dict = {"limit": args.limit}
        if name == "techjobsforgood":
            kwargs["allow_wayback_fallback"] = args.tjfg_wayback
        result = fn(**kwargs)
        _print_result(result, verbose=args.verbose)
        if not result.ok:
            failures += 1

    if args.climatebase_detail or (args.source in ("all", "climatebase") and failures == 0):
        base = fetch_climatebase(limit=1)
        if base.ok and base.sample_job.get("id"):
            try:
                detail = fetch_climatebase_detail(base.sample_job["id"])
                print("[OK] climatebase detail fields:", ", ".join(sorted(detail.keys())))
                if args.verbose:
                    preview = {k: detail[k] for k in detail if k != "raw"}
                    print(json.dumps(preview, indent=2, default=str)[:4000])
            except Exception as exc:  # noqa: BLE001
                print(f"[FAIL] climatebase detail: {exc}")
                failures += 1
            print()

    if failures:
        print(f"{failures} source(s) failed.")
        return 1
    print("All probed sources returned jobs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
