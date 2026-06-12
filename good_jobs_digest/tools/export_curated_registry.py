#!/usr/bin/env python3
"""Export curated employers to registry/curated_companies.csv."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings
from core.curated_registry import CURATED_CSV_FIELDS, load_curated_records
from storage.bq_repository import JobBigQuery

DEFAULT_OUT = ROOT / "registry" / "curated_companies.csv"


def _from_progress(paths: list[Path]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for path in paths:
        if not path.is_file():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data.get("ats_matched") or []:
            url = (row.get("job_board_url") or "").strip()
            name = (row.get("company_name") or "").strip()
            if not url or not name:
                continue
            merged[url.lower()] = {
                "company_name": name,
                "job_board_url": url,
                "mission_category": (row.get("mission_category") or "mission").strip(),
                "discovery_source": (row.get("discovery_source") or "discovery").strip(),
            }
    return sorted(merged.values(), key=lambda r: r["company_name"].lower())


def _write(rows: list[dict[str, str]], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(CURATED_CSV_FIELDS))
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in CURATED_CSV_FIELDS})
    print(f"Wrote {len(rows)} rows to {out}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export curated employers to CSV")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--from-progress",
        action="store_true",
        help="Merge data/build_registry_progress*.json (local discovery runs)",
    )
    args = parser.parse_args()

    if args.from_progress:
        rows = _from_progress(
            [
                ROOT / "data" / "build_registry_progress.json",
                ROOT / "data" / "build_registry_progress_v2.json",
            ]
        )
        _write(rows, args.out)
        return 0

    bq = None
    if settings.BQ_ENABLED:
        try:
            bq = JobBigQuery(settings)
            bq.ensure_tables()
        except Exception as exc:  # noqa: BLE001
            print(f"BigQuery unavailable ({exc}) — exporting CSV fallback only", file=sys.stderr)
    rows = load_curated_records(settings, bq)
    _write(rows, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
