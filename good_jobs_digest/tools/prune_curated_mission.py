#!/usr/bin/env python3
"""Re-score curated_companies and auto-remove employers below the mission score threshold."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from discovery.mission_filter import EmployerMissionFilter  # noqa: E402
from storage.bq_repository import JobBigQuery  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("prune_curated_mission")

AUDIT_FIELDS = [
    "company_name",
    "job_board_url",
    "discovery_source",
    "mission_category",
    "mission_score",
    "purpose_driven",
    "mission_type",
    "llm_reason",
    "approved",
]

DEFAULT_PROGRESS = ROOT / "data" / "build_registry_progress.json"


def _load_from_bq(*, limit: int | None) -> list[dict[str, str]]:
    if not settings.BQ_ENABLED:
        raise RuntimeError("BQ_ENABLED is false — set BQ_ENABLED=true in .env")
    bq = JobBigQuery(settings)
    bq.ensure_tables()
    rows = bq.fetch_curated_companies(limit=limit)
    return [
        {
            "company_name": r["company_name"],
            "job_board_url": r["job_board_url"],
            "discovery_source": "",
            "mission_category": "",
        }
        for r in rows
    ]


def _load_from_progress(path: Path, *, limit: int | None) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Progress file not found: {path}")
    state = json.loads(path.read_text(encoding="utf-8"))
    matched = state.get("ats_matched") or []
    rows: list[dict[str, str]] = []
    for item in matched:
        if not isinstance(item, dict):
            continue
        name = str(item.get("company_name") or "").strip()
        url = str(item.get("job_board_url") or "").strip()
        if not name or not url:
            continue
        rows.append(
            {
                "company_name": name,
                "job_board_url": url,
                "discovery_source": str(item.get("discovery_source") or ""),
                "mission_category": str(item.get("mission_category") or ""),
            }
        )
    if limit is not None and limit > 0:
        rows = rows[:limit]
    return rows


def _partition(
    rows: list[dict[str, str]],
    *,
    min_score: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    approved: list[dict[str, str]] = []
    rejected: list[dict[str, str]] = []
    for row in rows:
        try:
            score = int(row.get("mission_score") or 0)
        except ValueError:
            score = 0
        if score >= min_score:
            approved.append(row)
        else:
            rejected.append(row)
    return approved, rejected


def _write_audit_csv(path: Path, rows: list[dict[str, str]], *, min_score: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=AUDIT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            try:
                score = int(row.get("mission_score") or 0)
            except ValueError:
                score = 0
            writer.writerow(
                {
                    "company_name": row.get("company_name") or "",
                    "job_board_url": row.get("job_board_url") or "",
                    "discovery_source": row.get("discovery_source") or "",
                    "mission_category": row.get("mission_category") or "",
                    "mission_score": score,
                    "purpose_driven": row.get("purpose_driven") or "",
                    "mission_type": row.get("mission_type") or "",
                    "llm_reason": row.get("mission_llm_reason") or "",
                    "approved": "yes" if score >= min_score else "no",
                }
            )


def _print_summary(
    approved: list[dict[str, str]],
    rejected: list[dict[str, str]],
    *,
    min_score: int,
) -> None:
    total = len(approved) + len(rejected)
    if not total:
        print("No rows scored.")
        return

    all_scores = []
    for row in approved + rejected:
        try:
            all_scores.append(int(row.get("mission_score") or 0))
        except ValueError:
            all_scores.append(0)

    print(f"\n=== Mission auto-approve (score ≥{min_score}) ===")
    print(f"  Total:    {total}")
    print(f"  Approved: {len(approved)}")
    print(f"  Rejected: {len(rejected)}")
    if all_scores:
        print(f"  Avg score: {sum(all_scores) / len(all_scores):.1f}")

    if rejected:
        print(f"\n--- Rejected ({len(rejected)}) ---")
        for row in sorted(rejected, key=lambda r: int(r.get("mission_score") or 0))[:20]:
            print(
                f"  {row.get('mission_score'):>3}  {row.get('company_name', '')[:45]:45}  "
                f"{row.get('mission_llm_reason') or ''}"
            )
        if len(rejected) > 20:
            print(f"  ... and {len(rejected) - 20} more")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Re-score curated employers with Ollama; auto-remove those below MISSION_APPROVE_MIN_SCORE.",
    )
    parser.add_argument(
        "--source",
        choices=("bq", "progress"),
        default="bq",
        help="Load from BigQuery curated_companies (default) or build_registry progress file",
    )
    parser.add_argument(
        "--progress-path",
        type=Path,
        default=DEFAULT_PROGRESS,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional audit CSV (default: data/mission_audit_YYYY-MM-DD.csv)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Cap employers scored (0 = all)")
    parser.add_argument(
        "--min-score",
        type=int,
        default=None,
        help=f"Approval threshold (default: MISSION_APPROVE_MIN_SCORE={settings.MISSION_APPROVE_MIN_SCORE})",
    )
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Delete rejected rows from curated_companies (default: dry-run only)",
    )
    parser.add_argument("--no-audit-csv", action="store_true", help="Skip writing audit CSV")
    args = parser.parse_args()

    min_score = settings.MISSION_APPROVE_MIN_SCORE if args.min_score is None else args.min_score
    limit = args.limit if args.limit > 0 else None

    if args.source == "bq":
        employers = _load_from_bq(limit=limit)
    else:
        employers = _load_from_progress(args.progress_path, limit=limit)

    logger.info("Loaded %s employers from %s", len(employers), args.source)
    if not employers:
        print("No employers to score.")
        return 1

    scorer = EmployerMissionFilter(settings)
    scored = scorer.score_employers(employers)
    if len(scored) < len(employers):
        logger.warning("Only %s/%s employers scored successfully", len(scored), len(employers))

    approved, rejected = _partition(scored, min_score=min_score)
    _print_summary(approved, rejected, min_score=min_score)

    if not args.no_audit_csv:
        out_path = args.output or (ROOT / "data" / f"mission_audit_{date.today().isoformat()}.csv")
        _write_audit_csv(out_path, scored, min_score=min_score)
        print(f"\nWrote audit CSV: {out_path}")

    if args.source != "bq":
        print("\nSource is progress file — no BigQuery prune (re-run discovery to insert approved rows).")
        return 0

    if not rejected:
        print("\nNothing to prune.")
        return 0

    urls = [r["job_board_url"] for r in rejected if r.get("job_board_url")]
    if not args.prune:
        print(f"\nDry run — would remove {len(urls)} row(s). Re-run with --prune to apply.")
        return 0

    if not settings.BQ_ENABLED:
        raise RuntimeError("BQ_ENABLED is false — cannot prune curated_companies")

    bq = JobBigQuery(settings)
    bq.ensure_tables()
    deleted = bq.delete_curated_companies(urls)
    print(f"\nPruned {deleted} row(s) from curated_companies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
