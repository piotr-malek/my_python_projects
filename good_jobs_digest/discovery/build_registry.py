#!/usr/bin/env python3
"""Discover employers with public ATS boards; LLM-filter; save to BigQuery curated_companies."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from discovery.validate import validate_registry_entry  # noqa: E402
from discovery.resolve import careers_url, resolve_candidate  # noqa: E402
from discovery.sources import DEFAULT_SEEDS_PATH, collect_all  # noqa: E402
from discovery.mission_filter import EmployerMissionFilter  # noqa: E402
from storage.bq_repository import JobBigQuery  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_registry")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _candidate_key(company_name: str) -> str:
    return company_name.strip().lower()


def _progress_config_signature(args: argparse.Namespace) -> dict[str, str | int | bool]:
    return {
        "sources": args.sources,
        "seeds_path": str(args.seeds_path),
        "climatebase_max": args.climatebase_max,
        "no_climatebase_details": bool(args.no_climatebase_details),
        "eighty_k_max_pages": args.eighty_k_max_pages,
        "etc_max_pages": args.etc_max_pages,
        "bcorp_max_pages": args.bcorp_max_pages,
        "bcorp_per_page": args.bcorp_per_page,
        "bcorp_rps": args.bcorp_rps,
        "try_eu_lever": bool(args.try_eu_lever),
        "target": int(args.target),
    }


def _load_progress(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.warning("Progress file exists but is unreadable: %s", path)
        return {}


def _config_compatible(saved: object, expected: dict[str, str | int | bool]) -> bool:
    if not isinstance(saved, dict):
        return False
    for key, value in expected.items():
        if saved.get(key) != value:
            return False
    return True


def _save_progress(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _connect_bq() -> JobBigQuery:
    if not settings.BQ_ENABLED:
        raise RuntimeError("BQ_ENABLED is false — set BQ_ENABLED=true in .env")
    bq = JobBigQuery(settings)
    bq.ensure_tables()
    return bq


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Discover ATS job boards for mission employers → Ollama filter → BigQuery.",
    )
    parser.add_argument(
        "--sources",
        default="bcorp",
        help="Comma-separated: 80000hours, escapethecity, climatebase, seeds, bcorp (default: bcorp)",
    )
    parser.add_argument("--seeds-path", type=Path, default=DEFAULT_SEEDS_PATH)
    parser.add_argument("--min-rows", type=int, default=50, help="Minimum rows to insert into BQ")
    parser.add_argument(
        "--target",
        type=int,
        default=0,
        help="Stop ATS probing after N matches (0 = probe all candidates)",
    )
    parser.add_argument("--delay-ms", type=int, default=350, help="Delay between employer probes")
    parser.add_argument("--climatebase-max", type=int, default=100)
    parser.add_argument("--no-climatebase-details", action="store_true")
    parser.add_argument("--80k-max-pages", type=int, default=50, dest="eighty_k_max_pages")
    parser.add_argument("--etc-max-pages", type=int, default=12, dest="etc_max_pages")
    parser.add_argument("--bcorp-max-pages", type=int, default=0, help="Max B Corp pages (0 = all)")
    parser.add_argument("--bcorp-per-page", type=int, default=250, help="B Corp per_page (max 250)")
    parser.add_argument("--bcorp-rps", type=float, default=2.0, help="B Corp request rate")
    parser.add_argument("--bcorp-reset-checkpoint", action="store_true", help="Reset B Corp crawl checkpoint")
    parser.add_argument("--try-eu-lever", action="store_true", help="Also try api.eu.lever.co")
    parser.add_argument(
        "--progress-path",
        type=Path,
        default=ROOT / "data" / "build_registry_progress.json",
        help="Path to progress checkpoint file.",
    )
    parser.add_argument(
        "--resume-progress",
        action="store_true",
        default=True,
        help="Resume from progress-path when compatible (default: true).",
    )
    parser.add_argument(
        "--no-resume-progress",
        action="store_false",
        dest="resume_progress",
        help="Ignore existing progress file and start from scratch.",
    )
    parser.add_argument(
        "--reset-progress",
        action="store_true",
        help="Delete existing progress file before run starts.",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip Ollama purpose filter (insert all ATS-validated rows; not recommended)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover + validate only; do not call Ollama or write BigQuery",
    )
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    logger.info("Collecting candidates from: %s", ", ".join(sources))
    candidates = collect_all(
        sources=sources,
        climatebase_max_listings=args.climatebase_max,
        climatebase_fetch_details=not args.no_climatebase_details,
        eighty_k_max_pages=args.eighty_k_max_pages,
        escapethecity_max_pages=args.etc_max_pages,
        seeds_path=args.seeds_path,
        bcorp_max_pages=args.bcorp_max_pages,
        bcorp_per_page=args.bcorp_per_page,
        bcorp_requests_per_second=args.bcorp_rps,
        bcorp_reset_checkpoint=args.bcorp_reset_checkpoint,
    )
    logger.info("Unique employer candidates: %s", len(candidates))

    if args.reset_progress and args.progress_path.exists():
        args.progress_path.unlink()
        logger.info("Deleted old progress file: %s", args.progress_path)

    config_sig = _progress_config_signature(args)
    state = _load_progress(args.progress_path) if args.resume_progress else {}
    processed_candidates: set[str] = set()
    ats_matched: list[dict[str, str]] = []
    seen_ats: set[str] = set()
    probed = 0

    if state and _config_compatible(state.get("config"), config_sig):
        for name in state.get("processed_candidates", []):
            if isinstance(name, str):
                processed_candidates.add(name)
        for row in state.get("ats_matched", []):
            if not isinstance(row, dict):
                continue
            company_name = str(row.get("company_name") or "").strip()
            job_board_url = str(row.get("job_board_url") or "").strip()
            ats_key = str(row.get("ats_key") or "").strip().lower()
            if not company_name or not job_board_url or not ats_key:
                continue
            seen_ats.add(ats_key)
            ats_matched.append(
                {
                    "company_name": company_name,
                    "job_board_url": job_board_url,
                    "mission_category": str(row.get("mission_category") or "mission"),
                    "discovery_source": str(row.get("discovery_source") or ""),
                    "ats_key": ats_key,
                }
            )
        logger.info(
            "Resumed progress: processed=%s, ats_matched=%s (%s)",
            len(processed_candidates),
            len(ats_matched),
            args.progress_path,
        )
    elif state:
        logger.info("Progress file ignored due to config mismatch: %s", args.progress_path)

    existing_bq_companies: set[str] = set()
    try:
        bq_for_dedupe = _connect_bq()
        existing_bq_companies = bq_for_dedupe.fetch_curated_company_names()
        logger.info("Loaded %s existing curated companies for name dedupe", len(existing_bq_companies))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Skipping BQ company-name dedupe (%s)", exc)

    def checkpoint(force: bool = False) -> None:
        if not force and probed % 10 != 0:
            return
        payload = {
            "updated_at": _utc_now_iso(),
            "config": config_sig,
            "processed_candidates": sorted(processed_candidates),
            "ats_matched": ats_matched,
            "probed_count": probed,
        }
        _save_progress(args.progress_path, payload)

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for cand in candidates:
            if args.target and len(ats_matched) >= args.target:
                logger.info("Reached ATS target=%s — stopping probe loop", args.target)
                break
            ckey = _candidate_key(cand.company_name)
            if ckey in processed_candidates:
                continue
            if ckey in existing_bq_companies:
                processed_candidates.add(ckey)
                checkpoint()
                continue
            probed += 1
            try:
                match = resolve_candidate(client, cand, try_eu_lever=args.try_eu_lever)
            except httpx.RequestError as exc:
                logger.warning("ATS probe request error for %s: %s", cand.company_name, exc)
                processed_candidates.add(ckey)
                checkpoint()
                time.sleep(max(0, args.delay_ms) / 1000.0)
                continue
            if not match:
                if probed % 25 == 0:
                    logger.info("Probed %s/%s — ATS matched %s", probed, len(candidates), len(ats_matched))
                processed_candidates.add(ckey)
                checkpoint()
                time.sleep(max(0, args.delay_ms) / 1000.0)
                continue

            key = f"{match.ats_type}:{match.ats_slug.lower()}"
            if key in seen_ats:
                processed_candidates.add(ckey)
                checkpoint()
                time.sleep(max(0, args.delay_ms) / 1000.0)
                continue

            verified = validate_registry_entry(
                client,
                company_name=cand.company_name,
                ats_type=match.ats_type,
                ats_slug=match.ats_slug,
                ats_region=match.ats_region,
                require_identity=True,
            )
            if not verified.ok:
                logger.warning(
                    "ATS_REJECT %s → %s:%s (%s)",
                    cand.company_name,
                    match.ats_type,
                    match.ats_slug,
                    verified.reason,
                )
                processed_candidates.add(ckey)
                checkpoint()
                time.sleep(max(0, args.delay_ms) / 1000.0)
                continue

            seen_ats.add(key)
            board_url = match.careers_url or careers_url(match.ats_type, match.ats_slug)
            ats_matched.append(
                {
                    "company_name": cand.company_name,
                    "job_board_url": board_url,
                    "mission_category": cand.mission_category or "mission",
                    "discovery_source": cand.discovery_source or "",
                    "ats_key": key,
                }
            )
            logger.info(
                "ATS_MATCH %s → %s (%s)",
                cand.company_name,
                board_url,
                len(ats_matched),
            )
            processed_candidates.add(ckey)
            checkpoint(force=True)
            time.sleep(max(0, args.delay_ms) / 1000.0)

    print("\n=== ATS discovery summary ===")
    print(f"  Candidates:   {len(candidates)}")
    print(f"  Probed:         {probed}")
    print(f"  ATS validated:  {len(ats_matched)}")

    if args.dry_run:
        print("Dry run — skipping Ollama filter and BigQuery insert.")
        return 0

    use_llm = settings.REGISTRY_LLM_FILTER and not args.skip_llm
    if use_llm:
        mission_filter = EmployerMissionFilter(settings)
        approved = mission_filter.filter_employers(ats_matched)
    else:
        approved = ats_matched
        logger.warning("LLM purpose filter disabled — inserting all ATS-validated employers")

    print(
        f"\n=== Mission LLM auto-approve (score ≥{settings.MISSION_APPROVE_MIN_SCORE}) ===\n"
        f"  Approved: {len(approved)} / {len(ats_matched)}"
    )

    if len(approved) < args.min_rows:
        logger.error("Only %s approved rows (min-rows=%s)", len(approved), args.min_rows)
        if not approved:
            return 1

    try:
        bq = _connect_bq()
    except Exception as exc:
        logger.error("BigQuery unavailable: %s", exc)
        return 1

    to_insert = [
        {k: v for k, v in row.items() if k in {"company_name", "job_board_url", "mission_category", "discovery_source"}}
        for row in approved
    ]
    inserted = bq.insert_curated_companies(to_insert, added_at=_utc_now_iso())
    table = bq.table_id("curated_companies")
    checkpoint(force=True)
    print(f"\nInserted {inserted} new row(s) into `{table}`")
    return 0 if len(approved) >= args.min_rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
