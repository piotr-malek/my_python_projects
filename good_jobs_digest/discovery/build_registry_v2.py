#!/usr/bin/env python3
"""Mission-v2 org discovery → ATS probe → LLM filter → BigQuery curated_companies."""

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
from discovery.mission_filter import EmployerMissionFilter  # noqa: E402
from discovery.resolve import careers_url, resolve_candidate  # noqa: E402
from discovery.sources_mission_v2 import (  # noqa: E402
    MISSION_V2_CANDIDATES_PATH,
    MISSION_V2_SCRAPE_CHECKPOINT_PATH,
    collect_mission_v2,
    load_candidates_v2,
    reset_scrape_checkpoint,
)
from discovery.validate import validate_registry_entry  # noqa: E402
from storage.bq_repository import JobBigQuery  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_registry_v2")

MISSION_V2_PROGRESS_PATH = ROOT / "data" / "build_registry_progress_v2.json"
DEFAULT_V2_SOURCES = "coefficient,sff,gwwc,ace,givewell,gates,ea_funds"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _candidate_key(company_name: str) -> str:
    return company_name.strip().lower()


def _progress_config_signature(args: argparse.Namespace) -> dict[str, str | int | bool]:
    return {
        "pipeline": "mission_v2",
        "sources": args.sources,
        "candidates_path": str(args.candidates_path),
        "try_eu_lever": bool(args.try_eu_lever),
        "target": int(args.target),
        "skip_scrape": bool(args.skip_scrape),
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
        description="Mission-v2 grant/evaluator org discovery → ATS → BigQuery.",
    )
    parser.add_argument(
        "--sources",
        default=DEFAULT_V2_SOURCES,
        help=f"Comma-separated mission-v2 sources (default: {DEFAULT_V2_SOURCES})",
    )
    parser.add_argument(
        "--candidates-path",
        type=Path,
        default=MISSION_V2_CANDIDATES_PATH,
        help="JSONL path for scraped org candidates",
    )
    parser.add_argument(
        "--progress-path",
        type=Path,
        default=MISSION_V2_PROGRESS_PATH,
        help="ATS probe checkpoint file",
    )
    parser.add_argument(
        "--scrape-checkpoint-path",
        type=Path,
        default=MISSION_V2_SCRAPE_CHECKPOINT_PATH,
        help="Org scrape checkpoint (per-source / per-URL resume)",
    )
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Skip scraping; load candidates from existing JSONL only",
    )
    parser.add_argument(
        "--force-rescrape",
        action="store_true",
        help="Ignore scrape checkpoint and re-fetch all sources (still append-merge JSONL)",
    )
    parser.add_argument(
        "--reset-scrape-checkpoint",
        action="store_true",
        help="Delete scrape checkpoint before run (keeps existing JSONL)",
    )
    parser.add_argument("--min-rows", type=int, default=1, help="Minimum approved rows for success exit")
    parser.add_argument(
        "--target",
        type=int,
        default=0,
        help="Stop ATS probing after N matches (0 = probe all candidates)",
    )
    parser.add_argument("--delay-ms", type=int, default=200, help="Delay between employer probes")
    parser.add_argument(
        "--max-slug-attempts",
        type=int,
        default=6,
        help="Max ATS slug variants to probe per employer (0 = unlimited)",
    )
    parser.add_argument("--try-eu-lever", action="store_true", help="Also try api.eu.lever.co")
    parser.add_argument(
        "--resume-progress",
        action="store_true",
        default=True,
        help="Resume ATS probe checkpoint when compatible (default: true)",
    )
    parser.add_argument(
        "--no-resume-progress",
        action="store_false",
        dest="resume_progress",
        help="Ignore existing ATS probe progress",
    )
    parser.add_argument("--reset-progress", action="store_true", help="Delete ATS probe progress before run")
    parser.add_argument("--skip-llm", action="store_true", help="Skip Ollama purpose filter")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover + validate only; no Ollama or BigQuery writes",
    )
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    if args.reset_scrape_checkpoint:
        reset_scrape_checkpoint(args.scrape_checkpoint_path)

    if args.skip_scrape:
        candidates = load_candidates_v2(args.candidates_path)
        logger.info(
            "Skipped scrape — loaded %s candidates from %s",
            len(candidates),
            args.candidates_path.name,
        )
    else:
        logger.info(
            "Incremental scrape (resume-safe): %s%s",
            ", ".join(sources),
            " [force-rescrape]" if args.force_rescrape else "",
        )
        candidates = collect_mission_v2(
            sources=sources,
            candidates_path=args.candidates_path,
            scrape_checkpoint_path=args.scrape_checkpoint_path,
            incremental=True,
            force_rescrape=args.force_rescrape,
        )
    logger.info("Unique employer candidates: %s (from %s)", len(candidates), args.candidates_path.name)

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
            "candidates_total": len(candidates),
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
                match = resolve_candidate(
                    client,
                    cand,
                    try_eu_lever=args.try_eu_lever,
                    max_slug_attempts=args.max_slug_attempts,
                )
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

    print("\n=== Mission v2 ATS discovery summary ===")
    print(f"  Candidates file: {args.candidates_path}")
    print(f"  Candidates:      {len(candidates)}")
    print(f"  Probed:          {probed}")
    print(f"  ATS validated:   {len(ats_matched)}")

    if args.dry_run:
        print("Dry run — skipping Ollama filter and BigQuery insert.")
        checkpoint(force=True)
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

    if not approved:
        logger.error("No approved rows to insert")
        checkpoint(force=True)
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
