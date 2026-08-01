#!/usr/bin/env python3
"""Discover employers with public ATS boards; LLM-filter; save to BigQuery curated_companies."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from discovery.ats_registry import FAST_PROBE_ATS, set_bulk_probe  # noqa: E402
from discovery.validate import validate_registry_entry  # noqa: E402
from discovery.collector import CURATED_REVALIDATE_SOURCES, collect_unified  # noqa: E402
from discovery.resolve import EmployerCandidate, careers_url, resolve_candidate  # noqa: E402
from discovery.sources import DEFAULT_SEEDS_PATH  # noqa: E402
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
    """Fields that define *which* employers to probe (resume must match)."""
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


def _progress_probe_tuning(args: argparse.Namespace) -> dict[str, str | int | bool]:
    """Speed/tuning knobs — logged but do not invalidate resume."""
    return {
        "workers": int(args.workers),
        "delay_ms": int(args.delay_ms),
        "max_slug_attempts": int(args.max_slug_attempts),
        "fast": bool(args.fast),
        "skip_validate": bool(args.skip_validate),
        "only_ats": args.only_ats or "",
        "skip_ats": args.skip_ats or "",
        "no_website_first": bool(args.no_website_first),
        "sequential_ats": bool(args.sequential_ats),
        "batch_ats": bool(args.batch_ats),
        "bulk": bool(args.bulk),
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
        default="80000hours,escapethecity,climatebase,seeds",
        help=(
            "Comma-separated sources: v1 (80000hours, escapethecity, climatebase, seeds, "
            "eu_mission_tech, bcorp), "
            "v2 (coefficient,sff,gwwc,ace,givewell,gates,ea_funds), "
            "funders (openphil,echoing_green,ashoka_fellows,fast_forward), "
            "curated revalidate (curated_revalidate,registry_revalidate), mined"
        ),
    )
    parser.add_argument("--seeds-path", type=Path, default=DEFAULT_SEEDS_PATH)
    parser.add_argument("--min-rows", type=int, default=50, help="Minimum rows to insert into BQ")
    parser.add_argument(
        "--target",
        type=int,
        default=0,
        help="Stop ATS probing after N matches (0 = probe all candidates)",
    )
    parser.add_argument("--delay-ms", type=int, default=500, help="Delay between employer probes")
    parser.add_argument(
        "--max-slug-attempts",
        type=int,
        default=6,
        help="Max slug variants to probe per employer (0 = unlimited)",
    )
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
        "--no-website-first",
        action="store_true",
        help="Skip homepage careers-link discovery before ATS slug guessing",
    )
    parser.add_argument(
        "--sequential-ats",
        action="store_true",
        help="Probe ATS types one-by-one per slug (slower; gentler on rate limits)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel employer probes (8–12 recommended for bulk funder runs)",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip second-pass validate_registry_entry (faster; probe already checks identity)",
    )
    parser.add_argument(
        "--only-ats",
        default="",
        help="Comma-separated ATS types to probe (e.g. greenhouse,lever,ashby)",
    )
    parser.add_argument(
        "--skip-ats",
        default="",
        help="Comma-separated ATS types to skip (e.g. workable,workday)",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Subset ATS only (top 4): workers=8, delay=0, 2 slugs, no website-first",
    )
    parser.add_argument(
        "--bulk",
        action="store_true",
        help=(
            "Full ATS coverage, hours-not-days: workers=6, batched waves, "
            "3 slugs, no website-first, skip validate"
        ),
    )
    parser.add_argument(
        "--batch-ats",
        action="store_true",
        help="Probe ATS in waves (fast batch → Workable tier → tier-2); default with --bulk",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover + validate only; do not call Ollama or write BigQuery",
    )
    args = parser.parse_args()
    if args.bulk:
        args.workers = max(args.workers, 6)
        args.delay_ms = 0
        args.max_slug_attempts = min(args.max_slug_attempts, 3) if args.max_slug_attempts else 3
        args.no_website_first = True
        args.skip_validate = True
        args.batch_ats = True
        set_bulk_probe(True)
    if args.fast:
        args.workers = max(args.workers, 8)
        args.delay_ms = 0
        args.max_slug_attempts = min(args.max_slug_attempts, 2) if args.max_slug_attempts else 2
        args.no_website_first = True
        args.skip_validate = True
        if not args.only_ats:
            args.only_ats = ",".join(sorted(FAST_PROBE_ATS))
        set_bulk_probe(True)

    if args.batch_ats and not args.bulk and not args.fast:
        set_bulk_probe(True)

    only_ats = frozenset(
        a.strip().lower() for a in args.only_ats.split(",") if a.strip()
    ) or None
    skip_ats = frozenset(a.strip().lower() for a in args.skip_ats.split(",") if a.strip()) or None

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    source_keys = {s.lower() for s in sources}
    revalidate_mode = bool(source_keys & CURATED_REVALIDATE_SOURCES)
    include_mined = "mined" in source_keys
    source_list = [s for s in sources if s.lower() != "mined"]
    if revalidate_mode and not args.skip_llm:
        logger.info("Curated revalidate mode — skipping LLM filter (use --skip-llm to silence)")
        args.skip_llm = True
    if revalidate_mode and args.min_rows > 1:
        logger.info("Curated revalidate mode — lowering min-rows to 1")
        args.min_rows = 1

    bq: JobBigQuery | None = None
    try:
        bq = _connect_bq()
    except Exception as exc:  # noqa: BLE001
        if revalidate_mode:
            logger.error("Curated revalidate requires BigQuery: %s", exc)
            return 1
        logger.warning("BigQuery unavailable during collect (%s)", exc)

    logger.info("Collecting candidates from: %s", ", ".join(source_list))
    candidates = collect_unified(
        source_list,
        seeds_path=args.seeds_path,
        climatebase_max_listings=args.climatebase_max,
        climatebase_fetch_details=not args.no_climatebase_details,
        eighty_k_max_pages=args.eighty_k_max_pages,
        escapethecity_max_pages=args.etc_max_pages,
        bcorp_max_pages=args.bcorp_max_pages,
        bcorp_per_page=args.bcorp_per_page,
        bcorp_requests_per_second=args.bcorp_rps,
        bcorp_reset_checkpoint=args.bcorp_reset_checkpoint,
        include_mined=include_mined and not revalidate_mode,
        settings=settings,
        bq=bq,
    )
    logger.info("Unique employer candidates: %s", len(candidates))

    if args.reset_progress and args.progress_path.exists():
        args.progress_path.unlink()
        logger.info("Deleted old progress file: %s", args.progress_path)

    config_sig = _progress_config_signature(args)
    probe_tuning = _progress_probe_tuning(args)
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
                    "ats_type": str(row.get("ats_type") or ""),
                    "ats_slug": str(row.get("ats_slug") or ""),
                    "ats_region": str(row.get("ats_region") or "global"),
                    "careers_url": job_board_url,
                    "ats_key": ats_key,
                }
            )
        logger.info(
            "Resumed progress: processed=%s, ats_matched=%s (%s)",
            len(processed_candidates),
            len(ats_matched),
            args.progress_path,
        )
        saved_tuning = state.get("probe_tuning")
        if saved_tuning and saved_tuning != probe_tuning:
            logger.info("Resuming with updated probe tuning: %s", probe_tuning)
    elif state:
        logger.info("Progress file ignored due to config mismatch: %s", args.progress_path)

    existing_bq_companies: set[str] = set()
    if not revalidate_mode:
        try:
            bq_for_dedupe = bq or _connect_bq()
            existing_bq_companies = bq_for_dedupe.fetch_curated_company_names()
            logger.info("Loaded %s existing curated companies for name dedupe", len(existing_bq_companies))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping BQ company-name dedupe (%s)", exc)
    else:
        logger.info("Curated revalidate mode — processing all registry employers (no name skip)")

    def checkpoint(force: bool = False) -> None:
        if not force and probed % 10 != 0:
            return
        payload = {
            "updated_at": _utc_now_iso(),
            "config": config_sig,
            "probe_tuning": probe_tuning,
            "processed_candidates": sorted(processed_candidates),
            "ats_matched": ats_matched,
            "probed_count": probed,
        }
        _save_progress(args.progress_path, payload)

    state_lock = threading.Lock()

    def _sleep_probe_delay() -> None:
        if args.delay_ms > 0:
            time.sleep(args.delay_ms / 1000.0)

    def _process_candidate(cand: EmployerCandidate) -> None:
        nonlocal probed
        if args.target and len(ats_matched) >= args.target:
            return
        ckey = _candidate_key(cand.company_name)
        with state_lock:
            if ckey in processed_candidates:
                return
            if ckey in existing_bq_companies:
                processed_candidates.add(ckey)
                checkpoint()
                return
            processed_candidates.add(ckey)
            probed += 1
            current_probed = probed

        try:
            with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                match = resolve_candidate(
                    client,
                    cand,
                    try_eu_lever=args.try_eu_lever,
                    max_slug_attempts=args.max_slug_attempts,
                    website_first=not args.no_website_first,
                    parallel_ats=not args.sequential_ats,
                    skip_ats=skip_ats,
                    only_ats=only_ats,
                    batch_ats=args.batch_ats,
                )
        except httpx.RequestError as exc:
            logger.warning("ATS probe request error for %s: %s", cand.company_name, exc)
            with state_lock:
                checkpoint()
            _sleep_probe_delay()
            return

        if not match:
            if current_probed % 25 == 0:
                logger.info(
                    "Probed %s/%s — ATS matched %s",
                    current_probed,
                    len(candidates),
                    len(ats_matched),
                )
            with state_lock:
                checkpoint()
            _sleep_probe_delay()
            return

        key = f"{match.ats_type}:{match.ats_slug.lower()}"
        with state_lock:
            if not revalidate_mode and key in seen_ats:
                checkpoint()
                _sleep_probe_delay()
                return

        if not args.skip_validate:
            try:
                with httpx.Client(timeout=30.0, follow_redirects=True) as client:
                    verified = validate_registry_entry(
                        client,
                        company_name=cand.company_name,
                        ats_type=match.ats_type,
                        ats_slug=match.ats_slug,
                        ats_region=match.ats_region,
                        require_identity=True,
                    )
            except httpx.RequestError as exc:
                logger.warning("ATS validate request error for %s: %s", cand.company_name, exc)
                with state_lock:
                    checkpoint()
                _sleep_probe_delay()
                return
            if not verified.ok:
                logger.warning(
                    "ATS_REJECT %s → %s:%s (%s)",
                    cand.company_name,
                    match.ats_type,
                    match.ats_slug,
                    verified.reason,
                )
                with state_lock:
                    checkpoint()
                _sleep_probe_delay()
                return

        board_url = match.careers_url or careers_url(match.ats_type, match.ats_slug)
        with state_lock:
            seen_ats.add(key)
            ats_matched.append(
                {
                    "company_name": cand.company_name,
                    "job_board_url": board_url,
                    "mission_category": cand.mission_category or "mission",
                    "discovery_source": cand.discovery_source or "curated_revalidate",
                    "ats_type": match.ats_type,
                    "ats_slug": match.ats_slug,
                    "ats_region": match.ats_region,
                    "careers_url": board_url,
                    "ats_key": key,
                }
            )
            logger.info(
                "ATS_%s %s → %s (%s)",
                "REVALIDATE" if revalidate_mode else "MATCH",
                cand.company_name,
                board_url,
                len(ats_matched),
            )
            checkpoint(force=True)
        _sleep_probe_delay()

    pending = [c for c in candidates if _candidate_key(c.company_name) not in processed_candidates]
    if args.target:
        pending = pending[: max(0, args.target - len(ats_matched))]

    workers = max(1, int(args.workers))
    if workers == 1:
        for cand in pending:
            if args.target and len(ats_matched) >= args.target:
                break
            _process_candidate(cand)
    else:
        logger.info("Parallel probing with %s workers (%s pending employers)", workers, len(pending))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_process_candidate, cand) for cand in pending]
            for fut in as_completed(futures):
                if args.target and len(ats_matched) >= args.target:
                    break
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Worker failed: %s", exc)

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
        bq = bq or _connect_bq()
    except Exception as exc:
        logger.error("BigQuery unavailable: %s", exc)
        return 1

    validated_at = _utc_now_iso()
    if revalidate_mode:
        to_update = [
            {
                k: v
                for k, v in row.items()
                if k
                in {
                    "company_name",
                    "job_board_url",
                    "ats_type",
                    "ats_slug",
                    "ats_region",
                    "careers_url",
                }
            }
            for row in approved
        ]
        updated = bq.update_curated_companies_from_matches(to_update, validated_at=validated_at)
        table = bq.table_id("curated_companies")
        checkpoint(force=True)
        print(f"\nUpdated {updated} row(s) in `{table}` (revalidation)")
        return 0 if updated >= args.min_rows or not approved else 1

    to_insert = [
        {
            k: v
            for k, v in row.items()
            if k
            in {
                "company_name",
                "job_board_url",
                "mission_category",
                "discovery_source",
                "ats_type",
                "ats_slug",
                "ats_region",
                "careers_url",
            }
        }
        for row in approved
    ]
    inserted = bq.insert_curated_companies(to_insert, added_at=_utc_now_iso())
    table = bq.table_id("curated_companies")
    checkpoint(force=True)
    print(f"\nInserted {inserted} new row(s) into `{table}`")
    return 0 if len(approved) >= args.min_rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
