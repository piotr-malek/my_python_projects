#!/usr/bin/env python3
"""Probe EU employer candidates for public ATS boards and merge them into the curated CSV.

Reads candidates from the already-crawled EU B Corp directory
(`data/bcorp_companies.jsonl`) and/or the EU mission-tech seed list, resolves each
to an ATS board, optionally screens it with the mission LLM, and appends the
matches to `registry/curated_companies.csv`.

Writes CSV only — BigQuery is left alone (sync it yourself once you're happy with
the result). Resumable: every probed company is recorded in the checkpoint, so
re-running skips work already done.

Examples:
    python tools/probe_eu_employers.py --sources eu_seeds --workers 8
    python tools/probe_eu_employers.py --sources bcorp --limit 500 --skip-llm
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402
from core.curated_registry import CURATED_CSV_FIELDS, load_curated_csv  # noqa: E402
from discovery.mission_filter import EmployerMissionFilter  # noqa: E402
from discovery.resolve import EmployerCandidate, careers_url, resolve_candidate  # noqa: E402
from discovery.sources import (  # noqa: E402
    B_CORP_JSONL_PATH,
    EU_MISSION_TECH_SEEDS_PATH,
    _load_bcorp_candidates_from_jsonl,
    collect_from_seeds,
)
from discovery.validate import validate_registry_entry  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("probe_eu_employers")

CHECKPOINT_PATH = ROOT / "data" / "probe_eu_employers_state.json"


def _load_state(path: Path) -> dict:
    if not path.is_file():
        return {"probed": {}, "matched": []}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"probed": {}, "matched": []}
    state.setdefault("probed", {})
    state.setdefault("matched", [])
    return state


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=True, indent=1), encoding="utf-8")
    tmp.replace(path)


def _collect(sources: set[str], limit: int) -> list[EmployerCandidate]:
    candidates: dict[str, EmployerCandidate] = {}
    if "bcorp" in sources:
        found = _load_bcorp_candidates_from_jsonl(B_CORP_JSONL_PATH)
        logger.info("B Corp cache: %s employers (%s)", len(found), B_CORP_JSONL_PATH.name)
        candidates.update(found)
    if "eu_seeds" in sources:
        found = collect_from_seeds(
            EU_MISSION_TECH_SEEDS_PATH,
            discovery_source="eu_mission_tech",
            default_mission_category="eu_mission_tech",
        )
        logger.info("EU mission-tech seeds: %s employers", len(found))
        candidates.update(found)
    out = list(candidates.values())
    if limit > 0:
        out = out[:limit]
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sources",
        default="eu_seeds,bcorp",
        help="Comma list: eu_seeds, bcorp (default both; eu_seeds probed first)",
    )
    parser.add_argument("--csv", type=Path, default=settings.CURATED_COMPANIES_PATH)
    parser.add_argument("--limit", type=int, default=0, help="Max candidates to probe (0 = all)")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--delay-ms", type=int, default=150)
    parser.add_argument("--max-slug-attempts", type=int, default=4)
    parser.add_argument("--skip-llm", action="store_true", help="Skip the mission LLM screen")
    parser.add_argument("--skip-validate", action="store_true", help="Skip board validation")
    parser.add_argument("--dry-run", action="store_true", help="Do not write the CSV")
    parser.add_argument("--state", type=Path, default=CHECKPOINT_PATH)
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Skip probing; screen and merge the matches already in the checkpoint",
    )
    parser.add_argument(
        "--no-bq",
        action="store_true",
        help="Write the CSV only (default also upserts into BigQuery curated_companies)",
    )
    args = parser.parse_args()

    sources = {s.strip() for s in args.sources.split(",") if s.strip()}
    existing_rows = load_curated_csv(args.csv)
    existing_names = {r["company_name"].strip().lower() for r in existing_rows}
    existing_keys = {
        (r.get("ats_type", "").lower(), r.get("ats_slug", "").lower()) for r in existing_rows
    }
    logger.info("Registry already holds %s employers", len(existing_rows))

    state = _load_state(args.state)
    probed: dict[str, str] = state["probed"]

    if args.merge_only:
        saved = [m for m in state["matched"] if m.get("company_name")]
        logger.info("Merge-only: %s matches from the checkpoint", len(saved))
        return _screen_and_merge(saved, existing_rows, args, settings)

    candidates = [
        c
        for c in _collect(sources, args.limit)
        if c.company_name.strip().lower() not in existing_names
        and c.company_name.strip().lower() not in probed
    ]
    logger.info("To probe: %s employers (%s already probed in earlier runs)", len(candidates), len(probed))
    if not candidates:
        logger.info("Nothing to do")
        return 0

    lock = threading.Lock()
    matches: list[dict[str, str]] = []
    done = 0
    t0 = time.monotonic()

    def probe(candidate: EmployerCandidate) -> dict[str, str] | None:
        with httpx.Client(timeout=20.0, follow_redirects=True) as client:
            match = resolve_candidate(
                client,
                candidate,
                max_slug_attempts=args.max_slug_attempts,
            )
            if match is None:
                return None
            if not args.skip_validate:
                ok = validate_registry_entry(
                    client,
                    company_name=candidate.company_name,
                    ats_type=match.ats_type,
                    ats_slug=match.ats_slug,
                    ats_region=match.ats_region,
                )
                if not ok:
                    return None
        url = match.careers_url or careers_url(
            match.ats_type, match.ats_slug, region=match.ats_region
        )
        return {
            "company_name": candidate.company_name,
            "job_board_url": url,
            "careers_url": url,
            "mission_category": candidate.mission_category or "mission",
            "discovery_source": candidate.discovery_source or "eu_probe",
            "ats_type": match.ats_type,
            "ats_slug": match.ats_slug,
            "ats_region": match.ats_region or "global",
        }

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(probe, c): c for c in candidates}
        for future in as_completed(futures):
            candidate = futures[future]
            key = candidate.company_name.strip().lower()
            try:
                row = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.debug("probe failed for %s: %s", candidate.company_name, exc)
                row = None
            with lock:
                done += 1
                probed[key] = "match" if row else "no_ats"
                if row:
                    pair = (row["ats_type"].lower(), row["ats_slug"].lower())
                    if pair not in existing_keys:
                        existing_keys.add(pair)
                        matches.append(row)
                        logger.info(
                            "MATCH %s → %s:%s", row["company_name"], row["ats_type"], row["ats_slug"]
                        )
                if done % 50 == 0 or done == len(candidates):
                    rate = done / max(1e-9, time.monotonic() - t0)
                    logger.info(
                        "Probed %s/%s (%.1f/s) — %s ATS matches so far",
                        done,
                        len(candidates),
                        rate,
                        len(matches),
                    )
                    state["matched"] = matches
                    _save_state(args.state, state)
            if args.delay_ms > 0:
                time.sleep(args.delay_ms / 1000.0)

    state["matched"] = matches
    _save_state(args.state, state)
    logger.info("Probing done: %s ATS matches from %s candidates", len(matches), len(candidates))
    if not matches:
        return 0
    return _screen_and_merge(matches, existing_rows, args, settings)


def _screen_and_merge(
    matches: list[dict[str, str]],
    existing_rows: list[dict[str, str]],
    args: argparse.Namespace,
    settings: Any,
) -> int:
    """Mission-screen the ATS matches, then merge them into the CSV and BigQuery."""
    if not matches:
        logger.info("Nothing to merge")
        return 0

    existing_names = {r["company_name"].strip().lower() for r in existing_rows}
    matches = [m for m in matches if m["company_name"].strip().lower() not in existing_names]
    if not matches:
        logger.info("All matches are already in the registry")
        return 0

    if not args.skip_llm:
        logger.info("Mission-screening %s employers via LLM", len(matches))
        try:
            scorer = EmployerMissionFilter(settings)
        except RuntimeError as exc:
            # No API key yet: keep the matches rather than throwing away the probing
            # work, and let the mission gate screen these employers at score time.
            logger.warning("Mission screen unavailable (%s) — keeping all %s matches", exc, len(matches))
            scorer = None
        scored = scorer.score_employers([dict(m) for m in matches]) if scorer else []
        if scorer is None:
            scored = [dict(m, mission_score=str(settings.MISSION_APPROVE_MIN_SCORE)) for m in matches]
        by_name = {str(r.get("company_name") or "").strip().lower(): r for r in scored}
        threshold = settings.MISSION_APPROVE_MIN_SCORE
        kept: list[dict[str, str]] = []
        for row in matches:
            verdict = by_name.get(row["company_name"].strip().lower())
            if verdict is None:
                logger.warning("No mission score for %s — keeping", row["company_name"])
                kept.append(row)
                continue
            try:
                score = int(verdict.get("mission_score") or 0)
            except ValueError:
                score = 0
            if score >= threshold:
                kept.append(row)
            else:
                logger.info(
                    "Mission reject %s (%s) — %s",
                    row["company_name"],
                    score,
                    verdict.get("mission_llm_reason") or "",
                )
        logger.info("Mission screen: %s/%s kept", len(kept), len(matches))
        matches = kept

    if args.dry_run:
        logger.info("Dry run — not writing %s", args.csv)
        for row in matches[:20]:
            logger.info("  would add: %s (%s:%s)", row["company_name"], row["ats_type"], row["ats_slug"])
        return 0

    merged = existing_rows + matches
    merged.sort(key=lambda r: r["company_name"].lower())
    with args.csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(CURATED_CSV_FIELDS))
        writer.writeheader()
        for row in merged:
            writer.writerow({k: row.get(k, "") for k in CURATED_CSV_FIELDS})
    logger.info("Wrote %s (%s rows, +%s new)", args.csv, len(merged), len(matches))

    # BigQuery is what ingest actually reads (it overrides the CSV), so keep it in sync.
    if not args.no_bq and getattr(settings, "BQ_ENABLED", False):
        from datetime import datetime, timezone

        from storage.bq_repository import JobBigQuery

        try:
            bq = JobBigQuery(settings)
            added = bq.insert_curated_companies(
                matches,
                added_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            )
            logger.info(
                "BigQuery curated_companies: +%s rows (%s total)",
                added,
                len(bq.fetch_curated_companies()),
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("BigQuery sync failed (%s) — CSV is still updated", exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
