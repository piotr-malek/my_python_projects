#!/usr/bin/env python3
"""CLI: ingest | score | digest | run-all — good_jobs_digest."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from config import settings
from profile.preferences import build_scoring_input, digest_remote_only, load_preferences
from digest.builder import build_markdown_digest
from mail.mailer import JobDigestMailer
from core.curated import CURATED_ATS_TYPES, load_curated_board_keys
from digest.formatting import dedupe_by_company_title
from digest.selection import exclude_already_sent
from pipelines.curated_ats.ingest import ingest_curated_ats
from pipelines.job_boards import ingest_job_boards
from rank.scorer import JobScorer
from rank.employer_mission_gate import filter_jobs_by_employer_mission
from storage.bq_repository import JobBigQuery
from storage.repository import JobRepository

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOARD_ATS_TYPE = "job_board"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect_bq() -> JobBigQuery | None:
    if not settings.BQ_ENABLED:
        return None
    try:
        bq = JobBigQuery(settings)
        bq.ensure_tables()
        return bq
    except Exception as exc:
        logger.error(
            "BigQuery unavailable (%s). Fix credentials, then: python main.py init-bq",
            exc,
        )
        return None


def cmd_reset_db(_args: argparse.Namespace) -> None:
    """Drop local SQLite job state so the next ingest/score run starts fresh."""
    removed = JobRepository.wipe_local_db(settings.SQLITE_PATH)
    legacy_poll = settings.SQLITE_PATH.parent / "curated_poll_state.json"
    if legacy_poll.exists():
        legacy_poll.unlink()
        removed.append(legacy_poll.name)
    repo = JobRepository(settings.SQLITE_PATH)
    repo.init_db()
    logger.info(
        "SQLite reset at %s (%s). Re-run: python main.py init-bq (if you cleared BQ), then run-all",
        settings.SQLITE_PATH,
        ", ".join(removed) if removed else "new empty db",
    )


def cmd_init_bq(_args: argparse.Namespace) -> None:
    if not settings.BQ_ENABLED:
        raise SystemExit("BQ_ENABLED is false — set BQ_ENABLED=true in .env")
    try:
        bq = JobBigQuery(settings)
        bq.ensure_tables()
        tables = bq.verify_tables()
        for t in tables:
            logger.info("Verified table %s", t)
    except Exception as exc:
        raise SystemExit(
            f"init-bq failed: {exc}\n"
            "Set GOOGLE_APPLICATION_CREDENTIALS or run: gcloud auth application-default login"
        ) from exc
    print(f"BigQuery OK: {settings.BQ_PROJECT_ID}.{settings.BQ_DATASET_ID} ({len(tables)} tables)")


def cmd_ingest(args: argparse.Namespace) -> None:
    repo = JobRepository(settings.SQLITE_PATH)
    repo.init_db()
    bq = _connect_bq()
    ingest_batch_id = str(uuid4())
    fetched_at = _now_iso()

    boards_only = getattr(args, "boards_only", False)
    curated_only = getattr(args, "curated_only", False)
    skip_boards = getattr(args, "skip_boards", False)

    if not boards_only:
        n = ingest_curated_ats(
            repo,
            settings,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            limit=args.limit,
        )
        logger.info("Curated ATS ingest: %s companies", n)

    if not curated_only and not skip_boards and settings.JOB_BOARDS_ENABLED:
        board_counts = ingest_job_boards(
            repo,
            settings,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
        )
        if bq:
            bq.flush_raw_payloads()
            bq.flush_normalized_jobs()
        logger.info("Job boards ingest: %s", board_counts)

    if bq:
        bq.flush_raw_payloads()
        bq.flush_normalized_jobs()

    logger.info("Ingest finished")


def cmd_score(args: argparse.Namespace) -> None:
    repo = JobRepository(settings.SQLITE_PATH)
    repo.init_db()
    if not settings.PREFERENCES_PATH.exists() and not settings.PROFILE_PATH.exists():
        raise SystemExit(
            f"Missing preferences ({settings.PREFERENCES_PATH}) and profile ({settings.PROFILE_PATH})"
        )
    scoring_input = build_scoring_input(
        preferences_path=settings.PREFERENCES_PATH,
        profile_path=settings.PROFILE_PATH,
    )
    scorer = JobScorer(settings)
    bq = _connect_bq()
    score_limit = getattr(args, "max", None)
    if score_limit is None and settings.SCORE_MAX_PER_RUN > 0:
        score_limit = settings.SCORE_MAX_PER_RUN
    max_age = settings.SCORE_MAX_AGE_DAYS if settings.SCORE_MAX_AGE_DAYS > 0 else None
    rows = repo.jobs_needing_score(limit=score_limit, max_age_days=max_age)
    cap_note = score_limit if score_limit else "none"
    age_note = max_age if max_age else "none"
    logger.info(
        "Scoring %s unscored job(s) (cap=%s, max_age=%s days)",
        len(rows),
        cap_note,
        age_note,
    )
    jobs: list[dict] = []
    for row in rows:
        d = dict(row)
        if bq:
            bq_row = bq.fetch_for_scoring(
                source=d["source"],
                ats_slug=d["ats_slug"],
                source_job_id=d["source_job_id"],
            )
            if bq_row:
                d.update(bq_row)
        jobs.append(d)

    jobs = filter_jobs_by_employer_mission(jobs, repo=repo, settings=settings)
    jobs_by_id = {int(j["id"]): j for j in jobs}
    logger.info("After employer mission gate: %s job(s) to score", len(jobs))
    ok = 0
    skipped = 0
    for job_id, out in scorer.score_jobs_parallel(jobs, scoring_input):
        row = jobs_by_id.get(job_id)
        if row is None or out is None:
            skipped += 1
            logger.warning("Skip job id=%s (score failed)", job_id)
            continue
        ok += 1
        combined = settings.combined_weighted(
            float(out.role_relevance),
            float(out.mission_alignment),
            float(out.candidate_fit),
        )
        repo.save_score(
            job_id,
            relevance=out.role_relevance,
            mission=out.mission_alignment,
            fit=out.candidate_fit,
            remote_ok=out.remote_ok,
            combined=combined,
            llm_payload=out.as_dict(),
            eu_hire_ok=out.eu_hire_ok,
            timezone_ok=out.timezone_ok,
            seniority_ok=out.seniority_ok,
        )
        if bq and settings.BQ_WRITE_LLM_SCORES:
            bq.append_llm_score(
                sqlite_job_id=job_id,
                source=row["source"],
                ats_slug=row["ats_slug"],
                source_job_id=row["source_job_id"],
                ollama_model=settings.GEMINI_MODEL,  # BQ column predates the provider swap
                role_relevance=out.role_relevance,
                mission_alignment=out.mission_alignment,
                candidate_fit=out.candidate_fit,
                remote_ok=out.remote_ok,
                combined_score=combined,
                llm_json=json.dumps(out.as_dict()),
                scored_at=_now_iso(),
            )
    if bq and settings.BQ_WRITE_LLM_SCORES:
        bq.flush_llm_scores()
    logger.info("Score finished (%s ok, %s skipped)", ok, skipped)


def cmd_digest(args: argparse.Namespace) -> None:
    repo = JobRepository(settings.SQLITE_PATH)
    repo.init_db()
    bq = _connect_bq()

    curated_keys = load_curated_board_keys(settings, bq)
    if not curated_keys:
        logger.warning(
            "No curated companies in BigQuery or %s — curated section will be empty",
            settings.CURATED_COMPANIES_PATH.name,
        )
    else:
        removed = repo.delete_stale_curated_jobs(curated_keys)
        if removed:
            logger.info("Purged %s jobs from boards not in curated registry", removed)

    sent_keys = bq.fetch_sent_job_keys() if bq and settings.BQ_WRITE_DIGEST_HISTORY else set()
    if bq is None:
        logger.warning("BigQuery unavailable — prior digest de-dupe is disabled")
    elif sent_keys:
        logger.info("BQ: %s job(s) already sent in a prior digest", len(sent_keys))

    prefs = load_preferences(settings.PREFERENCES_PATH)
    # An explicit .env setting wins; preferences.yaml digest.remote_only is the fallback.
    if os.getenv("DIGEST_REMOTE_ONLY") is not None:
        remote_only = settings.DIGEST_REMOTE_ONLY
        logger.info("Digest remote-only filter: %s (from .env)", remote_only)
    else:
        remote_only = digest_remote_only(prefs, default=settings.DIGEST_REMOTE_ONLY)
        logger.info("Digest remote-only filter: %s (from preferences)", remote_only)

    top_n = settings.DIGEST_TOP_N if settings.DIGEST_TOP_N > 0 else None
    curated_rows = exclude_already_sent(
        repo.jobs_for_digest(
            min_combined=settings.MIN_COMBINED_SCORE,
            remote_only=remote_only,
            ats_types=list(CURATED_ATS_TYPES),
            curated_board_keys=curated_keys,
            unsent_only=True,
            min_fit=settings.MIN_CANDIDATE_FIT,
            top_n=top_n,
        ),
        sent_keys,
    )
    board_rows = exclude_already_sent(
        repo.jobs_for_digest(
            min_combined=settings.MIN_COMBINED_SCORE,
            remote_only=remote_only,
            ats_types=[BOARD_ATS_TYPE],
            unsent_only=True,
            min_fit=settings.MIN_CANDIDATE_FIT,
            top_n=top_n,
        ),
        sent_keys,
    )
    logger.info(
        "Digest candidates: %s curated, %s job boards (unsent, above score threshold)",
        len(curated_rows),
        len(board_rows),
    )
    usage_path = getattr(settings, "GEMINI_USAGE_PATH", None)
    llm_usage: dict[str, object] | None = None
    if usage_path and Path(usage_path).is_file():
        try:
            recorded = json.loads(Path(usage_path).read_text(encoding="utf-8"))
            if str(recorded.get("date")) == date.today().isoformat():
                llm_usage = {
                    "used": recorded.get("count"),
                    "budget": settings.GEMINI_DAILY_REQUEST_BUDGET,
                }
        except (OSError, json.JSONDecodeError):
            llm_usage = None

    text = build_markdown_digest(
        curated_rows,
        board_rows,
        digest_date=date.today(),
        source_stats=repo.latest_source_stats(),
        llm_usage=llm_usage,
    )
    mailer = JobDigestMailer(settings)
    curated_deduped = dedupe_by_company_title([dict(r) for r in curated_rows])
    board_deduped = dedupe_by_company_title([dict(r) for r in board_rows])
    n = len(curated_deduped) + len(board_deduped)
    if n == 0:
        logger.info("No unsent jobs to email — skipping send")
        if args.dry_run_email:
            path = mailer.write_fallback(text, digest_date=date.today())
            logger.info("Wrote empty digest preview to %s", path)
        return
    if args.dry_run_email:
        logger.info("Dry run: not sending email. Preview:\n%s", text[:2000])
        path = mailer.write_fallback(text, digest_date=date.today())
        logger.info("Wrote %s", path)
        return
    if not settings.SMTP_USER or not settings.EMAIL_TO:
        path = mailer.write_fallback(text, digest_date=date.today())
        raise SystemExit(f"SMTP_USER / EMAIL_TO not set; wrote digest to {path}")
    try:
        mailer.send(text, digest_date=date.today(), n_jobs=n)
    except Exception as exc:
        path = mailer.write_fallback(text, digest_date=date.today())
        raise SystemExit(f"SMTP failed ({exc}); wrote {path}") from exc
    included_ids = [int(r["id"]) for r in curated_rows] + [int(r["id"]) for r in board_rows]
    repo.mark_digest_included(included_ids)
    if bq and settings.BQ_WRITE_DIGEST_HISTORY and not args.dry_run_email:
        bq.append_selected_jobs(
            digest_date=date.today().isoformat(),
            selected_at=_now_iso(),
            rows=curated_deduped + board_deduped,
        )
    logger.info("Digest sent (%s jobs)", n)


def cmd_run_all(args: argparse.Namespace) -> None:
    cmd_ingest(args)
    cmd_score(args)
    cmd_digest(args)


def cmd_check_email(args: argparse.Namespace) -> None:
    """Authenticate against SMTP without sending, so bad credentials fail fast."""
    from mail.mailer import JobDigestMailer

    try:
        JobDigestMailer(settings).verify_login()
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"SMTP check failed: {exc}") from exc
    logger.info("SMTP login OK (%s → %s)", settings.SMTP_USER, settings.EMAIL_TO)


def cmd_discover_candidates(args: argparse.Namespace) -> None:
    from discovery.discover_candidates import run_discover_candidates

    code = run_discover_candidates(
        limit=getattr(args, "limit", 100) or 100,
        dry_run=getattr(args, "dry_run", False),
        skip_llm=getattr(args, "skip_llm", False),
        bulk=getattr(args, "bulk", False),
    )
    if code != 0:
        raise SystemExit(code)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="good_jobs_digest — mission boards + curated ATS → SQLite → Gemini → email"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="Poll curated ATS (BQ) + mission job boards")
    p_ingest.add_argument("--limit", type=int, default=None, help="Max curated companies to poll")
    p_ingest.add_argument("--skip-boards", action="store_true", help="Skip mission job boards")
    p_ingest.add_argument("--boards-only", action="store_true", help="Only mission job boards")
    p_ingest.add_argument("--curated-only", action="store_true", help="Only curated ATS from BQ")
    p_ingest.set_defaults(func=cmd_ingest)

    p_score = sub.add_parser("score", help="Prefiltered jobs → Ollama scores")
    p_score.add_argument("--max", type=int, default=None, metavar="N")
    p_score.set_defaults(func=cmd_score)

    p_check = sub.add_parser("check-email", help="Verify SMTP credentials without sending")
    p_check.set_defaults(func=cmd_check_email)

    p_digest = sub.add_parser("digest", help="Build ranked digest (two sections) and email")
    p_digest.add_argument("--dry-run-email", action="store_true")
    p_digest.set_defaults(func=cmd_digest)

    p_init = sub.add_parser("init-bq", help="Create BigQuery dataset + tables")
    p_init.set_defaults(func=cmd_init_bq)

    p_reset = sub.add_parser(
        "reset-db",
        help="Delete local SQLite jobs DB (use after clearing BQ for a clean run)",
    )
    p_reset.set_defaults(func=cmd_reset_db)

    p_discover = sub.add_parser(
        "discover-candidates",
        help="Probe mined employer pool → curated_companies (run after ingest)",
    )
    p_discover.add_argument("--limit", type=int, default=100)
    p_discover.add_argument("--dry-run", action="store_true")
    p_discover.add_argument("--skip-llm", action="store_true")
    p_discover.add_argument("--bulk", action="store_true")
    p_discover.set_defaults(func=cmd_discover_candidates)

    p_all = sub.add_parser("run-all", help="ingest → score → digest")
    p_all.add_argument("--limit", type=int, default=None)
    p_all.add_argument("--max", type=int, default=None)
    p_all.add_argument("--dry-run-email", action="store_true")
    p_all.add_argument("--skip-boards", action="store_true")
    p_all.add_argument("--boards-only", action="store_true")
    p_all.add_argument("--curated-only", action="store_true")
    p_all.set_defaults(func=cmd_run_all)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
