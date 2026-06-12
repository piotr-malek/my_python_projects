"""Poll ATS APIs for employers listed in BigQuery curated_companies."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import Settings
from core.curated import load_curated_board_keys
from core.models import CompanyRow, effective_poll_enabled
from core.persist import persist_normalized_job
from normalize.handlers import (
    normalize_greenhouse,
    normalize_lever,
    normalize_smartrecruiters,
)
from pipelines.curated_ats.clients import greenhouse, lever, smartrecruiters
from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp
from pipelines.curated_ats.loader import load_curated_companies
from rank.prefilter import prefilter_title
from storage.poll_overrides import load_overrides, set_poll_disabled

if TYPE_CHECKING:
    from storage.bq_repository import JobBigQuery
    from storage.repository import JobRepository

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _title_passes(settings: Settings, title: str) -> bool:
    return prefilter_title(
        title,
        include_keywords=settings.TARGET_ROLE_KEYWORDS,
        exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
    )


def _ingest_greenhouse(
    http: HostRateLimitedHttp,
    repo: JobRepository,
    settings: Settings,
    row: CompanyRow,
    *,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> None:
    listings, status = greenhouse.fetch_greenhouse_job_list(http, row.ats_slug)
    if status == 404:
        logger.warning("Disabling poll: greenhouse:%s (404)", row.ats_slug)
        set_poll_disabled(settings.POLL_OVERRIDES_PATH, "greenhouse", row.ats_slug)
        return
    if status != 200:
        logger.warning("greenhouse:%s status %s", row.ats_slug, status)
        return

    for listing in listings:
        title = str(listing.get("title") or "")
        if not _title_passes(settings, title):
            continue
        jid = str(listing.get("id") or "")
        if not jid:
            continue
        existing = repo.get_job_by_key("greenhouse", row.ats_slug, jid)
        if existing is not None:
            repo.touch_job("greenhouse", row.ats_slug, jid, fetched_at)
            continue

        job, jstatus = greenhouse.fetch_greenhouse_job(http, row.ats_slug, jid)
        if jstatus != 200 or not job:
            continue
        if bq:
            bq.insert_raw_payload(
                fetched_at=fetched_at,
                ingest_batch_id=ingest_batch_id,
                ats_type="greenhouse",
                ats_slug=row.ats_slug,
                company_name=row.company_name,
                source_job_id=jid,
                request_url=f"https://boards-api.greenhouse.io/v1/boards/{row.ats_slug}/jobs/{jid}",
                http_status=jstatus,
                payload_kind="detail_item",
                payload=job,
            )
        norm = normalize_greenhouse(
            job,
            company_name=row.company_name,
            mission_category=row.mission_category,
            ats_slug=row.ats_slug,
        )
        persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)


def _ingest_lever(
    http: HostRateLimitedHttp,
    repo: JobRepository,
    settings: Settings,
    row: CompanyRow,
    *,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> None:
    jobs, status = lever.fetch_lever_postings(http, row.ats_slug, row.ats_region)
    lever_base = (
        "https://api.eu.lever.co/v0/postings"
        if row.ats_region == "eu"
        else "https://api.lever.co/v0/postings"
    )
    if bq:
        for job in jobs:
            bq.insert_raw_payload(
                fetched_at=fetched_at,
                ingest_batch_id=ingest_batch_id,
                ats_type="lever",
                ats_slug=row.ats_slug,
                company_name=row.company_name,
                source_job_id=str(job.get("id") or ""),
                request_url=f"{lever_base}/{row.ats_slug}?mode=json",
                http_status=status,
                payload_kind="listing_item",
                payload=job,
            )
    if status == 404:
        logger.warning("Disabling poll: lever:%s (404)", row.ats_slug)
        set_poll_disabled(settings.POLL_OVERRIDES_PATH, "lever", row.ats_slug)
        return
    if status != 200 and not jobs:
        logger.warning("lever:%s status %s", row.ats_slug, status)
        return
    for job in jobs:
        title = str(job.get("text") or job.get("title") or "")
        if not _title_passes(settings, title):
            continue
        sid = str(job.get("id") or "")
        existing = repo.get_job_by_key("lever", row.ats_slug, sid)
        if existing is not None:
            repo.touch_job("lever", row.ats_slug, sid, fetched_at)
            continue
        norm = normalize_lever(
            job,
            company_name=row.company_name,
            mission_category=row.mission_category,
            ats_slug=row.ats_slug,
        )
        persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)


def _ingest_smartrecruiters(
    http: HostRateLimitedHttp,
    repo: JobRepository,
    settings: Settings,
    row: CompanyRow,
    *,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
    smartrecruiters_api_key: str,
) -> None:
    items, status = smartrecruiters.fetch_smartrecruiters_posting_list(
        http, row.ats_slug, smartrecruiters_api_key
    )
    if status == 404:
        logger.warning("Disabling poll: smartrecruiters:%s (404)", row.ats_slug)
        set_poll_disabled(settings.POLL_OVERRIDES_PATH, "smartrecruiters", row.ats_slug)
        return
    if status != 200 and not items:
        logger.warning("smartrecruiters:%s status %s", row.ats_slug, status)
        return

    for item in items:
        title = str(item.get("name") or "")
        if not _title_passes(settings, title):
            continue
        sid = str(item.get("id") or "")
        if not sid:
            continue
        if bq:
            bq.insert_raw_payload(
                fetched_at=fetched_at,
                ingest_batch_id=ingest_batch_id,
                ats_type="smartrecruiters",
                ats_slug=row.ats_slug,
                company_name=row.company_name,
                source_job_id=sid,
                request_url=f"https://api.smartrecruiters.com/v1/companies/{row.ats_slug}/postings",
                http_status=status,
                payload_kind="listing_item",
                payload=item,
            )

        existing = repo.get_job_by_key("smartrecruiters", row.ats_slug, sid)
        if existing is not None:
            repo.touch_job("smartrecruiters", row.ats_slug, sid, fetched_at)
            continue

        detail = None
        ref = item.get("ref")
        if isinstance(ref, str) and ref.startswith("http"):
            detail = smartrecruiters.fetch_sr_posting_detail(http, ref, smartrecruiters_api_key)
            if bq and detail:
                bq.insert_raw_payload(
                    fetched_at=fetched_at,
                    ingest_batch_id=ingest_batch_id,
                    ats_type="smartrecruiters",
                    ats_slug=row.ats_slug,
                    company_name=row.company_name,
                    source_job_id=sid,
                    request_url=ref,
                    http_status=200,
                    payload_kind="detail_item",
                    payload=detail,
                )

        bundle = {"list": item, "detail": detail}
        norm = normalize_smartrecruiters(
            bundle,
            company_name=row.company_name,
            mission_category=row.mission_category,
            ats_slug=row.ats_slug,
        )
        persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)


def _ingest_company(
    http: HostRateLimitedHttp,
    repo: JobRepository,
    settings: Settings,
    row: CompanyRow,
    *,
    bq: JobBigQuery | None = None,
    ingest_batch_id: str = "",
    smartrecruiters_api_key: str = "",
) -> None:
    fetched_at = _now_iso()
    ats = row.ats_type
    if ats == "greenhouse":
        _ingest_greenhouse(
            http, repo, settings, row, bq=bq, ingest_batch_id=ingest_batch_id, fetched_at=fetched_at
        )
    elif ats == "lever":
        _ingest_lever(
            http, repo, settings, row, bq=bq, ingest_batch_id=ingest_batch_id, fetched_at=fetched_at
        )
    elif ats == "smartrecruiters":
        _ingest_smartrecruiters(
            http,
            repo,
            settings,
            row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            smartrecruiters_api_key=smartrecruiters_api_key,
        )
    else:
        logger.warning("Unknown ats_type %s for %s", ats, row.company_name)


def _ingest_company_task(
    row: CompanyRow,
    *,
    repo: JobRepository,
    settings: Settings,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    http: HostRateLimitedHttp,
) -> None:
    logger.info("Curated ATS: %s (%s)", row.company_name, row.ats_type)
    try:
        _ingest_company(
            http,
            repo,
            settings,
            row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            smartrecruiters_api_key=settings.SMARTRECRUITERS_API_KEY,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Curated ingest failed for %s: %s", row.company_name, exc)


def ingest_curated_ats(
    repo: JobRepository,
    settings: Settings,
    *,
    bq: JobBigQuery | None = None,
    ingest_batch_id: str = "",
    limit: int | None = None,
) -> int:
    """Poll curated employers from BigQuery or registry CSV (optional --limit)."""
    board_keys = load_curated_board_keys(settings, bq)
    removed = repo.delete_stale_curated_jobs(board_keys)
    if removed:
        logger.info("Removed %s stale jobs not in curated registry", removed)

    all_companies = load_curated_companies(settings, bq)
    overrides = load_overrides(settings.POLL_OVERRIDES_PATH)
    all_companies = [c for c in all_companies if effective_poll_enabled(c, overrides)]
    if not all_companies:
        logger.info("No curated companies to ingest")
        return 0

    companies = all_companies
    if limit is not None and limit > 0:
        companies = all_companies[:limit]
    logger.info(
        "Curated ingest: polling %s / %s companies (workers=%s)",
        len(companies),
        len(all_companies),
        settings.INGEST_WORKERS,
    )

    workers = max(1, settings.INGEST_WORKERS)
    with HostRateLimitedHttp(settings.INGEST_DELAY_MS) as http:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _ingest_company_task,
                    row,
                    repo=repo,
                    settings=settings,
                    bq=bq,
                    ingest_batch_id=ingest_batch_id,
                    http=http,
                )
                for row in companies
            ]
            for fut in as_completed(futures):
                fut.result()

    if bq:
        bq.flush_raw_payloads()
        bq.flush_normalized_jobs()
    return len(companies)
