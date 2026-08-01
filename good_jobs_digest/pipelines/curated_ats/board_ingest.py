"""Per-ATS curated board ingest handlers."""

from __future__ import annotations

import logging
from typing import Any, Callable

from core.models import CompanyRow
from core.persist import persist_normalized_job
from normalize.handlers import (
    normalize_ashby,
    normalize_bamboohr,
    normalize_breezy,
    normalize_greenhouse,
    normalize_jazzhr,
    normalize_lever,
    normalize_personio,
    normalize_recruitee,
    normalize_smartrecruiters,
    normalize_teamtailor,
    normalize_workable,
    normalize_workday,
)
from pipelines.curated_ats.clients import (
    ashby,
    bamboohr,
    breezy,
    greenhouse,
    jazzhr,
    lever,
    personio,
    recruitee,
    smartrecruiters,
    teamtailor,
    workable,
    workday,
)
from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp
from rank.prefilter import prefilter_title
from storage.poll_overrides import set_poll_disabled

if False:  # TYPE_CHECKING
    from config import Settings
    from storage.bq_repository import JobBigQuery
    from storage.repository import JobRepository

logger = logging.getLogger(__name__)


def _title_passes(settings, title: str) -> bool:
    return prefilter_title(
        title,
        include_keywords=settings.TARGET_ROLE_KEYWORDS,
        exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
        seniority_exclude_keywords=getattr(settings, "SENIORITY_EXCLUDE_KEYWORDS", ()),
    )


def _ingest_listings(
    *,
    repo,
    settings,
    row: CompanyRow,
    bq,
    ingest_batch_id: str,
    fetched_at: str,
    http: HostRateLimitedHttp,
    listings: list[dict[str, Any]],
    normalizer: Callable[..., dict[str, Any]],
    ats_type: str,
    list_url: str,
    status: int,
    title_key: str = "title",
    id_key: str = "id",
) -> None:
    if status == 404:
        logger.warning("Disabling poll: %s:%s (404)", ats_type, row.ats_slug)
        set_poll_disabled(settings.POLL_OVERRIDES_PATH, ats_type, row.ats_slug)
        return
    if status != 200 and not listings:
        logger.warning("%s:%s status %s", ats_type, row.ats_slug, status)
        return
    for listing in listings:
        title = str(listing.get(title_key) or listing.get("text") or listing.get("name") or "")
        if not _title_passes(settings, title):
            continue
        sid = str(listing.get(id_key) or listing.get("shortcode") or listing.get("slug") or title)
        existing = repo.get_job_by_key(ats_type, row.ats_slug, sid)
        if existing is not None:
            repo.touch_job(ats_type, row.ats_slug, sid, fetched_at)
            continue
        if bq:
            bq.insert_raw_payload(
                fetched_at=fetched_at,
                ingest_batch_id=ingest_batch_id,
                ats_type=ats_type,
                ats_slug=row.ats_slug,
                company_name=row.company_name,
                source_job_id=sid,
                request_url=list_url,
                http_status=status,
                payload_kind="listing_item",
                payload=listing,
            )
        norm = normalizer(
            listing,
            company_name=row.company_name,
            mission_category=row.mission_category,
            ats_slug=row.ats_slug,
        )
        persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)


def ingest_company_board(
    http: HostRateLimitedHttp,
    repo,
    settings,
    row: CompanyRow,
    *,
    bq=None,
    ingest_batch_id: str = "",
    fetched_at: str = "",
    smartrecruiters_api_key: str = "",
) -> None:
    ats = row.ats_type
    if ats == "greenhouse":
        listings, status = greenhouse.fetch_greenhouse_job_list(http, row.ats_slug)
        url = f"https://boards-api.greenhouse.io/v1/boards/{row.ats_slug}/jobs"
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
                    request_url=f"{url}/{jid}",
                    http_status=jstatus,
                    payload_kind="detail_item",
                    payload=job,
                )
            norm = normalize_greenhouse(
                job, company_name=row.company_name, mission_category=row.mission_category, ats_slug=row.ats_slug
            )
            persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)
        return

    if ats == "lever":
        jobs, status = lever.fetch_lever_postings(http, row.ats_slug, row.ats_region)
        _ingest_listings(
            repo=repo,
            settings=settings,
            row=row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            http=http,
            listings=jobs,
            normalizer=normalize_lever,
            ats_type="lever",
            list_url=f"lever:{row.ats_slug}",
            status=status,
            title_key="text",
            id_key="id",
        )
        return

    if ats == "smartrecruiters":
        items, status = smartrecruiters.fetch_smartrecruiters_posting_list(
            http, row.ats_slug, smartrecruiters_api_key
        )
        if status == 404:
            set_poll_disabled(settings.POLL_OVERRIDES_PATH, "smartrecruiters", row.ats_slug)
            return
        for item in items:
            title = str(item.get("name") or "")
            if not _title_passes(settings, title):
                continue
            sid = str(item.get("id") or "")
            if not sid:
                continue
            existing = repo.get_job_by_key("smartrecruiters", row.ats_slug, sid)
            if existing is not None:
                repo.touch_job("smartrecruiters", row.ats_slug, sid, fetched_at)
                continue
            detail = None
            ref = item.get("ref")
            if isinstance(ref, str) and ref.startswith("http"):
                detail = smartrecruiters.fetch_sr_posting_detail(http, ref, smartrecruiters_api_key)
            norm = normalize_smartrecruiters(
                {"list": item, "detail": detail},
                company_name=row.company_name,
                mission_category=row.mission_category,
                ats_slug=row.ats_slug,
            )
            persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)
        return

    simple_fetchers: dict[str, tuple[Any, Any, str]] = {
        "ashby": (ashby.fetch_ashby_jobs, normalize_ashby, "ashby"),
        "workable": (workable.fetch_workable_jobs, normalize_workable, "workable"),
        "recruitee": (recruitee.fetch_recruitee_offers, normalize_recruitee, "recruitee"),
        "personio": (personio.fetch_personio_jobs, normalize_personio, "personio"),
        "bamboohr": (bamboohr.fetch_bamboohr_jobs, normalize_bamboohr, "bamboohr"),
        "breezy": (breezy.fetch_breezy_jobs, normalize_breezy, "breezy"),
        "jazzhr": (jazzhr.fetch_jazzhr_jobs, normalize_jazzhr, "jazzhr"),
        "teamtailor": (teamtailor.fetch_teamtailor_jobs, normalize_teamtailor, "teamtailor"),
    }
    if ats in simple_fetchers:
        fetch_fn, norm_fn, label = simple_fetchers[ats]
        listings, status = fetch_fn(http, row.ats_slug)
        _ingest_listings(
            repo=repo,
            settings=settings,
            row=row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            http=http,
            listings=listings,
            normalizer=norm_fn,
            ats_type=label,
            list_url=f"{label}:{row.ats_slug}",
            status=status,
        )
        return

    if ats == "workday":
        listings, status, _wd = workday.fetch_workday_jobs(http, row.ats_slug)
        _ingest_listings(
            repo=repo,
            settings=settings,
            row=row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            http=http,
            listings=listings,
            normalizer=normalize_workday,
            ats_type="workday",
            list_url=f"workday:{row.ats_slug}",
            status=status,
            title_key="title",
            id_key="bulletFields",
        )
        return

    logger.warning("Unknown ats_type %s for %s", ats, row.company_name)
