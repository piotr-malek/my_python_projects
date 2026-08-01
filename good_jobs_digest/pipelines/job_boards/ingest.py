"""Ingest jobs from mission job boards into SQLite + BigQuery."""

from __future__ import annotations

import logging
from typing import Any, Callable

from config import Settings
from normalize.boards import (
    BOARD_80000HOURS,
    BOARD_AAC,
    BOARD_ARBEITNOW,
    BOARD_CLIMATEBASE,
    BOARD_ESCAPETHECITY,
    BOARD_HIMALAYAS,
    BOARD_IDEALIST,
    BOARD_IMPACTPOOL,
    BOARD_HN_WHOISHIRING,
    BOARD_INDEED,
    BOARD_JOBICY,
    BOARD_RELIEFWEB,
    BOARD_REMOTEOK,
    BOARD_REMOTIVE,
    BOARD_TECHJOBSFORGOOD,
    BOARD_WEWORKREMOTELY,
    BOARD_WORKINGNOMADS,
    BOARD_WORKONCLIMATE,
    normalize_80000hours,
    normalize_animaladvocacycareers,
    normalize_arbeitnow,
    normalize_climatebase_listing,
    normalize_escapethecity,
    normalize_himalayas,
    normalize_hn_whoishiring,
    normalize_idealist,
    normalize_impactpool,
    normalize_indeed,
    normalize_jobicy,
    normalize_reliefweb,
    normalize_remoteok,
    normalize_remotive,
    normalize_techjobsforgood,
    normalize_weworkremotely,
    normalize_workingnomads,
    normalize_workonclimate,
)
from discovery.employer_candidates import mine_employer_candidate
from core.persist import persist_normalized_job
from rank.prefilter import prefilter_title
from pipelines.job_boards.sources.climatebase import JOB_DETAIL_URL, JOBS_URL, _parse_next_data
from pipelines.job_boards.sources.eighty_k_hours import (
    ALGOLIA_API_KEY,
    ALGOLIA_APP_ID,
    ALGOLIA_INDEX,
    ALGOLIA_URL,
    _normalize as normalize_80k_hit,
)
from pipelines.job_boards.sources.escapethecity import (
    ALGOLIA_API_KEY as ETC_API_KEY,
    ALGOLIA_APP_ID as ETC_APP_ID,
    ALGOLIA_INDEX as ETC_INDEX,
    JOB_FILTER as ETC_JOB_FILTER,
    _normalize as normalize_etc_hit,
)
from pipelines.job_boards.sources.http import BROWSER_HEADERS, polite_sleep
from pipelines.job_boards.sources.proxy_pool import ProxyPool
from pipelines.job_boards.sources.resilient_http import ResilientHttp
from pipelines.job_boards.sources.techjobsforgood import JOBS_URL as TJFG_JOBS_URL, _parse_listing_cards
from pipelines.job_boards.sources import (
    animaladvocacycareers,
    arbeitnow,
    himalayas,
    hn_whoishiring,
    idealist,
    impactpool,
    jobicy,
    jobspy_indeed,
    remoteok,
    remotive,
    weworkremotely,
    workingnomads,
    workonclimate,
)
from storage.bq_repository import JobBigQuery
from storage.repository import JobRepository

logger = logging.getLogger(__name__)

# Populated by _ingest_api_board so the health footer can distinguish "fetched a lot
# but nothing matched" from "fetched nothing at all" (a broken source).
_LAST_FETCH_COUNTS: dict[str, int] = {}
_LAST_FETCH_ERRORS: dict[str, str] = {}


def ingest_job_boards(
    repo: JobRepository,
    settings: Settings,
    *,
    bq: JobBigQuery | None = None,
    ingest_batch_id: str = "",
    fetched_at: str = "",
) -> dict[str, int]:
    """Fetch all enabled boards; return counts per board id."""
    if not settings.JOB_BOARDS_ENABLED:
        logger.info("JOB_BOARDS_ENABLED=false — skipping job boards")
        return {}

    pool = ProxyPool(
        settings.WEBSHARE_PROXIES_PATH,
        refresh_url=settings.WEBSHARE_PROXY_LIST_URL,
        api_key=settings.WEBSHARE_API_KEY,
        max_age_hours=settings.WEBSHARE_PROXY_MAX_AGE_HOURS,
    )
    http = ResilientHttp(delay_ms=settings.BOARD_INGEST_DELAY_MS, proxy_pool=pool)
    detail_http = ResilientHttp(delay_ms=settings.BOARD_DETAIL_DELAY_MS, proxy_pool=pool)

    counts: dict[str, int] = {}
    steps: list[tuple[str, Callable[[], int]]] = [
        (BOARD_CLIMATEBASE, lambda: _ingest_climatebase(repo, settings, http, detail_http, bq, ingest_batch_id, fetched_at)),
        (BOARD_80000HOURS, lambda: _ingest_80000hours(repo, settings, http, bq, ingest_batch_id, fetched_at)),
        (BOARD_ESCAPETHECITY, lambda: _ingest_escapethecity(repo, settings, http, bq, ingest_batch_id, fetched_at)),
        (BOARD_TECHJOBSFORGOOD, lambda: _ingest_techjobsforgood(repo, settings, http, detail_http, bq, ingest_batch_id, fetched_at)),
    ]
    # API/feed boards: (board id, enabled, zero-arg fetch, normalizer).
    api_boards: tuple[tuple[str, bool, Callable[[], Any], Callable[..., dict[str, Any]]], ...] = (
        (
            BOARD_REMOTIVE,
            settings.BOARD_REMOTIVE_ENABLED,
            remotive.fetch_jobs,
            normalize_remotive,
        ),
        (
            BOARD_ARBEITNOW,
            settings.BOARD_ARBEITNOW_ENABLED,
            lambda: arbeitnow.fetch_jobs(max_pages=settings.BOARD_ARBEITNOW_MAX_PAGES),
            normalize_arbeitnow,
        ),
        (
            BOARD_JOBICY,
            settings.BOARD_JOBICY_ENABLED,
            lambda: jobicy.fetch_jobs(
                count=settings.BOARD_JOBICY_COUNT, geo=settings.BOARD_JOBICY_GEO
            ),
            normalize_jobicy,
        ),
        (
            BOARD_HIMALAYAS,
            settings.BOARD_HIMALAYAS_ENABLED,
            lambda: himalayas.fetch_jobs(limit=settings.BOARD_HIMALAYAS_LIMIT),
            normalize_himalayas,
        ),
        (
            BOARD_REMOTEOK,
            settings.BOARD_REMOTEOK_ENABLED,
            remoteok.fetch_jobs,
            normalize_remoteok,
        ),
        (
            BOARD_WEWORKREMOTELY,
            settings.BOARD_WEWORKREMOTELY_ENABLED,
            weworkremotely.fetch_jobs,
            normalize_weworkremotely,
        ),
        (
            BOARD_WORKINGNOMADS,
            settings.BOARD_WORKINGNOMADS_ENABLED,
            workingnomads.fetch_jobs,
            normalize_workingnomads,
        ),
        (
            BOARD_HN_WHOISHIRING,
            settings.BOARD_HN_ENABLED,
            lambda: hn_whoishiring.fetch_jobs(max_comments=settings.BOARD_HN_MAX_COMMENTS),
            normalize_hn_whoishiring,
        ),
        (
            BOARD_INDEED,
            settings.BOARD_INDEED_ENABLED,
            lambda: jobspy_indeed.fetch_jobs(
                countries=settings.INDEED_COUNTRIES,
                search_terms=settings.INDEED_SEARCH_TERMS,
                results_wanted=settings.INDEED_RESULTS_WANTED,
            ),
            normalize_indeed,
        ),
    )
    for board_id, enabled, fetch_fn, norm_fn in api_boards:
        if not enabled:
            logger.info("Skipping %s (disabled)", board_id)
            continue
        steps.append(
            (
                board_id,
                lambda ff=fetch_fn, nf=norm_fn, bid=board_id: _ingest_api_board(
                    repo, settings, ff, nf, bid, bq, ingest_batch_id, fetched_at
                ),
            )
        )

    html_boards = (
        (BOARD_IDEALIST, idealist, normalize_idealist, settings.BOARD_IDEALIST_ENABLED),
        (BOARD_IMPACTPOOL, impactpool, normalize_impactpool, settings.BOARD_IMPACTPOOL_ENABLED),
        (BOARD_AAC, animaladvocacycareers, normalize_animaladvocacycareers, settings.BOARD_AAC_ENABLED),
        (
            BOARD_WORKONCLIMATE,
            workonclimate,
            normalize_workonclimate,
            settings.BOARD_WORKONCLIMATE_ENABLED,
        ),
    )
    for board_id, mod, norm_fn, enabled in html_boards:
        if not enabled:
            logger.info("Skipping %s (disabled — zero-yield HTML board)", board_id)
            continue
        steps.append(
            (
                board_id,
                lambda m=mod, nf=norm_fn, bid=board_id: _ingest_html_board(
                    repo, settings, m, nf, bid, bq, ingest_batch_id, fetched_at
                ),
            )
        )
    stats: list[dict[str, Any]] = []
    for board_id, fn in steps:
        logger.info("Job board ingest: %s", board_id)
        error: str | None = None
        try:
            counts[board_id] = fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("Job board %s failed: %s", board_id, exc)
            counts[board_id] = 0
            error = str(exc)[:300]
        stats.append(
            {
                "source": board_id,
                "fetched": _LAST_FETCH_COUNTS.get(board_id, 0),
                "passed": counts[board_id],
                "error": error or _LAST_FETCH_ERRORS.get(board_id),
            }
        )
        polite_sleep(settings.BOARD_PAUSE_BETWEEN_MS / 1000.0)
    repo.record_source_stats(stats, run_at=fetched_at or None)
    return counts


def _persist_board_job(
    repo: JobRepository,
    settings: Settings,
    norm: dict[str, Any],
    *,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
    raw_payload: dict[str, Any] | None = None,
    request_url: str | None = None,
    http_status: int = 200,
) -> bool:
    """Upsert job; return True if prefilter passed."""
    mine_employer_candidate(norm, discovery_source=norm.get("source") or norm.get("ats_slug") or "job_board")
    if bq and raw_payload is not None:
        bq.insert_raw_payload(
            fetched_at=fetched_at,
            ingest_batch_id=ingest_batch_id,
            ats_type=norm["ats_type"],
            ats_slug=norm["ats_slug"],
            company_name=norm["company_name"],
            source_job_id=norm["source_job_id"],
            request_url=request_url,
            http_status=http_status,
            payload_kind="listing_item",
            payload=raw_payload,
        )

    jid = persist_normalized_job(repo, settings, norm, bq=bq, ingested_at=fetched_at)
    saved = repo.get_job(jid)
    return bool(saved and saved["prefilter_pass"])


def _ingest_climatebase(
    repo: JobRepository,
    settings: Settings,
    http: ResilientHttp,
    detail_http: ResilientHttp,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    resp = http.get(JOBS_URL, headers={**BROWSER_HEADERS, "Referer": "https://climatebase.org/"})
    payload = _parse_next_data(resp.text)
    rows = payload.get("props", {}).get("pageProps", {}).get("jobs") or []
    rows = rows[: settings.CLIMATEBASE_MAX_LISTINGS]
    _LAST_FETCH_COUNTS[BOARD_CLIMATEBASE] = len(rows)
    n = 0
    for listing in rows:
        title = str(listing.get("title") or "")
        if not prefilter_title(
            title,
            include_keywords=settings.TARGET_ROLE_KEYWORDS,
            exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
            seniority_exclude_keywords=getattr(settings, "SENIORITY_EXCLUDE_KEYWORDS", ()),
        ):
            continue
        detail = None
        if settings.CLIMATEBASE_FETCH_DETAILS:
            jid = listing.get("id")
            if jid:
                url = JOB_DETAIL_URL.format(job_id=jid)
                try:
                    dresp = detail_http.get(
                        url,
                        headers={**BROWSER_HEADERS, "Referer": JOBS_URL},
                    )
                    dpayload = _parse_next_data(dresp.text)
                    detail = dpayload.get("props", {}).get("pageProps", {})
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Climatebase detail %s: %s", jid, exc)
        norm = normalize_climatebase_listing(listing, detail)
        if _persist_board_job(
            repo,
            settings,
            norm,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            raw_payload={"listing": listing, "detail": detail},
            request_url=JOBS_URL,
        ):
            n += 1
    return n


def _ingest_80000hours(
    repo: JobRepository,
    settings: Settings,
    http: ResilientHttp,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    headers = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
    }
    n = 0
    _LAST_FETCH_COUNTS[BOARD_80000HOURS] = 0
    for page in range(settings.BOARD_80000HOURS_MAX_PAGES):
        params = f"hitsPerPage=100&page={page}"
        resp = http.post_json(ALGOLIA_URL, body={"params": params}, extra_headers=headers)
        resp.raise_for_status()
        hits = resp.json().get("hits") or []
        if not hits:
            break
        _LAST_FETCH_COUNTS[BOARD_80000HOURS] += len(hits)
        for hit in hits:
            job = normalize_80k_hit(hit)
            norm = normalize_80000hours(job)
            if _persist_board_job(
                repo,
                settings,
                norm,
                bq=bq,
                ingest_batch_id=ingest_batch_id,
                fetched_at=fetched_at,
                raw_payload=hit,
                request_url=ALGOLIA_URL,
            ):
                n += 1
    return n


def _ingest_escapethecity(
    repo: JobRepository,
    settings: Settings,
    http: ResilientHttp,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    url = f"https://{ETC_APP_ID}-dsn.algolia.net/1/indexes/{ETC_INDEX}/query"
    headers = {
        "X-Algolia-Application-Id": ETC_APP_ID,
        "X-Algolia-API-Key": ETC_API_KEY,
        "Content-Type": "application/json",
    }
    n = 0
    _LAST_FETCH_COUNTS[BOARD_ESCAPETHECITY] = 0
    for page in range(settings.BOARD_ESCAPETHECITY_MAX_PAGES):
        params = f"hitsPerPage=100&page={page}&filters={ETC_JOB_FILTER}"
        resp = http.post_json(url, body={"params": params}, extra_headers=headers)
        resp.raise_for_status()
        hits = resp.json().get("hits") or []
        if not hits:
            break
        _LAST_FETCH_COUNTS[BOARD_ESCAPETHECITY] += len(hits)
        for hit in hits:
            job = normalize_etc_hit(hit)
            norm = normalize_escapethecity(job)
            if _persist_board_job(
                repo,
                settings,
                norm,
                bq=bq,
                ingest_batch_id=ingest_batch_id,
                fetched_at=fetched_at,
                raw_payload=hit,
                request_url=url,
            ):
                n += 1
    return n


def _ingest_techjobsforgood(
    repo: JobRepository,
    settings: Settings,
    http: ResilientHttp,
    detail_http: ResilientHttp,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    resp = http.get(
        TJFG_JOBS_URL,
        try_direct=True,
        try_proxies=True,
        proxy_only=False,
    )
    listings = _parse_listing_cards(resp.text)
    _LAST_FETCH_COUNTS[BOARD_TECHJOBSFORGOOD] = len(listings)
    n = 0
    for listing in listings:
        title = str(listing.get("title") or "")
        if not prefilter_title(
            title,
            include_keywords=settings.TARGET_ROLE_KEYWORDS,
            exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
            seniority_exclude_keywords=getattr(settings, "SENIORITY_EXCLUDE_KEYWORDS", ()),
        ):
            continue
        detail = None
        if settings.TJFG_FETCH_DETAILS and listing.get("id"):
            try:
                url = f"https://techjobsforgood.com/jobs/{listing['id']}/"
                dresp = detail_http.get(url, try_direct=False, try_proxies=True)
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(dresp.text, "html.parser")
                h1 = soup.find("h1")
                meta = soup.select_one('meta[name="description"]')
                detail = {
                    "title": h1.get_text(strip=True) if h1 else title,
                    "meta_description": meta.get("content") if meta else None,
                    "text": soup.get_text("\n", strip=True)[:8000],
                }
            except Exception as exc:  # noqa: BLE001
                logger.warning("TJFG detail %s: %s", listing.get("id"), exc)
        norm = normalize_techjobsforgood(listing, detail)
        if _persist_board_job(
            repo,
            settings,
            norm,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            raw_payload={"listing": listing, "detail": detail},
            request_url=TJFG_JOBS_URL,
        ):
            n += 1
    return n


def _ingest_reliefweb(
    repo: JobRepository,
    settings: Settings,
    http: ResilientHttp,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    if not settings.reliefweb_configured():
        return 0
    appname = settings.RELIEFWEB_APPNAME
    api = "https://api.reliefweb.int/v2/jobs"
    n = 0
    offset = 0
    limit = min(100, settings.RELIEFWEB_JOBS_LIMIT)
    while offset < settings.RELIEFWEB_JOBS_LIMIT:
        url = f"{api}?appname={appname}&limit={limit}&offset={offset}&profile=full&sort[]=date:desc"
        resp = http.get(url)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data") or []
        if not items:
            break
        for item in items:
            norm = normalize_reliefweb(item)
            if _persist_board_job(
                repo,
                settings,
                norm,
                bq=bq,
                ingest_batch_id=ingest_batch_id,
                fetched_at=fetched_at,
                raw_payload=item,
                request_url=url,
            ):
                n += 1
        offset += len(items)
        total = (data.get("total") or {}).get("value")
        if total is not None and offset >= int(total):
            break
        polite_sleep(0.5)
    return n


def _ingest_html_board(
    repo: JobRepository,
    settings: Settings,
    module,
    normalize_fn,
    board_id: str,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    listings = module.fetch_jobs()
    _LAST_FETCH_COUNTS[board_id] = len(listings)
    n = 0
    for listing in listings:
        norm = normalize_fn(listing)
        if _persist_board_job(
            repo,
            settings,
            norm,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            raw_payload=listing,
            request_url=getattr(module, "LIST_URL", board_id),
        ):
            n += 1
    return n


def _ingest_api_board(
    repo: JobRepository,
    settings: Settings,
    fetch_fn,
    normalize_fn,
    board_id: str,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    fetched_at: str,
) -> int:
    result = fetch_fn()
    _LAST_FETCH_COUNTS[board_id] = int(result.job_count or 0)
    _LAST_FETCH_ERRORS.pop(board_id, None)
    if not result.ok:
        logger.error("%s fetch failed: %s", board_id, result.error)
        _LAST_FETCH_ERRORS[board_id] = str(result.error or "fetch failed")[:300]
        return 0
    jobs = list(result.jobs or [])
    n = 0
    for job in jobs:
        # Gate on the NORMALIZED title: raw payloads disagree about where the title
        # lives (Jobicy uses jobTitle, RemoteOK position, HN has none at all — its
        # title is parsed out of the comment body), and WWR's raw title is prefixed
        # with the company name. Normalizing first is cheap and uniform.
        norm = normalize_fn(job)
        if not prefilter_title(
            str(norm.get("title") or ""),
            include_keywords=settings.TARGET_ROLE_KEYWORDS,
            exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
            seniority_exclude_keywords=getattr(settings, "SENIORITY_EXCLUDE_KEYWORDS", ()),
        ):
            continue
        if _persist_board_job(
            repo,
            settings,
            norm,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=fetched_at,
            raw_payload=job,
            request_url=board_id,
        ):
            n += 1
    logger.info("%s: fetched=%s prefilter_pass=%s", board_id, result.job_count, n)
    return n
