"""Ingest jobs from mission job boards into SQLite + BigQuery."""

from __future__ import annotations

import logging
from typing import Any, Callable

from config import Settings
from normalize.boards import (
    BOARD_80000HOURS,
    BOARD_CLIMATEBASE,
    BOARD_ESCAPETHECITY,
    BOARD_RELIEFWEB,
    BOARD_TECHJOBSFORGOOD,
    normalize_80000hours,
    normalize_climatebase_listing,
    normalize_escapethecity,
    normalize_reliefweb,
    normalize_techjobsforgood,
)
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
from storage.bq_repository import JobBigQuery
from storage.repository import JobRepository

logger = logging.getLogger(__name__)


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

    pool = ProxyPool(settings.WEBSHARE_PROXIES_PATH)
    http = ResilientHttp(delay_ms=settings.BOARD_INGEST_DELAY_MS, proxy_pool=pool)
    detail_http = ResilientHttp(delay_ms=settings.BOARD_DETAIL_DELAY_MS, proxy_pool=pool)

    counts: dict[str, int] = {}
    steps: list[tuple[str, Callable[[], int]]] = [
        (BOARD_CLIMATEBASE, lambda: _ingest_climatebase(repo, settings, http, detail_http, bq, ingest_batch_id, fetched_at)),
        (BOARD_80000HOURS, lambda: _ingest_80000hours(repo, settings, http, bq, ingest_batch_id, fetched_at)),
        (BOARD_ESCAPETHECITY, lambda: _ingest_escapethecity(repo, settings, http, bq, ingest_batch_id, fetched_at)),
        (BOARD_TECHJOBSFORGOOD, lambda: _ingest_techjobsforgood(repo, settings, http, detail_http, bq, ingest_batch_id, fetched_at)),
    ]
    if settings.reliefweb_configured():
        steps.append(
            (BOARD_RELIEFWEB, lambda: _ingest_reliefweb(repo, settings, http, bq, ingest_batch_id, fetched_at)),
        )
    else:
        logger.info(
            "Skipping ReliefWeb (set a real RELIEFWEB_APPNAME or RELIEFWEB_ENABLED=true when approved)"
        )
    for board_id, fn in steps:
        logger.info("Job board ingest: %s", board_id)
        try:
            counts[board_id] = fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("Job board %s failed: %s", board_id, exc)
            counts[board_id] = 0
        polite_sleep(settings.BOARD_PAUSE_BETWEEN_MS / 1000.0)
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
    n = 0
    for listing in rows:
        title = str(listing.get("title") or "")
        if not prefilter_title(
            title,
            include_keywords=settings.TARGET_ROLE_KEYWORDS,
            exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
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
    for page in range(settings.BOARD_80000HOURS_MAX_PAGES):
        params = f"hitsPerPage=100&page={page}"
        resp = http.post_json(ALGOLIA_URL, body={"params": params}, extra_headers=headers)
        resp.raise_for_status()
        hits = resp.json().get("hits") or []
        if not hits:
            break
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
    for page in range(settings.BOARD_ESCAPETHECITY_MAX_PAGES):
        params = f"hitsPerPage=100&page={page}&filters={ETC_JOB_FILTER}"
        resp = http.post_json(url, body={"params": params}, extra_headers=headers)
        resp.raise_for_status()
        hits = resp.json().get("hits") or []
        if not hits:
            break
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
    n = 0
    for listing in listings:
        title = str(listing.get("title") or "")
        if not prefilter_title(
            title,
            include_keywords=settings.TARGET_ROLE_KEYWORDS,
            exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
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
        url = f"{api}?appname={appname}&limit={limit}&offset={offset}&profile=full"
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
