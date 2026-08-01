"""Curated ingest polls a rotating stalest-first batch each run."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import Settings
from core.curated import load_curated_board_keys
from core.curated_poll_rotation import poll_batch_size, select_poll_batch
from core.models import CompanyRow, effective_poll_enabled
from pipelines.curated_ats.board_ingest import ingest_company_board
from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp
from pipelines.curated_ats.loader import load_curated_companies
from storage.poll_overrides import load_overrides

if TYPE_CHECKING:
    from storage.bq_repository import JobBigQuery
    from storage.repository import JobRepository

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _merge_local_poll_times(
    companies: list[CompanyRow],
    poll_times: dict[tuple[str, str], str],
) -> None:
    for row in companies:
        if row.last_validated_at:
            continue
        key = (row.ats_type.lower(), row.ats_slug.lower())
        ts = poll_times.get(key)
        if ts:
            row.last_validated_at = ts


def _touch_poll_time(
    row: CompanyRow,
    *,
    repo: JobRepository,
    bq: JobBigQuery | None,
    polled_at: str,
    polled_keys: list[tuple[str, str]] | None = None,
    lock: threading.Lock | None = None,
) -> None:
    repo.touch_curated_poll(row.ats_type, row.ats_slug, polled_at=polled_at)
    # BigQuery is stamped once at the end of the run, not per employer — see
    # touch_curated_last_validated_batch.
    if polled_keys is not None:
        if lock is not None:
            with lock:
                polled_keys.append((row.ats_type, row.ats_slug))
        else:
            polled_keys.append((row.ats_type, row.ats_slug))


def _ingest_company_task(
    row: CompanyRow,
    *,
    repo: JobRepository,
    settings: Settings,
    bq: JobBigQuery | None,
    ingest_batch_id: str,
    http: HostRateLimitedHttp,
    polled_keys: list[tuple[str, str]] | None = None,
    lock: threading.Lock | None = None,
) -> None:
    logger.info("Curated ATS: %s (%s)", row.company_name, row.ats_type)
    polled_at = _now_iso()
    try:
        ingest_company_board(
            http,
            repo,
            settings,
            row,
            bq=bq,
            ingest_batch_id=ingest_batch_id,
            fetched_at=polled_at,
            smartrecruiters_api_key=settings.SMARTRECRUITERS_API_KEY,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Curated ingest failed for %s: %s", row.company_name, exc)
    finally:
        _touch_poll_time(
            row,
            repo=repo,
            bq=bq,
            polled_at=polled_at,
            polled_keys=polled_keys,
            lock=lock,
        )


def ingest_curated_ats(
    repo: JobRepository,
    settings: Settings,
    *,
    bq: JobBigQuery | None = None,
    ingest_batch_id: str = "",
    limit: int | None = None,
) -> int:
    """Poll a rotating batch of curated employers (stalest first, total / divisor per run)."""
    board_keys = load_curated_board_keys(settings, bq)
    removed = repo.delete_stale_curated_jobs(board_keys)
    if removed:
        logger.info("Removed %s stale jobs not in curated registry", removed)

    all_companies = load_curated_companies(settings, bq)
    _merge_local_poll_times(all_companies, repo.fetch_curated_poll_times())
    overrides = load_overrides(settings.POLL_OVERRIDES_PATH)
    all_companies = [c for c in all_companies if effective_poll_enabled(c, overrides)]
    if not all_companies:
        logger.info("No curated companies to ingest")
        return 0

    divisor = settings.CURATED_POLL_ROTATION_DIVISOR
    companies = select_poll_batch(
        all_companies,
        rotation_divisor=divisor,
        limit=limit,
    )
    batch_cap = poll_batch_size(len(all_companies), divisor)
    logger.info(
        "Curated ingest: polling %s / %s companies "
        "(rotation divisor=%s, batch cap=%s, workers=%s)",
        len(companies),
        len(all_companies),
        divisor,
        batch_cap,
        settings.INGEST_WORKERS,
    )

    workers = max(1, settings.INGEST_WORKERS)
    polled_keys: list[tuple[str, str]] = []
    lock = threading.Lock()
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
                    polled_keys=polled_keys,
                    lock=lock,
                )
                for row in companies
            ]
            for fut in as_completed(futures):
                fut.result()

    if bq:
        try:
            updated = bq.touch_curated_last_validated_batch(
                polled_keys, validated_at=_now_iso()
            )
            logger.info("Stamped last_validated_at for %s curated board(s) in BQ", updated)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not stamp last_validated_at in BQ: %s", exc)
        bq.flush_raw_payloads()
        bq.flush_normalized_jobs()
    return len(companies)
