"""Upsert normalized jobs into SQLite (+ optional BigQuery)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from rank.prefilter import prefilter_title
from storage.repository import JobRepository, content_hash

if TYPE_CHECKING:
    from config import Settings
    from storage.bq_repository import JobBigQuery


def persist_normalized_job(
    repo: JobRepository,
    settings: Settings,
    norm: dict[str, Any],
    *,
    bq: JobBigQuery | None = None,
    ingested_at: str | None = None,
) -> int:
    """Upsert job, apply title prefilter, queue BQ batch merge. Returns sqlite job id."""
    chash = content_hash(norm["description_text"])
    jid, _changed = repo.upsert_job(
        company_name=norm["company_name"],
        mission_category=norm.get("mission_category"),
        ats_type=norm["ats_type"],
        ats_slug=norm["ats_slug"],
        source=norm["source"],
        source_job_id=norm["source_job_id"],
        title=norm["title"],
        url=norm["url"],
        location_text=norm.get("location_text"),
        is_remote=bool(norm.get("is_remote")),
        salary_text=norm.get("salary_text"),
        description_text=norm["description_text"],
        chash=chash,
        posted_at=norm.get("posted_at_hint"),
    )
    passes = prefilter_title(
        norm["title"],
        include_keywords=settings.TARGET_ROLE_KEYWORDS,
        exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
    )
    repo.set_prefilter(jid, passes)
    if bq and passes:
        use_batch = getattr(settings, "BQ_BATCH_NORMALIZED", True)
        merge_on_ingest = getattr(settings, "BQ_MERGE_ON_INGEST", False)
        if use_batch or merge_on_ingest:
            saved = repo.get_job(jid)
            if saved is not None:
                ts = ingested_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                job_dict = dict(saved)
                if use_batch:
                    bq.queue_normalized_job(job_dict, ingested_at=ts)
                elif merge_on_ingest:
                    bq.merge_normalized_job(job_dict, ingested_at=ts)
    return jid
