"""Which jobs belong in the next digest email."""

from __future__ import annotations

from typing import Any


def job_identity_key(job: dict[str, Any] | Any) -> tuple[str, str, str]:
    """Stable key across SQLite resets (matches BQ selected_digest_jobs)."""
    if isinstance(job, dict):
        source = job.get("source") or ""
        ats_slug = job.get("ats_slug") or ""
        source_job_id = job.get("source_job_id") or ""
    else:
        source = job["source"]
        ats_slug = job["ats_slug"]
        source_job_id = job["source_job_id"]
    return (
        str(source).lower(),
        str(ats_slug).lower(),
        str(source_job_id),
    )


def exclude_already_sent(
    rows: list[Any],
    sent_keys: set[tuple[str, str, str]],
) -> list[Any]:
    if not sent_keys:
        return list(rows)
    return [r for r in rows if job_identity_key(r) not in sent_keys]
