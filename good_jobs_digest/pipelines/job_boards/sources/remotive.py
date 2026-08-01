"""Remotive remote jobs API (https://remotive.com/api/remote-jobs).

Remotive sits behind a Cloudflare cache whose key ignores the query string, so
`category`, `search` and `limit` params have no effect — every request returns the
same full dump. We therefore fetch once (their legal notice asks for very few calls
per day) and filter categories client-side.
"""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://remotive.com/api/remote-jobs"
# Category labels as they appear in the payload (the API's own slugs are not honoured).
# The whole feed is only ~34 jobs, so we take all of them by default and let the
# title prefilter decide — category filtering would risk dropping relevant roles
# filed under "All others" or "Information Technology".
DATA_CATEGORIES = (
    "Data and Analytics",
    "Artificial Intelligence",
    "Software Development",
    "Information Technology",
    "Devops",
)


def fetch_jobs(
    *,
    categories: tuple[str, ...] | list[str] | None = None,
    **_kwargs: object,
) -> JobBoardFetchResult:
    try:
        with json_client(timeout=45.0) as client:
            resp = client.get(API_URL)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="remotive",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    jobs = data.get("jobs") or []
    if not isinstance(jobs, list):
        jobs = []
    fetched = len(jobs)

    if categories:
        wanted = {str(c).strip().lower() for c in categories}
        jobs = [j for j in jobs if str(j.get("category") or "").strip().lower() in wanted]

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="remotive",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"{len(jobs)}/{fetched} after client-side category filter",
        jobs=jobs,
    )
