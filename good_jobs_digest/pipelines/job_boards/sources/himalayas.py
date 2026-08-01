"""Himalayas remote jobs API (https://himalayas.app/jobs/api)."""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client, polite_sleep
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://himalayas.app/jobs/api"
# The API silently caps `limit` at 20 regardless of what is requested, so reaching
# a useful slice of the ~97k postings means walking `offset` (which does work).
PAGE_SIZE = 20


def fetch_jobs(
    *,
    limit: int = 100,
    offset: int = 0,
    **_kwargs: object,
) -> JobBoardFetchResult:
    jobs: list[dict[str, Any]] = []
    total: int | None = None
    wanted = max(1, limit)
    try:
        with json_client(timeout=45.0) as client:
            cursor = max(0, offset)
            while len(jobs) < wanted:
                resp = client.get(API_URL, params={"limit": PAGE_SIZE, "offset": cursor})
                resp.raise_for_status()
                data = resp.json()
                if total is None:
                    total = data.get("totalCount")
                chunk = data.get("jobs") or []
                if not isinstance(chunk, list) or not chunk:
                    break
                jobs.extend(chunk)
                cursor += len(chunk)
                if len(chunk) < PAGE_SIZE:
                    break
                polite_sleep(0.3)
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="himalayas",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    jobs = jobs[:wanted]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="himalayas",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"total={total}; pages of {PAGE_SIZE}",
        jobs=jobs,
    )
