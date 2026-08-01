"""Jobicy remote jobs API (https://jobicy.com/api/v2/remote-jobs)."""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://jobicy.com/api/v2/remote-jobs"


def fetch_jobs(*, count: int = 50, geo: str = "emea", **_kwargs: object) -> JobBoardFetchResult:
    try:
        with json_client(timeout=45.0) as client:
            resp = client.get(API_URL, params={"count": max(1, min(count, 100)), "geo": geo})
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="jobicy",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    jobs = data.get("jobs") or []
    if not isinstance(jobs, list):
        jobs = []
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="jobicy",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"geo={geo}",
        jobs=jobs,
    )
