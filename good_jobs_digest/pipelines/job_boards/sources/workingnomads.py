"""Working Nomads jobs API (https://www.workingnomads.com/api/exposed_jobs/).

A bare JSON array of recent jobs — no pagination, no filtering params. Low volume
(~40 items) but the `location` field often states a timezone requirement outright
("Time zone: CET (+/- 3 hours)"), which is a strong EU signal.
"""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://www.workingnomads.com/api/exposed_jobs/"


def fetch_jobs(**_kwargs: object) -> JobBoardFetchResult:
    try:
        with json_client(timeout=45.0) as client:
            resp = client.get(API_URL)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="workingnomads",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    jobs = [j for j in data if isinstance(j, dict)] if isinstance(data, list) else []
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="workingnomads",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"{len(jobs)} jobs (fixed recent window)",
        jobs=jobs,
    )
