"""Arbeitnow EU-centric job board API (https://www.arbeitnow.com/api/job-board-api)."""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://www.arbeitnow.com/api/job-board-api"


def fetch_jobs(*, max_pages: int = 5, **_kwargs: object) -> JobBoardFetchResult:
    jobs: list[dict[str, Any]] = []
    try:
        with json_client(timeout=45.0) as client:
            page = 1
            while page <= max(1, max_pages):
                resp = client.get(API_URL, params={"page": page})
                resp.raise_for_status()
                data = resp.json()
                chunk = data.get("data") or []
                if not isinstance(chunk, list) or not chunk:
                    break
                jobs.extend(chunk)
                links = data.get("links") or {}
                next_url = links.get("next") if isinstance(links, dict) else None
                if not next_url:
                    break
                page += 1
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="arbeitnow",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="arbeitnow",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"pages<={max_pages}",
        jobs=jobs,
    )
