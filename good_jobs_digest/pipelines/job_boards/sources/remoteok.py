"""RemoteOK jobs API (https://remoteok.com/api).

The first array element is a legal/attribution notice rather than a job, and it
has no `id` — that is what we filter on. Their terms require crediting Remote OK
with a dofollow link back to the job URL, which the digest does via the job link.
"""

from __future__ import annotations

from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_URL = "https://remoteok.com/api"


def _fix_mojibake(text: str) -> str:
    """RemoteOK double-encodes UTF-8 ("CataluÃ±a"); undo it when it round-trips."""
    if not text or not any(ch in text for ch in ("Ã", "â", "å")):
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def fetch_jobs(**_kwargs: object) -> JobBoardFetchResult:
    try:
        with json_client(timeout=45.0) as client:
            resp = client.get(API_URL)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="remoteok",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_URL,
        )

    if not isinstance(data, list):
        data = []
    # Drop the legal notice element (no id) and any malformed rows.
    jobs = [j for j in data if isinstance(j, dict) and j.get("id")]
    for job in jobs:
        for key in ("position", "company", "location", "description"):
            if isinstance(job.get(key), str):
                job[key] = _fix_mojibake(job[key])

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="remoteok",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"{len(jobs)} jobs (no pagination)",
        jobs=jobs,
    )
