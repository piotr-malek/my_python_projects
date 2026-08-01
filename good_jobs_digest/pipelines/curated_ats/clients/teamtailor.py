"""Teamtailor jobs.json feed."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def fetch_teamtailor_jobs(http: HostRateLimitedHttp, company: str) -> tuple[list[dict[str, Any]], int]:
    url = f"https://{company}.teamtailor.com/jobs.json"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    data = r.json()
    jobs = data.get("jobs") if isinstance(data, dict) else None
    return (jobs if isinstance(jobs, list) else []), 200
