"""JazzHR applytojob JSON feed."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def fetch_jazzhr_jobs(http: HostRateLimitedHttp, company: str) -> tuple[list[dict[str, Any]], int]:
    url = f"https://{company}.applytojob.com/apply/jobs/json"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    data = r.json()
    jobs = data if isinstance(data, list) else data.get("jobs") if isinstance(data, dict) else None
    return (jobs if isinstance(jobs, list) else []), 200
