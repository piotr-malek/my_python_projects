"""Ashby public job board API."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp

ASHBY_API = "https://api.ashbyhq.com/posting-api/job-board"


def fetch_ashby_jobs(http: HostRateLimitedHttp, slug: str) -> tuple[list[dict[str, Any]], int]:
    url = f"{ASHBY_API}/{slug}?includeCompensation=true"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    data = r.json()
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        return [], 200
    listed = [j for j in jobs if j.get("isListed") is not False]
    return listed, 200
