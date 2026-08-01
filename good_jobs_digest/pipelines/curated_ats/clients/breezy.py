"""Breezy HR public JSON feed."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def fetch_breezy_jobs(http: HostRateLimitedHttp, company: str) -> tuple[list[dict[str, Any]], int]:
    url = f"https://{company}.breezy.hr/json"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    data = r.json()
    return (data if isinstance(data, list) else []), 200
