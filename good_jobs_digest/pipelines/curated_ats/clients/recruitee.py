"""Recruitee public offers API."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def fetch_recruitee_offers(http: HostRateLimitedHttp, company: str) -> tuple[list[dict[str, Any]], int]:
    url = f"https://{company}.recruitee.com/api/offers/"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    data = r.json()
    offers = data.get("offers") if isinstance(data, dict) else None
    return (offers if isinstance(offers, list) else []), 200
