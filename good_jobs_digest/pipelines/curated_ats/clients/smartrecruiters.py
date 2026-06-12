"""SmartRecruiters public Posting API ingest."""

from __future__ import annotations

import logging
from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp

logger = logging.getLogger(__name__)

SR_BASE = "https://api.smartrecruiters.com/v1/companies"


def _headers(api_key: str) -> dict[str, str]:
    h: dict[str, str] = {}
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


def fetch_sr_posting_detail(
    http: HostRateLimitedHttp, detail_url: str, api_key: str
) -> dict[str, Any] | None:
    r = http.get(detail_url, headers=_headers(api_key) or None)
    if r.status_code >= 400:
        return None
    data = r.json()
    return data if isinstance(data, dict) else None


def fetch_smartrecruiters_posting_list(
    http: HostRateLimitedHttp,
    company_identifier: str,
    api_key: str,
) -> tuple[list[dict[str, Any]], int]:
    """List postings only (no per-job detail calls)."""
    offset = 0
    limit = 100
    items: list[dict[str, Any]] = []
    last_status = 200
    hdrs = _headers(api_key) or None
    while True:
        url = f"{SR_BASE}/{company_identifier}/postings?limit={limit}&offset={offset}"
        r = http.get(url, headers=hdrs)
        last_status = r.status_code
        if r.status_code == 404:
            return [], 404
        if r.status_code >= 400:
            logger.warning("SmartRecruiters %s list HTTP %s", company_identifier, r.status_code)
            return items if items else [], r.status_code
        data = r.json()
        if not isinstance(data, dict):
            break
        content = data.get("content") or []
        total = int(data.get("totalFound") or 0)
        if not content:
            break
        for item in content:
            if isinstance(item, dict):
                items.append(item)
        offset += len(content)
        if offset >= total or len(content) < limit:
            break
    return items, last_status


def fetch_all_smartrecruiters_postings(http, company_identifier: str, api_key: str):
    """Legacy full fetch with detail per posting."""
    from pipelines.curated_ats.clients.base import ThrottledHttp

    if isinstance(http, ThrottledHttp):
        pass
    items, status = fetch_smartrecruiters_posting_list(http, company_identifier, api_key)
    if status != 200 and not items:
        return [], status
    combined: list[dict[str, Any]] = []
    for item in items:
        ref = item.get("ref")
        detail = None
        if isinstance(ref, str) and ref.startswith("http"):
            detail = fetch_sr_posting_detail(http, ref, api_key)
        combined.append({"list": item, "detail": detail})
    return combined, status
