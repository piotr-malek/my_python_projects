"""Workday CXS jobs API (tenant/site slug format: tenant|site)."""

from __future__ import annotations

from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp


def _split_slug(slug: str) -> tuple[str, str]:
    parts = slug.split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return slug, "External"


def fetch_workday_jobs(http: HostRateLimitedHttp, slug: str) -> tuple[list[dict[str, Any]], int, str]:
    tenant, site = _split_slug(slug)
    for wd in ("wd1", "wd3", "wd5"):
        url = f"https://{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
        body = {"appliedFacets": {}, "limit": 50, "offset": 0, "searchText": ""}
        r = http.post(url, json=body)
        if r.status_code != 200:
            continue
        data = r.json()
        postings = data.get("jobPostings") or data.get("jobs") or []
        if isinstance(postings, list) and postings:
            for item in postings:
                item["_wd_host"] = wd
                item["_tenant"] = tenant
                item["_site"] = site
            return postings, 200, wd
    return [], 404, ""
