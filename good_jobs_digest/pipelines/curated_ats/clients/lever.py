"""Lever postings API ingest (global or EU host)."""

from __future__ import annotations

import logging
from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp

logger = logging.getLogger(__name__)

LEVER_GLOBAL = "https://api.lever.co/v0/postings"
LEVER_EU = "https://api.eu.lever.co/v0/postings"


def _lever_base(region: str) -> str:
    return LEVER_EU if (region or "").lower() == "eu" else LEVER_GLOBAL


def fetch_lever_postings(http: HostRateLimitedHttp, site: str, region: str) -> tuple[list[dict[str, Any]], int]:
    """Paginate Lever postings. Returns (all postings, last_http_status)."""
    base = _lever_base(region)
    out: list[dict[str, Any]] = []
    skip = 0
    limit = 100
    last_status = 200
    while True:
        url = f"{base}/{site}?mode=json&limit={limit}&skip={skip}"
        r = http.get(url)
        last_status = r.status_code
        if r.status_code == 404:
            try:
                err = r.json()
                if isinstance(err, dict) and err.get("ok") is False:
                    return [], 404
            except Exception:
                return [], 404
            return [], 404
        if r.status_code >= 400:
            logger.warning("Lever %s HTTP %s", site, r.status_code)
            return out if out else [], r.status_code
        chunk = r.json()
        if not isinstance(chunk, list):
            break
        out.extend(chunk)
        if len(chunk) < limit:
            break
        skip += limit
    return out, last_status
