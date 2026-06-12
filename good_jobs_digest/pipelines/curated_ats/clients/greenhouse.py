"""Greenhouse Job Board API ingest."""

from __future__ import annotations

import logging
from typing import Any

from pipelines.curated_ats.clients.host_pool import HostRateLimitedHttp

logger = logging.getLogger(__name__)

GREENHOUSE_BASE = "https://boards-api.greenhouse.io/v1/boards"


def fetch_greenhouse_job_list(
    http: HostRateLimitedHttp, board_token: str
) -> tuple[list[dict[str, Any]], int]:
    """List jobs without full description (fast pass)."""
    url = f"{GREENHOUSE_BASE}/{board_token}/jobs"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        logger.warning("Greenhouse list %s HTTP %s", board_token, r.status_code)
        return [], r.status_code
    jobs = r.json().get("jobs") or []
    if not isinstance(jobs, list):
        return [], 200
    return jobs, 200


def fetch_greenhouse_job(
    http: HostRateLimitedHttp, board_token: str, job_id: str | int
) -> tuple[dict[str, Any] | None, int]:
    """Fetch single job with full content."""
    url = f"{GREENHOUSE_BASE}/{board_token}/jobs/{job_id}"
    r = http.get(url)
    if r.status_code == 404:
        return None, 404
    if r.status_code >= 400:
        logger.warning("Greenhouse job %s/%s HTTP %s", board_token, job_id, r.status_code)
        return None, r.status_code
    data = r.json()
    return data if isinstance(data, dict) else None, 200


def fetch_greenhouse_jobs(http, board_token: str) -> tuple[list[dict[str, Any]], int]:
    """Legacy: full board with content=true (avoid in daily ingest)."""
    url = f"{GREENHOUSE_BASE}/{board_token}/jobs?content=true"
    r = http.get(url)
    if r.status_code == 404:
        return [], 404
    if r.status_code >= 400:
        return [], r.status_code
    jobs = r.json().get("jobs") or []
    return (jobs if isinstance(jobs, list) else []), 200
