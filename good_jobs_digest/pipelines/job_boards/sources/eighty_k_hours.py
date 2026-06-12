"""80,000 Hours job board — public Algolia search API."""

from __future__ import annotations

import json
from typing import Any

from pipelines.job_boards.sources.http import json_client, polite_sleep
from pipelines.job_boards.sources.types import JobBoardFetchResult

ALGOLIA_APP_ID = "W6KM1UDIB3"
ALGOLIA_API_KEY = "d1d7f2c8696e7b36837d5ed337c4a319"
ALGOLIA_INDEX = "jobs_prod"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"


def _normalize(hit: dict[str, Any]) -> dict[str, Any]:
    company = hit.get("company") or {}
    return {
        "id": hit.get("post_pk") or hit.get("objectID"),
        "title": hit.get("title"),
        "company_name": hit.get("company_name") or company.get("name"),
        "company_url": hit.get("company_url") or company.get("url"),
        "description_short": hit.get("description_short"),
        "description": hit.get("description"),
        "url": hit.get("url_external"),
        "salary": hit.get("salary"),
        "salary_type": hit.get("salary_type"),
        "locations": hit.get("card_locations") or hit.get("tags_city"),
        "remote": hit.get("tags_location_type"),
        "experience_min": hit.get("experience_min"),
        "posted_at": hit.get("posted_at"),
        "closes_at": hit.get("closes_at"),
        "tags": {
            "area": hit.get("tags_area"),
            "role_type": hit.get("tags_role_type"),
            "skills": hit.get("tags_skill"),
            "country": hit.get("tags_country"),
        },
        "raw": hit,
    }


def fetch_jobs(*, limit: int = 20, hits_per_page: int | None = None, page: int = 0) -> JobBoardFetchResult:
    hits_per_page = limit if hits_per_page is None else hits_per_page
    params = f"hitsPerPage={hits_per_page}&page={page}"
    body = {"params": params}
    headers = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
    }
    try:
        with json_client() as client:
            resp = client.post(ALGOLIA_URL, headers=headers, content=json.dumps(body))
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="80000hours",
            ok=False,
            method="algolia",
            job_count=0,
            error=str(exc),
            notes="https://jobs.80000hours.org/",
        )

    hits = data.get("hits") or []
    jobs = [_normalize(h) for h in hits]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="80000hours",
        ok=bool(jobs),
        method=f"algolia:{ALGOLIA_INDEX}",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job={k: v for k, v in sample.items() if k != "raw"},
        notes=(
            f"Algolia reports {data.get('nbHits', '?')} total hits. "
            "Full descriptions may be empty; use url_external for ATS page."
        ),
    )


def fetch_jobs_paginated(*, max_pages: int = 3, hits_per_page: int = 100) -> list[dict[str, Any]]:
    """Fetch multiple Algolia pages (for ingest pipelines)."""
    all_jobs: list[dict[str, Any]] = []
    with json_client() as client:
        headers = {
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "X-Algolia-API-Key": ALGOLIA_API_KEY,
            "Content-Type": "application/json",
        }
        for page in range(max_pages):
            params = f"hitsPerPage={hits_per_page}&page={page}"
            resp = client.post(
                ALGOLIA_URL,
                headers=headers,
                content=json.dumps({"params": params}),
            )
            resp.raise_for_status()
            hits = resp.json().get("hits") or []
            if not hits:
                break
            all_jobs.extend(_normalize(h) for h in hits)
            polite_sleep(0.3)
    return all_jobs
