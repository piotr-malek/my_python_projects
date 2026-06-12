"""Escape the City — public Algolia index (credentials embedded in site JS)."""

from __future__ import annotations

import json
from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

ALGOLIA_APP_ID = "6E1NSXNTTH"
ALGOLIA_API_KEY = "d4ceccfb371537bb6eab4cebd7f33f98"
ALGOLIA_INDEX = "listings-live"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"
JOB_FILTER = "option-listing-type:Job"


def _normalize(hit: dict[str, Any]) -> dict[str, Any]:
    slug = hit.get("slug")
    object_id = hit.get("objectID")
    return {
        "id": object_id,
        "slug": slug,
        "title": hit.get("job-title"),
        "headline": hit.get("headline"),
        "description": hit.get("description"),
        "company_name": hit.get("org-name"),
        "company_summary": hit.get("org-summary"),
        "company_logo": hit.get("org-logo"),
        "location": hit.get("location-txt"),
        "geo": hit.get("_geoloc"),
        "salary_low": hit.get("salary-low"),
        "salary_max": hit.get("salary-max"),
        "industries": hit.get("option-industry"),
        "term": hit.get("option-term"),
        "seniority": hit.get("option-seniority"),
        "remote": hit.get("option-remote"),
        "work_area": hit.get("option-work-area"),
        "posted_date": hit.get("posted-date"),
        "updated_at": hit.get("updated-at"),
        "url": f"https://www.escapethecity.org/opportunity/{slug}" if slug else None,
        "raw": hit,
    }


def fetch_jobs(*, limit: int = 20, hits_per_page: int | None = None, page: int = 0) -> JobBoardFetchResult:
    hits_per_page = limit if hits_per_page is None else hits_per_page
    params = f"hitsPerPage={hits_per_page}&page={page}&filters={JOB_FILTER}"
    headers = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
    }
    try:
        with json_client() as client:
            resp = client.post(
                ALGOLIA_URL,
                headers=headers,
                content=json.dumps({"params": params}),
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="escapethecity",
            ok=False,
            method="algolia",
            job_count=0,
            error=str(exc),
            notes="https://www.escapethecity.org/search/jobs",
        )

    hits = data.get("hits") or []
    jobs = [_normalize(h) for h in hits]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="escapethecity",
        ok=bool(jobs),
        method=f"algolia:{ALGOLIA_INDEX}",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job={k: v for k, v in sample.items() if k != "raw"},
        notes=(
            f"Algolia reports {data.get('nbHits', '?')} jobs. "
            "Many records have null description in the index; open url for full text."
        ),
    )
