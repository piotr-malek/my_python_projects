"""Climatebase — Next.js SSR (__NEXT_DATA__) for listings and job detail pages."""

from __future__ import annotations

import json
import re
from typing import Any

from pipelines.job_boards.sources.http import browser_client, polite_sleep
from pipelines.job_boards.sources.types import JobBoardFetchResult

JOBS_URL = "https://climatebase.org/jobs"
JOB_DETAIL_URL = "https://climatebase.org/job/{job_id}"
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def _parse_next_data(html: str) -> dict[str, Any]:
    match = NEXT_DATA_RE.search(html)
    if not match:
        raise ValueError("__NEXT_DATA__ not found in HTML")
    return json.loads(match.group(1))


def _normalize_listing(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id") or row.get("objectID"),
        "title": row.get("title"),
        "featured": row.get("featured"),
        "employer_name": row.get("name_of_employer"),
        "employer_id": row.get("employer_id"),
        "employer_short_description": row.get("employer_short_description"),
        "logo": row.get("logo"),
        "locations": row.get("locations"),
        "sectors": row.get("sectors"),
        "remote_preferences": row.get("remote_preferences"),
        "job_types": row.get("job_types"),
        "salary_from": row.get("salary_from"),
        "salary_to": row.get("salary_to"),
        "salary_period": row.get("salary_period"),
        "activation_date": row.get("activation_date"),
        "url": f"https://climatebase.org/job/{row.get('id')}",
        "raw": row,
    }


def _normalize_detail(page_props: dict[str, Any]) -> dict[str, Any]:
    data = page_props.get("data") or {}
    employer = data.get("employer") or {}
    return {
        "id": data.get("id"),
        "title": data.get("title"),
        "description": data.get("description"),
        "sanitized_description": page_props.get("sanitizedJobDescription"),
        "how_to_apply": data.get("how_to_apply"),
        "employer_name": data.get("employer_name") or employer.get("name"),
        "employer_logo": data.get("employer_logo") or employer.get("logo"),
        "remote": data.get("remote"),
        "remote_preferences": data.get("remote_preferences"),
        "job_types": data.get("job_types"),
        "experience_levels": data.get("experience_levels"),
        "active_categories": data.get("active_categories"),
        "locations": data.get("jobs_locations") or data.get("geo_location"),
        "salary_from": data.get("salary_from"),
        "salary_to": data.get("salary_to"),
        "salary_period": data.get("salary_period"),
        "activation_date": data.get("activation_date"),
        "expiration_date": data.get("expiration_date"),
        "similar_jobs": data.get("similar_jobs"),
        "url": f"https://climatebase.org/job/{data.get('id')}",
        "raw": data,
    }


def fetch_job_listings(*, limit: int = 20, **_kwargs: object) -> JobBoardFetchResult:
    try:
        with browser_client() as client:
            resp = client.get(JOBS_URL)
            resp.raise_for_status()
            payload = _parse_next_data(resp.text)
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="climatebase",
            ok=False,
            method="next_ssr",
            job_count=0,
            error=str(exc),
            notes=JOBS_URL,
        )

    rows = payload.get("props", {}).get("pageProps", {}).get("jobs") or []
    jobs = [_normalize_listing(r) for r in rows[:limit]]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="climatebase",
        ok=bool(jobs),
        method="next_ssr:__NEXT_DATA__",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job={k: v for k, v in sample.items() if k != "raw"},
        notes=(
            f"SSR ships {len(rows)} listings on the jobs page. "
            "Use fetch_job_detail(id) for full description. "
            "Additional pages may require browser automation."
        ),
    )


def fetch_job_detail(job_id: int | str) -> dict[str, Any]:
    url = JOB_DETAIL_URL.format(job_id=job_id)
    with browser_client() as client:
        resp = client.get(url)
        resp.raise_for_status()
        payload = _parse_next_data(resp.text)
    page_props = payload.get("props", {}).get("pageProps", {})
    polite_sleep(0.4)
    return _normalize_detail(page_props)
