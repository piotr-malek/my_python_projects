"""Indeed via python-jobspy, queried per EU country.

jobspy is an optional dependency (it drags in pandas and a pinned numpy), so the
import is lazy: without it installed this source reports a clean failure instead
of breaking the whole ingest.

Indeed only honours ONE of `hours_old` / (`job_type` + `is_remote`) / `easy_apply`
per search, so we send `is_remote=True` and rely on the pipeline's own freshness
window rather than `hours_old`.
"""

from __future__ import annotations

import logging
from typing import Any

from pipelines.job_boards.sources.types import JobBoardFetchResult

logger = logging.getLogger(__name__)

# jobspy expects full country names for Indeed.
DEFAULT_COUNTRIES = ("Germany", "Netherlands", "Poland", "Ireland", "Spain")
DEFAULT_SEARCH_TERMS = ("data engineer", "analytics engineer", "data platform engineer")


def _row_to_dict(row: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if value is None:
            out[key] = None
        elif hasattr(value, "isoformat"):
            out[key] = value.isoformat()
        else:
            # Normalize pandas NaN/NaT to None without importing pandas here.
            out[key] = None if value != value else value  # noqa: PLR0124
    return out


def fetch_jobs(
    *,
    countries: tuple[str, ...] | list[str] = DEFAULT_COUNTRIES,
    search_terms: tuple[str, ...] | list[str] = DEFAULT_SEARCH_TERMS,
    results_wanted: int = 25,
    proxies: list[str] | None = None,
    **_kwargs: object,
) -> JobBoardFetchResult:
    try:
        from jobspy import scrape_jobs
    except ImportError:
        return JobBoardFetchResult(
            source="indeed",
            ok=False,
            method="library",
            job_count=0,
            error="python-jobspy not installed (pip install python-jobspy)",
            notes="optional dependency",
        )

    jobs: list[dict[str, Any]] = []
    notes: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()

    for country in countries:
        for term in search_terms:
            try:
                frame = scrape_jobs(
                    site_name=["indeed"],
                    search_term=term,
                    location=country,
                    country_indeed=country,
                    results_wanted=results_wanted,
                    is_remote=True,
                    description_format="markdown",
                    proxies=proxies,
                    verbose=0,
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{country}/{term}: {exc}")
                continue
            if frame is None or getattr(frame, "empty", True):
                notes.append(f"{country}/{term}=0")
                continue
            fresh = 0
            for _, row in frame.iterrows():
                record = _row_to_dict(row)
                key = str(record.get("id") or record.get("job_url") or "")
                if not key or key in seen:
                    continue
                seen.add(key)
                record["search_country"] = country
                record["search_term"] = term
                jobs.append(record)
                fresh += 1
            notes.append(f"{country}/{term}={fresh}")

    if not jobs and errors:
        return JobBoardFetchResult(
            source="indeed",
            ok=False,
            method="library",
            job_count=0,
            error="; ".join(errors[:3]),
            notes="; ".join(notes),
        )

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="indeed",
        ok=True,
        method="library",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes="; ".join(notes + ([f"errors: {len(errors)}"] if errors else [])),
        jobs=jobs,
    )
