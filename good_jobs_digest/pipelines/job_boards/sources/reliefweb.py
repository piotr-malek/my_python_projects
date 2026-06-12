"""ReliefWeb Jobs API — requires approved appname (see RELIEFWEB_APPNAME)."""

from __future__ import annotations

import os
from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

API_BASE = "https://api.reliefweb.int/v2/jobs"


def _appname() -> str | None:
    return os.environ.get("RELIEFWEB_APPNAME") or os.environ.get("RELIEFWEB_APP_NAME")


def _normalize(item: dict[str, Any]) -> dict[str, Any]:
    fields = item.get("fields") or {}
    source = (fields.get("source") or [{}])[0] if fields.get("source") else {}
    country = fields.get("country") or []
    theme = fields.get("theme") or []
    return {
        "id": item.get("id"),
        "title": fields.get("title"),
        "body": fields.get("body"),
        "body_html": fields.get("body-html"),
        "url": fields.get("url"),
        "date_created": fields.get("date", {}).get("created"),
        "date_closing": fields.get("date", {}).get("closing"),
        "organization": source.get("name") or source.get("shortname"),
        "organization_type": source.get("type"),
        "countries": [c.get("name") for c in country if isinstance(c, dict)],
        "themes": [t.get("name") for t in theme if isinstance(t, dict)],
        "career_categories": [
            c.get("name") for c in (fields.get("career-category") or []) if isinstance(c, dict)
        ],
        "type": [t.get("name") for t in (fields.get("type") or []) if isinstance(t, dict)],
        "experience": fields.get("experience"),
        "raw": item,
    }


def fetch_jobs(*, limit: int = 20, appname: str | None = None, **_kwargs: object) -> JobBoardFetchResult:
    app = appname or _appname()
    if not app:
        return JobBoardFetchResult(
            source="reliefweb",
            ok=False,
            method="api",
            job_count=0,
            error="RELIEFWEB_APPNAME is not set",
            notes=(
                "Request an appname at https://apidoc.reliefweb.int/parameters#appname "
                "and set RELIEFWEB_APPNAME in .env"
            ),
        )

    params = {
        "appname": app,
        "limit": min(limit, 100),
        "profile": "full",
    }
    try:
        with json_client() as client:
            resp = client.get(API_BASE, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="reliefweb",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=API_BASE,
        )

    items = data.get("data") or []
    jobs = [_normalize(item) for item in items]
    sample = jobs[0] if jobs else {}
    total = (data.get("total") or {}).get("value")
    return JobBoardFetchResult(
        source="reliefweb",
        ok=bool(jobs),
        method="reliefweb:v2/jobs",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job={k: v for k, v in sample.items() if k != "raw"},
        notes=f"API total (approx): {total}. Use offset/page for pagination.",
    )
