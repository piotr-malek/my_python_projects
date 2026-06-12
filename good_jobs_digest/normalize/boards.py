"""Normalize job-board listings into the shared jobs table shape."""

from __future__ import annotations

import json
from typing import Any

from normalize.cleaners import strip_html_to_text

ATS_TYPE_JOB_BOARD = "job_board"

BOARD_CLIMATEBASE = "climatebase"
BOARD_80000HOURS = "80000hours"
BOARD_ESCAPETHECITY = "escapethecity"
BOARD_TECHJOBSFORGOOD = "techjobsforgood"
BOARD_RELIEFWEB = "reliefweb"

BOARD_SOURCES = (
    BOARD_CLIMATEBASE,
    BOARD_80000HOURS,
    BOARD_ESCAPETHECITY,
    BOARD_TECHJOBSFORGOOD,
    BOARD_RELIEFWEB,
)

BOARD_DISPLAY_NAMES = {
    BOARD_CLIMATEBASE: "Climatebase",
    BOARD_80000HOURS: "80,000 Hours",
    BOARD_ESCAPETHECITY: "Escape the City",
    BOARD_TECHJOBSFORGOOD: "Tech Jobs for Good",
    BOARD_RELIEFWEB: "ReliefWeb",
}


def _remote_from_strings(*parts: str | None) -> bool:
    blob = " ".join(p or "" for p in parts).lower()
    return any(k in blob for k in ("remote", "work from home", "distributed", "wfh"))


def _base(
    *,
    board: str,
    source_job_id: str,
    title: str,
    company_name: str,
    url: str,
    description_text: str,
    location_text: str | None = None,
    is_remote: bool = False,
    salary_text: str | None = None,
    mission_category: str | None = None,
    posted_at_hint: str | None = None,
) -> dict[str, Any]:
    return {
        "source": board,
        "source_job_id": source_job_id,
        "company_name": company_name or "Unknown",
        "mission_category": mission_category or board,
        "ats_type": ATS_TYPE_JOB_BOARD,
        "ats_slug": board,
        "title": title or "(untitled)",
        "url": url or "",
        "location_text": location_text,
        "is_remote": is_remote,
        "salary_text": salary_text,
        "description_text": description_text or title or "",
        "posted_at_hint": posted_at_hint,
    }


def normalize_climatebase_listing(
    listing: dict[str, Any],
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    jid = str(listing.get("id") or "")
    title = str(listing.get("title") or "")
    employer = str(listing.get("employer_name") or listing.get("name_of_employer") or "")
    locs = listing.get("locations")
    if isinstance(locs, list):
        location = "; ".join(str(x) for x in locs)
    else:
        location = str(locs) if locs else None
    remote_prefs = listing.get("remote_preferences") or []
    is_remote = _remote_from_strings(
        location,
        title,
        " ".join(str(x) for x in remote_prefs) if isinstance(remote_prefs, list) else str(remote_prefs),
    )
    salary_parts = []
    if listing.get("salary_from"):
        salary_parts.append(str(listing["salary_from"]))
    if listing.get("salary_to"):
        salary_parts.append(str(listing["salary_to"]))
    if listing.get("salary_period"):
        salary_parts.append(str(listing["salary_period"]))
    salary = " – ".join(salary_parts) if salary_parts else None

    desc = ""
    if detail:
        desc = strip_html_to_text(
            str(detail.get("sanitized_description") or detail.get("description") or "")
        )
        if detail.get("employer_name"):
            employer = str(detail["employer_name"])
    if not desc and listing.get("employer_short_description"):
        desc = str(listing["employer_short_description"])

    sectors = listing.get("sectors")
    mission = sectors[0] if isinstance(sectors, list) and sectors else "climate"

    return _base(
        board=BOARD_CLIMATEBASE,
        source_job_id=jid,
        title=title,
        company_name=employer,
        url=str(listing.get("url") or f"https://climatebase.org/job/{jid}"),
        description_text=desc,
        location_text=location or None,
        is_remote=is_remote,
        salary_text=salary,
        mission_category=str(mission),
        posted_at_hint=str(listing.get("activation_date") or "") or None,
    )


def normalize_80000hours(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or "")
    title = str(job.get("title") or "")
    company = str(job.get("company_name") or "")
    desc = strip_html_to_text(
        str(job.get("description") or job.get("description_short") or "")
    )
    locs = job.get("locations")
    if isinstance(locs, list):
        location = "; ".join(str(x) for x in locs)
    else:
        location = None
    remote_tags = job.get("remote")
    is_remote = _remote_from_strings(
        location,
        title,
        desc,
        json.dumps(remote_tags) if remote_tags else "",
    )
    return _base(
        board=BOARD_80000HOURS,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or ""),
        description_text=desc,
        location_text=location,
        is_remote=is_remote,
        salary_text=str(job["salary"]) if job.get("salary") else None,
        mission_category="effective_altruism",
        posted_at_hint=str(job.get("posted_at") or "") or None,
    )


def normalize_escapethecity(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or job.get("slug") or "")
    title = str(job.get("title") or "")
    company = str(job.get("company_name") or "")
    desc = strip_html_to_text(str(job.get("description") or job.get("headline") or ""))
    location = str(job.get("location") or "") or None
    remote = job.get("remote")
    is_remote = _remote_from_strings(
        location,
        title,
        desc,
        json.dumps(remote) if isinstance(remote, list) else str(remote or ""),
    )
    salary = None
    if job.get("salary_low") or job.get("salary_max"):
        salary = f"{job.get('salary_low', '')} – {job.get('salary_max', '')}".strip(" –")
    return _base(
        board=BOARD_ESCAPETHECITY,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or ""),
        description_text=desc,
        location_text=location,
        is_remote=is_remote,
        salary_text=salary,
        mission_category="impact",
        posted_at_hint=str(job.get("posted_date") or job.get("updated_at") or "") or None,
    )


def normalize_techjobsforgood(
    listing: dict[str, Any],
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    jid = str(listing.get("id") or "")
    title = str(listing.get("title") or "")
    company = str(listing.get("company_name") or "")
    location = str(listing.get("location") or "") or None
    desc = ""
    if detail:
        desc = str(detail.get("text") or detail.get("meta_description") or "")
    if not desc and listing.get("card_text"):
        desc = "\n".join(str(x) for x in listing["card_text"])
    is_remote = _remote_from_strings(location, title, desc)
    return _base(
        board=BOARD_TECHJOBSFORGOOD,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(listing.get("url") or f"https://techjobsforgood.com/jobs/{jid}/"),
        description_text=desc,
        location_text=location,
        is_remote=is_remote,
        salary_text=None,
        mission_category="tech_for_good",
    )


def normalize_reliefweb(item: dict[str, Any]) -> dict[str, Any]:
    fields = item.get("fields") if isinstance(item.get("fields"), dict) else item
    jid = str(item.get("id") or fields.get("id") or "")
    title = str(fields.get("title") or "")
    body = strip_html_to_text(str(fields.get("body-html") or fields.get("body") or ""))
    source = (fields.get("source") or [{}])[0] if fields.get("source") else {}
    org = str(source.get("name") or source.get("shortname") or "ReliefWeb")
    countries = fields.get("country") or []
    loc = ", ".join(
        c.get("name", "") for c in countries if isinstance(c, dict) and c.get("name")
    )
    url = str(fields.get("url") or "")
    date_created = (fields.get("date") or {}).get("created") if isinstance(fields.get("date"), dict) else None
    return _base(
        board=BOARD_RELIEFWEB,
        source_job_id=jid,
        title=title,
        company_name=org,
        url=url,
        description_text=body,
        location_text=loc or None,
        is_remote=_remote_from_strings(body, title),
        salary_text=None,
        mission_category="humanitarian",
        posted_at_hint=str(date_created) if date_created else None,
    )
