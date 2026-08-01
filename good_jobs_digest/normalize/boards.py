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
BOARD_IDEALIST = "idealist"
BOARD_IMPACTPOOL = "impactpool"
BOARD_AAC = "animaladvocacycareers"
BOARD_WORKONCLIMATE = "workonclimate"
BOARD_REMOTIVE = "remotive"
BOARD_ARBEITNOW = "arbeitnow"
BOARD_JOBICY = "jobicy"
BOARD_HIMALAYAS = "himalayas"
BOARD_REMOTEOK = "remoteok"
BOARD_WEWORKREMOTELY = "weworkremotely"
BOARD_WORKINGNOMADS = "workingnomads"
BOARD_HN_WHOISHIRING = "hn_whoishiring"
BOARD_INDEED = "indeed"

BOARD_SOURCES = (
    BOARD_CLIMATEBASE,
    BOARD_80000HOURS,
    BOARD_ESCAPETHECITY,
    BOARD_TECHJOBSFORGOOD,
    BOARD_RELIEFWEB,
    BOARD_IDEALIST,
    BOARD_IMPACTPOOL,
    BOARD_AAC,
    BOARD_WORKONCLIMATE,
    BOARD_REMOTIVE,
    BOARD_ARBEITNOW,
    BOARD_JOBICY,
    BOARD_HIMALAYAS,
    BOARD_REMOTEOK,
    BOARD_WEWORKREMOTELY,
    BOARD_WORKINGNOMADS,
    BOARD_HN_WHOISHIRING,
    BOARD_INDEED,
)

BOARD_DISPLAY_NAMES = {
    BOARD_CLIMATEBASE: "Climatebase",
    BOARD_80000HOURS: "80,000 Hours",
    BOARD_ESCAPETHECITY: "Escape the City",
    BOARD_TECHJOBSFORGOOD: "Tech Jobs for Good",
    BOARD_RELIEFWEB: "ReliefWeb",
    BOARD_IDEALIST: "Idealist",
    BOARD_IMPACTPOOL: "Impactpool",
    BOARD_AAC: "Animal Advocacy Careers",
    BOARD_WORKONCLIMATE: "Work on Climate",
    BOARD_REMOTIVE: "Remotive",
    BOARD_ARBEITNOW: "Arbeitnow",
    BOARD_JOBICY: "Jobicy",
    BOARD_HIMALAYAS: "Himalayas",
    BOARD_REMOTEOK: "Remote OK",
    BOARD_WEWORKREMOTELY: "We Work Remotely",
    BOARD_WORKINGNOMADS: "Working Nomads",
    BOARD_HN_WHOISHIRING: "HN Who is hiring",
    BOARD_INDEED: "Indeed",
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
    themes = fields.get("theme") or []
    theme_names = ", ".join(
        t.get("name", "") for t in themes if isinstance(t, dict) and t.get("name")
    )
    career_cats = fields.get("career-category") or []
    career_text = ", ".join(
        c.get("name", "") for c in career_cats if isinstance(c, dict) and c.get("name")
    )
    org_type = str(source.get("type") or "")
    url = str(fields.get("url") or "")
    date_created = (fields.get("date") or {}).get("created") if isinstance(fields.get("date"), dict) else None
    desc_parts = [body]
    if theme_names:
        desc_parts.append(f"Themes: {theme_names}")
    if career_text:
        desc_parts.append(f"Career categories: {career_text}")
    if org_type:
        desc_parts.append(f"Org type: {org_type}")
    return _base(
        board=BOARD_RELIEFWEB,
        source_job_id=jid,
        title=title,
        company_name=org,
        url=url,
        description_text="\n".join(desc_parts),
        location_text=loc or None,
        is_remote=_remote_from_strings(body, title),
        salary_text=None,
        mission_category="humanitarian",
        posted_at_hint=str(date_created) if date_created else None,
    )


def normalize_html_board(job: dict[str, Any], *, board: str) -> dict[str, Any]:
    return _base(
        board=board,
        source_job_id=str(job.get("id") or ""),
        title=str(job.get("title") or ""),
        company_name=str(job.get("company_name") or "Unknown"),
        url=str(job.get("url") or ""),
        description_text=str(job.get("description") or job.get("title") or ""),
        location_text=str(job.get("location") or "") or None,
        is_remote=_remote_from_strings(str(job.get("location")), str(job.get("title"))),
        salary_text=None,
        mission_category=str(job.get("mission_category") or board),
    )


def normalize_idealist(job: dict[str, Any]) -> dict[str, Any]:
    return normalize_html_board(job, board=BOARD_IDEALIST)


def normalize_impactpool(job: dict[str, Any]) -> dict[str, Any]:
    return normalize_html_board(job, board=BOARD_IMPACTPOOL)


def normalize_animaladvocacycareers(job: dict[str, Any]) -> dict[str, Any]:
    return normalize_html_board(job, board=BOARD_AAC)


def normalize_workonclimate(job: dict[str, Any]) -> dict[str, Any]:
    return normalize_html_board(job, board=BOARD_WORKONCLIMATE)


def normalize_remotive(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or "")
    title = str(job.get("title") or "")
    company = str(job.get("company_name") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    required = str(job.get("candidate_required_location") or "").strip()
    category = str(job.get("category") or "")
    tags = job.get("tags") or []
    tag_text = ", ".join(str(t) for t in tags) if isinstance(tags, list) else str(tags)
    loc_parts = [p for p in (required, category) if p]
    location = "; ".join(loc_parts) if loc_parts else None
    # Remotive listings are remote by definition; pass required location through for LLM.
    extra = []
    if required:
        extra.append(f"candidate_required_location: {required}")
    if tag_text:
        extra.append(f"tags: {tag_text}")
    if job.get("job_type"):
        extra.append(f"job_type: {job.get('job_type')}")
    desc_full = "\n".join([desc] + extra).strip()
    return _base(
        board=BOARD_REMOTIVE,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or ""),
        description_text=desc_full or title,
        location_text=location,
        is_remote=True,
        salary_text=str(job.get("salary") or "") or None,
        mission_category="remote",
        posted_at_hint=str(job.get("publication_date") or "") or None,
    )


def normalize_arbeitnow(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("slug") or job.get("url") or "")
    title = str(job.get("title") or "")
    company = str(job.get("company_name") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    location = str(job.get("location") or "") or None
    is_remote = bool(job.get("remote")) or _remote_from_strings(location, title)
    tags = job.get("tags") or []
    job_types = job.get("job_types") or []
    extras = []
    if isinstance(tags, list) and tags:
        extras.append("tags: " + ", ".join(str(t) for t in tags))
    if isinstance(job_types, list) and job_types:
        extras.append("job_types: " + ", ".join(str(t) for t in job_types))
    if job.get("remote") is not None:
        extras.append(f"remote_flag: {bool(job.get('remote'))}")
    desc_full = "\n".join([desc] + extras).strip()
    return _base(
        board=BOARD_ARBEITNOW,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or ""),
        description_text=desc_full or title,
        location_text=location,
        is_remote=is_remote,
        salary_text=None,
        mission_category="eu_jobs",
        posted_at_hint=str(job.get("created_at") or "") or None,
    )


def normalize_jobicy(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or job.get("jobSlug") or "")
    title = str(job.get("jobTitle") or job.get("title") or "")
    company = str(job.get("companyName") or job.get("company_name") or "")
    desc = strip_html_to_text(
        str(job.get("jobDescription") or job.get("jobExcerpt") or job.get("description") or "")
    )
    geo = str(job.get("jobGeo") or "") or None
    level = str(job.get("jobLevel") or "")
    industry = job.get("jobIndustry") or []
    ind_text = ", ".join(str(x) for x in industry) if isinstance(industry, list) else str(industry)
    extras = []
    if geo:
        extras.append(f"jobGeo: {geo}")
    if level:
        extras.append(f"jobLevel: {level}")
    if ind_text:
        extras.append(f"industry: {ind_text}")
    desc_full = "\n".join([desc] + extras).strip()
    return _base(
        board=BOARD_JOBICY,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or ""),
        description_text=desc_full or title,
        location_text=geo,
        is_remote=True,  # Jobicy lists remote roles only
        salary_text=None,
        mission_category="remote_emea",
        posted_at_hint=str(job.get("pubDate") or "") or None,
    )


def normalize_himalayas(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(
        job.get("guid")
        or job.get("id")
        or f"{job.get('companySlug') or ''}-{job.get('title') or ''}"
    )
    title = str(job.get("title") or "")
    company = str(job.get("companyName") or "")
    desc = strip_html_to_text(str(job.get("description") or job.get("excerpt") or ""))
    restrictions = job.get("locationRestrictions") or []
    if isinstance(restrictions, list):
        loc = "; ".join(str(x) for x in restrictions)
    else:
        loc = str(restrictions or "")
    tz = job.get("timezoneRestrictions") or []
    tz_text = ", ".join(str(x) for x in tz) if isinstance(tz, list) else str(tz or "")
    extras = []
    if loc:
        extras.append(f"locationRestrictions: {loc}")
    if tz_text:
        extras.append(f"timezoneRestrictions: {tz_text}")
    if job.get("employmentType"):
        extras.append(f"employmentType: {job.get('employmentType')}")
    seniority = job.get("seniority") or []
    if isinstance(seniority, list) and seniority:
        extras.append("seniority: " + ", ".join(str(s) for s in seniority))
    desc_full = "\n".join([desc] + extras).strip()
    salary = None
    if job.get("minSalary") or job.get("maxSalary"):
        currency = job.get("currency") or ""
        salary = f"{job.get('minSalary', '')}–{job.get('maxSalary', '')} {currency}".strip()
    return _base(
        board=BOARD_HIMALAYAS,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("applicationLink") or job.get("url") or ""),
        description_text=desc_full or title,
        location_text=loc or None,
        is_remote=True,
        salary_text=salary,
        mission_category="remote",
        posted_at_hint=str(job.get("pubDate") or "") or None,
    )


def normalize_remoteok(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or job.get("slug") or "")
    title = str(job.get("position") or "")
    company = str(job.get("company") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    location = str(job.get("location") or "").strip(" ,")
    tags = job.get("tags") or []
    tag_text = ", ".join(str(t) for t in tags) if isinstance(tags, list) else str(tags)
    extras = []
    if location:
        extras.append(f"location: {location}")
    if tag_text:
        extras.append(f"tags: {tag_text}")
    # salary_min/max are ints that are 0 when unknown.
    salary = None
    smin, smax = job.get("salary_min") or 0, job.get("salary_max") or 0
    if smin or smax:
        salary = f"{smin}–{smax}"
    return _base(
        board=BOARD_REMOTEOK,
        source_job_id=jid,
        title=title,
        company_name=company,
        url=str(job.get("url") or job.get("apply_url") or ""),
        description_text="\n".join([desc] + extras).strip() or title,
        location_text=location or None,
        is_remote=True,
        salary_text=salary,
        mission_category="remote",
        posted_at_hint=str(job.get("date") or job.get("epoch") or "") or None,
    )


def normalize_weworkremotely(job: dict[str, Any]) -> dict[str, Any]:
    raw_title = str(job.get("title") or "")
    # WWR titles are "Company: Job Title".
    company, _, role = raw_title.partition(":")
    if role.strip():
        company_name, title = company.strip(), role.strip()
    else:
        company_name, title = "", raw_title.strip()
    link = str(job.get("link") or job.get("guid") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    region = str(job.get("region") or "")
    country = str(job.get("country") or "")
    state = str(job.get("state") or "")
    skills = str(job.get("skills") or "")
    extras = []
    for label, value in (
        ("region", region),
        ("hireable countries", country),
        ("state", state),
        ("skills", skills),
        ("type", str(job.get("type") or "")),
    ):
        if value:
            extras.append(f"{label}: {value}")
    location = "; ".join(p for p in (region, state, country) if p) or None
    return _base(
        board=BOARD_WEWORKREMOTELY,
        source_job_id=link,
        title=title,
        company_name=company_name or "Unknown",
        url=link,
        description_text="\n".join([desc] + extras).strip() or title,
        location_text=location,
        is_remote=True,
        salary_text=None,
        mission_category="remote",
        posted_at_hint=str(job.get("pubDate") or "") or None,
    )


def normalize_workingnomads(job: dict[str, Any]) -> dict[str, Any]:
    url = str(job.get("url") or "")
    title = str(job.get("title") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    location = str(job.get("location") or "")
    tags = str(job.get("tags") or "")
    extras = []
    if location:
        # Often states the timezone requirement outright, e.g. "Time zone: CET (+/- 3 hours)".
        extras.append(f"location: {location}")
    if tags:
        extras.append(f"tags: {tags}")
    if job.get("category_name"):
        extras.append(f"category: {job.get('category_name')}")
    return _base(
        board=BOARD_WORKINGNOMADS,
        source_job_id=url,
        title=title,
        company_name=str(job.get("company_name") or ""),
        url=url,
        description_text="\n".join([desc] + extras).strip() or title,
        location_text=location or None,
        is_remote=True,
        salary_text=None,
        mission_category="remote",
        posted_at_hint=str(job.get("pub_date") or "") or None,
    )


def normalize_hn_whoishiring(job: dict[str, Any]) -> dict[str, Any]:
    """One HN comment = one posting. Convention is "Company | Role | Location | REMOTE"."""
    parts = job.get("parts") or []
    text = str(job.get("text") or "")
    first_line = str(job.get("first_line") or "")
    company = str(parts[0]) if parts else ""
    # The role is usually the next pipe field; fall back to the whole first line
    # so the title gate still gets something to judge.
    title = str(parts[1]) if len(parts) > 1 else first_line
    location = " | ".join(str(p) for p in parts[2:5]) if len(parts) > 2 else None
    return _base(
        board=BOARD_HN_WHOISHIRING,
        source_job_id=str(job.get("id") or ""),
        title=title,
        company_name=company or "Unknown",
        url=f"https://news.ycombinator.com/item?id={job.get('id')}",
        description_text=text or title,
        location_text=location,
        is_remote=_remote_from_strings(first_line, text[:500]),
        salary_text=None,
        mission_category="hn",
        posted_at_hint=str(job.get("created_at") or "") or None,
    )


def normalize_indeed(job: dict[str, Any]) -> dict[str, Any]:
    jid = str(job.get("id") or job.get("job_url") or "")
    desc = strip_html_to_text(str(job.get("description") or ""))
    location = str(job.get("location") or "")
    extras = []
    if location:
        extras.append(f"location: {location}")
    if job.get("is_remote") is not None:
        extras.append(f"is_remote: {job.get('is_remote')}")
    if job.get("job_type"):
        extras.append(f"job_type: {job.get('job_type')}")
    if job.get("search_country"):
        extras.append(f"searched in: {job.get('search_country')}")
    salary = None
    if job.get("min_amount") or job.get("max_amount"):
        salary = (
            f"{job.get('min_amount') or ''}–{job.get('max_amount') or ''} "
            f"{job.get('currency') or ''} {job.get('interval') or ''}"
        ).strip()
    return _base(
        board=BOARD_INDEED,
        source_job_id=jid,
        title=str(job.get("title") or ""),
        company_name=str(job.get("company") or ""),
        url=str(job.get("job_url_direct") or job.get("job_url") or ""),
        description_text="\n".join([desc] + extras).strip() or str(job.get("title") or ""),
        location_text=location or None,
        is_remote=bool(job.get("is_remote")),
        salary_text=salary,
        mission_category="jobboard",
        posted_at_hint=str(job.get("date_posted") or "") or None,
    )
