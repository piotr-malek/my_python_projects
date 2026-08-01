"""Map ATS-specific JSON into normalized job fields for storage."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from normalize.cleaners import strip_html_to_text


def _ms_to_iso(ms: Any) -> str | None:
    try:
        v = int(ms)
    except (TypeError, ValueError):
        return None
    dt = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat()


def _remote_from_strings(*parts: str | None) -> bool:
    blob = " ".join(p or "" for p in parts).lower()
    if "remote" in blob or "work from home" in blob or "distributed" in blob:
        return True
    return False


def normalize_greenhouse(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    loc = job.get("location") or {}
    loc_name = loc.get("name") if isinstance(loc, dict) else None
    loc_name = str(loc_name) if loc_name else ""
    title = str(job.get("title") or "")
    content_html = str(job.get("content") or "")
    desc = strip_html_to_text(content_html)
    posted = job.get("updated_at") or job.get("first_published")
    posted_s = str(posted) if posted else None
    is_remote = _remote_from_strings(loc_name, title, desc)
    jid = str(job.get("id") or "")
    return {
        "source": "greenhouse",
        "source_job_id": jid,
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "greenhouse",
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get("absolute_url") or ""),
        "location_text": loc_name or None,
        "is_remote": is_remote,
        "salary_text": None,
        "description_text": desc,
        "posted_at_hint": posted_s,
    }


def normalize_ashby(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("title") or "")
    desc = strip_html_to_text(str(job.get("descriptionHtml") or job.get("description") or ""))
    loc = job.get("location") or job.get("locationName") or ""
    loc_s = str(loc) if loc else ""
    is_remote = _remote_from_strings(title, desc, loc_s) or bool(job.get("isRemote"))
    return {
        "source": "ashby",
        "source_job_id": str(job.get("id") or job.get("jobUrl") or title),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "ashby",
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get("jobUrl") or job.get("applyUrl") or ""),
        "location_text": loc_s or None,
        "is_remote": is_remote,
        "salary_text": str(job.get("compensationTierSummary") or "") or None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("publishedAt") or "") or None,
    }


def normalize_workable(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("title") or "")
    desc = strip_html_to_text(str(job.get("description") or job.get("full_description") or ""))
    loc = job.get("location") or {}
    loc_s = ""
    if isinstance(loc, dict):
        loc_s = str(loc.get("country") or loc.get("city") or loc.get("location_str") or "")
    elif loc:
        loc_s = str(loc)
    is_remote = _remote_from_strings(title, desc, loc_s) or bool(job.get("remote"))
    return {
        "source": "workable",
        "source_job_id": str(job.get("shortcode") or job.get("id") or title),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "workable",
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get("url") or job.get("application_url") or ""),
        "location_text": loc_s or None,
        "is_remote": is_remote,
        "salary_text": None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("published") or "") or None,
    }


def normalize_recruitee(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("title") or "")
    desc = strip_html_to_text(str(job.get("description") or job.get("requirements") or ""))
    loc_s = str(job.get("location") or job.get("city") or "")
    is_remote = _remote_from_strings(title, desc, loc_s) or bool(job.get("remote"))
    return {
        "source": "recruitee",
        "source_job_id": str(job.get("id") or job.get("slug") or title),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "recruitee",
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get("careers_url") or job.get("url") or ""),
        "location_text": loc_s or None,
        "is_remote": is_remote,
        "salary_text": str(job.get("salary") or "") or None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("published_at") or "") or None,
    }


def normalize_personio(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("name") or job.get("title") or "")
    desc = strip_html_to_text(str(job.get("jobDescriptions") or job.get("description") or ""))
    loc_s = str(job.get("office") or job.get("subcompany") or "")
    is_remote = _remote_from_strings(title, desc, loc_s)
    jid = str(job.get("id") or title)
    tld = job.get("tld") or "de"
    return {
        "source": "personio",
        "source_job_id": jid,
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "personio",
        "ats_slug": ats_slug,
        "title": title,
        "url": f"https://{ats_slug}.jobs.personio.{tld}/job/{jid}",
        "location_text": loc_s or None,
        "is_remote": is_remote,
        "salary_text": None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("createdAt") or "") or None,
    }


def normalize_workday(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("title") or job.get("jobPostingTitle") or "")
    desc = strip_html_to_text(str(job.get("jobDescription") or job.get("description") or ""))
    loc = job.get("locationsText") or job.get("location") or ""
    loc_s = str(loc) if loc else ""
    is_remote = _remote_from_strings(title, desc, loc_s)
    tenant = job.get("_tenant") or ats_slug.split("|")[0]
    site = job.get("_site") or (ats_slug.split("|")[1] if "|" in ats_slug else "External")
    wd = job.get("_wd_host") or "wd1"
    ext_path = str(job.get("externalPath") or "")
    url = f"https://{wd}.myworkdayjobs.com{ext_path}" if ext_path.startswith("/") else ext_path
    return {
        "source": "workday",
        "source_job_id": str(job.get("bulletFields", [title])[0] if job.get("bulletFields") else title),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "workday",
        "ats_slug": ats_slug,
        "title": title,
        "url": url or f"https://{wd}.myworkdayjobs.com/{tenant}/{site}",
        "location_text": loc_s or None,
        "is_remote": is_remote,
        "salary_text": None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("postedOn") or "") or None,
    }


def _simple_list_normalizer(
    job: dict[str, Any],
    *,
    source: str,
    ats_type: str,
    ats_slug: str,
    company_name: str,
    mission_category: str | None,
    title_key: str = "title",
    id_key: str = "id",
    url_key: str = "url",
) -> dict[str, Any]:
    title = str(job.get(title_key) or job.get("name") or "")
    desc = strip_html_to_text(str(job.get("description") or job.get("summary") or ""))
    loc_s = str(job.get("location") or job.get("city") or "")
    return {
        "source": source,
        "source_job_id": str(job.get(id_key) or title),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": ats_type,
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get(url_key) or job.get("link") or ""),
        "location_text": loc_s or None,
        "is_remote": _remote_from_strings(title, desc, loc_s),
        "salary_text": None,
        "description_text": desc or title,
        "posted_at_hint": str(job.get("posted") or job.get("date") or "") or None,
    }


def normalize_bamboohr(job: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return _simple_list_normalizer(job, source="bamboohr", ats_type="bamboohr", **kwargs)


def normalize_breezy(job: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return _simple_list_normalizer(job, source="breezy", ats_type="breezy", title_key="name", **kwargs)


def normalize_jazzhr(job: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return _simple_list_normalizer(job, source="jazzhr", ats_type="jazzhr", **kwargs)


def normalize_teamtailor(job: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return _simple_list_normalizer(
        job, source="teamtailor", ats_type="teamtailor", title_key="title", url_key="url", **kwargs
    )


def normalize_lever(
    job: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    title = str(job.get("text") or job.get("title") or "")
    plain = job.get("descriptionPlain") or job.get("descriptionBodyPlain") or ""
    desc = str(plain).strip() or strip_html_to_text(str(job.get("description") or ""))
    wt = str(job.get("workplaceType") or "").lower()
    is_remote = wt == "remote" or _remote_from_strings(title, desc, str(job.get("country") or ""))
    salary = job.get("salaryRange")
    salary_s = None
    if isinstance(salary, dict):
        salary_s = json.dumps(salary)
    elif salary is not None:
        salary_s = str(salary)
    posted_s = _ms_to_iso(job.get("createdAt"))
    loc_parts = []
    for key in ("country", "workplaceType"):
        v = job.get(key)
        if v:
            loc_parts.append(str(v))
    categories = job.get("categories") or {}
    if isinstance(categories, dict):
        for k in ("location", "team", "department"):
            v = categories.get(k)
            if v:
                loc_parts.append(str(v))
    return {
        "source": "lever",
        "source_job_id": str(job.get("id") or ""),
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "lever",
        "ats_slug": ats_slug,
        "title": title,
        "url": str(job.get("hostedUrl") or job.get("applyUrl") or ""),
        "location_text": ", ".join(loc_parts) if loc_parts else None,
        "is_remote": is_remote,
        "salary_text": salary_s,
        "description_text": desc,
        "posted_at_hint": posted_s,
    }


def _sr_extract_sections_html(detail: dict[str, Any] | None) -> str:
    if not detail:
        return ""
    job_ad = detail.get("jobAd") or {}
    sections = job_ad.get("sections") if isinstance(job_ad, dict) else None
    if not isinstance(sections, dict):
        return ""
    chunks: list[str] = []
    for _k, block in sections.items():
        if isinstance(block, dict):
            t = block.get("text")
            if isinstance(t, str) and t.strip():
                chunks.append(t)
        elif isinstance(block, str):
            chunks.append(block)
    return "\n\n".join(chunks)


def normalize_smartrecruiters(
    bundle: dict[str, Any],
    *,
    company_name: str,
    mission_category: str | None,
    ats_slug: str,
) -> dict[str, Any]:
    item = bundle.get("list") or {}
    detail = bundle.get("detail")
    if not isinstance(item, dict):
        raise ValueError("invalid SR bundle")
    jid = str(item.get("id") or "")
    title = str(item.get("name") or "")
    loc = item.get("location") if isinstance(item.get("location"), dict) else {}
    loc = loc or {}
    loc_bits = [str(loc.get(k)) for k in ("fullLocation", "city", "country") if loc.get(k)]
    location_text = ", ".join(loc_bits) if loc_bits else None
    is_remote = bool(loc.get("remote")) if isinstance(loc, dict) else False
    if not is_remote:
        is_remote = _remote_from_strings(title, location_text)

    html_body = _sr_extract_sections_html(detail if isinstance(detail, dict) else None)
    desc = strip_html_to_text(html_body)
    if not desc:
        desc = strip_html_to_text(str(item.get("name")))

    url = ""
    if isinstance(detail, dict):
        url = str(detail.get("postingUrl") or detail.get("applyUrl") or "")
    if not url:
        url = f"https://careers.smartrecruiters.com/{ats_slug}/{jid}"

    posted = item.get("releasedDate") or (detail or {}).get("releasedDate")
    posted_s = str(posted) if posted else None

    return {
        "source": "smartrecruiters",
        "source_job_id": jid,
        "company_name": company_name,
        "mission_category": mission_category,
        "ats_type": "smartrecruiters",
        "ats_slug": ats_slug,
        "title": title,
        "url": url,
        "location_text": location_text,
        "is_remote": is_remote,
        "salary_text": None,
        "description_text": desc,
        "posted_at_hint": posted_s,
    }

