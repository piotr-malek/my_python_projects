"""Load existing curated_companies rows for website-first revalidation."""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import httpx

from config import Settings
from core.curated_registry import load_curated_records
from discovery.ats_registry import parse_ats_from_text
from discovery.resolve import EmployerCandidate
from storage.bq_repository import JobBigQuery

logger = logging.getLogger(__name__)

_ATS_HOST_MARKERS = (
    "greenhouse.io",
    "lever.co",
    "smartrecruiters.com",
    "ashbyhq.com",
    "workable.com",
    "recruitee.com",
    "personio",
    "myworkdayjobs.com",
    "bamboohr.com",
    "breezy.hr",
    "applytojob.com",
    "teamtailor.com",
)


def _is_ats_host(url: str) -> bool:
    try:
        host = urlparse(url if "://" in url else f"https://{url}").netloc.lower()
    except Exception:  # noqa: BLE001
        return False
    return any(marker in host for marker in _ATS_HOST_MARKERS)


def _website_from_row_urls(row: dict[str, str]) -> str:
    for field in ("careers_url", "job_board_url"):
        url = (row.get(field) or "").strip()
        if not url or _is_ats_host(url):
            continue
        parsed = urlparse(url if "://" in url else f"https://{url}")
        if parsed.netloc:
            scheme = parsed.scheme or "https"
            return f"{scheme}://{parsed.netloc}"
    return ""


def greenhouse_board_website(client: httpx.Client, slug: str) -> str:
    """Best-effort company homepage from Greenhouse board metadata."""
    slug = slug.strip()
    if not slug:
        return ""
    try:
        resp = client.get(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}",
            timeout=12.0,
        )
    except httpx.RequestError:
        return ""
    if resp.status_code != 200:
        return ""
    data = resp.json()
    for key in ("website", "company_url", "url"):
        raw = str(data.get(key) or "").strip()
        if raw and not _is_ats_host(raw):
            return raw if "://" in raw else f"https://{raw}"
    return ""


def enrich_candidate_website(client: httpx.Client, candidate: EmployerCandidate) -> None:
    """Fill candidate.website from Greenhouse metadata when missing."""
    if candidate.website.strip():
        return
    hint = candidate.ats_hint
    if hint and hint[0].lower() == "greenhouse":
        website = greenhouse_board_website(client, hint[1])
        if website:
            candidate.website = website


def collect_curated_revalidate(
    settings: Settings,
    bq: JobBigQuery | None = None,
    *,
    resolve_greenhouse_websites: bool = True,
) -> list[EmployerCandidate]:
    """Turn registry rows into candidates for website-first ATS revalidation."""
    rows = load_curated_records(settings, bq)
    out: list[EmployerCandidate] = []
    client: httpx.Client | None = None
    if resolve_greenhouse_websites:
        client = httpx.Client(timeout=12.0, follow_redirects=True)

    try:
        for row in rows:
            name = (row.get("company_name") or "").strip()
            if not name:
                continue
            board_url = (row.get("careers_url") or row.get("job_board_url") or "").strip()
            ats_type = (row.get("ats_type") or "").strip().lower()
            ats_slug = (row.get("ats_slug") or "").strip()
            if board_url and (not ats_type or not ats_slug):
                parsed = parse_ats_from_text(board_url)
                if parsed:
                    ats_type, ats_slug = parsed
            website = _website_from_row_urls(row)
            hint = (ats_type, ats_slug) if ats_type and ats_slug else None
            cand = EmployerCandidate(
                company_name=name,
                mission_category=(row.get("mission_category") or "mission").strip(),
                website=website,
                discovery_source="curated_revalidate",
                ats_hint=hint,
            )
            if client is not None and not website and ats_type == "greenhouse":
                enrich_candidate_website(client, cand)
            out.append(cand)
    finally:
        if client is not None:
            client.close()

    logger.info("Curated revalidate queue: %s employers from registry", len(out))
    return sorted(out, key=lambda c: c.company_name.lower())
