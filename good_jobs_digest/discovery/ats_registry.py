"""Central ATS definitions: URL parsing, probe order, board probes, careers URLs."""

from __future__ import annotations

import json
import re
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

import httpx

# High-ROI ATS types for optional --fast mode (subset only).
FAST_PROBE_ATS: frozenset[str] = frozenset({"ashby", "greenhouse", "lever", "smartrecruiters"})

# Wave probing: all ATS types, but fast 404 batch before rate-limited/slow backends.
ATS_PROBE_BATCHES: tuple[tuple[str, ...], ...] = (
    ("ashby", "greenhouse", "lever", "smartrecruiters"),
    ("workable", "recruitee", "personio"),
    ("workday", "bamboohr", "breezy", "jazzhr", "teamtailor"),
)

_WORKABLE_SEM = threading.Semaphore(2)
_BULK_PROBE_ENABLED = False

# Tier 1 first (highest ROI), then legacy GH/Lever/SR, then tier 2.
ATS_PROBE_ORDER: tuple[str, ...] = (
    "ashby",
    "greenhouse",
    "lever",
    "smartrecruiters",
    "workable",
    "recruitee",
    "personio",
    "workday",
    "bamboohr",
    "breezy",
    "jazzhr",
    "teamtailor",
)

CURATED_ATS_TYPES: frozenset[str] = frozenset(ATS_PROBE_ORDER)

TIER1_ATS: frozenset[str] = frozenset({"ashby", "workable", "recruitee", "personio"})
TIER2_ATS: frozenset[str] = frozenset(
    {"workday", "bamboohr", "breezy", "jazzhr", "teamtailor"}
)

_URL_PATTERNS: list[tuple[re.Pattern[str], str, int]] = [
    (re.compile(r"jobs\.ashbyhq\.com/([^/?#\s\"']+)", re.I), "ashby", 1),
    (re.compile(r"api\.ashbyhq\.com/posting-api/job-board/([^/?#\s\"']+)", re.I), "ashby", 1),
    (
        re.compile(
            r"(?:boards(?:-api)?\.greenhouse\.io|job-boards\.greenhouse\.io)/([^/?#\s\"']+)",
            re.I,
        ),
        "greenhouse",
        1,
    ),
    (re.compile(r"jobs\.lever\.co/([^/?#\s\"']+)", re.I), "lever", 1),
    (
        re.compile(
            r"(?:careers\.smartrecruiters\.com|api\.smartrecruiters\.com/v1/companies)/([^/?#\s\"']+)",
            re.I,
        ),
        "smartrecruiters",
        1,
    ),
    (re.compile(r"(?:[\w-]+\.)?apply\.workable\.com/([^/?#\s\"']+)", re.I), "workable", 1),
    (re.compile(r"([\w-]+)\.recruitee\.com", re.I), "recruitee", 1),
    (re.compile(r"([\w-]+)\.jobs\.personio\.(?:de|com)", re.I), "personio", 1),
    (
        re.compile(r"wd\d*\.myworkdayjobs\.com/(?:wday/cxs/)?([^/]+)/([^/?#\s\"']+)", re.I),
        "workday",
        0,
    ),
    (re.compile(r"([\w-]+)\.bamboohr\.com/careers", re.I), "bamboohr", 1),
    (re.compile(r"([\w-]+)\.breezy\.hr", re.I), "breezy", 1),
    (re.compile(r"([\w-]+)\.applytojob\.com", re.I), "jazzhr", 1),
    (re.compile(r"([\w-]+)\.teamtailor\.com", re.I), "teamtailor", 1),
]


@dataclass(frozen=True)
class BoardProbeResult:
    has_jobs: bool
    board_name: str = ""
    extra: dict[str, Any] | None = None


def set_bulk_probe(enabled: bool) -> None:
    """Enable shorter retries for high-volume registry builds."""
    global _BULK_PROBE_ENABLED
    _BULK_PROBE_ENABLED = enabled


def is_bulk_probe() -> bool:
    return _BULK_PROBE_ENABLED


def probe_batches_for(order: list[str]) -> list[list[str]]:
    """Split an ATS probe order into waves (full coverage, better latency)."""
    batches: list[list[str]] = []
    for batch in ATS_PROBE_BATCHES:
        wave = [ats for ats in batch if ats in order]
        if wave:
            batches.append(wave)
    covered = {ats for wave in batches for ats in wave}
    remainder = [ats for ats in order if ats not in covered]
    if remainder:
        batches.append(remainder)
    return batches


def _get(
    client: httpx.Client, url: str, *, retries: int | None = None, follow_redirects: bool | None = None
) -> httpx.Response:
    if retries is None:
        retries = 0 if is_bulk_probe() else 2
    kwargs: dict[str, Any] = {}
    if follow_redirects is not None:
        kwargs["follow_redirects"] = follow_redirects
    for attempt in range(retries + 1):
        try:
            r = client.get(url, **kwargs)
        except httpx.RequestError:
            if attempt >= retries:
                raise
            time.sleep(0.25 * (attempt + 1) if is_bulk_probe() else 0.5 * (attempt + 1))
            continue
        if r.status_code == 429 and attempt < retries:
            time.sleep(0.35 * (attempt + 1) if is_bulk_probe() else 1.0 * (attempt + 1))
            continue
        return r
    return r  # unreachable


def _post(client: httpx.Client, url: str, *, json_body: dict, retries: int | None = None) -> httpx.Response:
    if retries is None:
        retries = 0 if is_bulk_probe() else 2
    for attempt in range(retries + 1):
        try:
            r = client.post(url, json=json_body)
        except httpx.RequestError:
            if attempt >= retries:
                raise
            time.sleep(0.25 * (attempt + 1) if is_bulk_probe() else 0.5 * (attempt + 1))
            continue
        if r.status_code == 429 and attempt < retries:
            time.sleep(0.35 * (attempt + 1) if is_bulk_probe() else 1.0 * (attempt + 1))
            continue
        return r
    return r


def parse_ats_from_text(text: str) -> tuple[str, str] | None:
    """Return (ats_type, slug) from a URL or HTML blob."""
    if not text:
        return None
    for pattern, ats, group in _URL_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        if ats == "workday":
            tenant = m.group(1).strip()
            site = m.group(2).strip()
            if tenant and site:
                return "workday", f"{tenant}|{site}"
            continue
        slug = m.group(group).split("/")[0].strip()
        if slug:
            return ats, slug
    return None


def careers_url(ats_type: str, slug: str, *, region: str = "global") -> str:
    ats = ats_type.lower()
    if ats == "ashby":
        return f"https://jobs.ashbyhq.com/{slug}"
    if ats == "greenhouse":
        return f"https://boards.greenhouse.io/{slug}"
    if ats == "lever":
        return f"https://jobs.lever.co/{slug}"
    if ats == "smartrecruiters":
        return f"https://careers.smartrecruiters.com/{slug}"
    if ats == "workable":
        return f"https://apply.workable.com/{slug}/"
    if ats == "recruitee":
        return f"https://{slug}.recruitee.com/"
    if ats == "personio":
        return f"https://{slug}.jobs.personio.de/"
    if ats == "workday":
        tenant, site = _split_workday_slug(slug)
        return f"https://wd1.myworkdayjobs.com/{tenant}/{site}"
    if ats == "bamboohr":
        return f"https://{slug}.bamboohr.com/careers"
    if ats == "breezy":
        return f"https://{slug}.breezy.hr"
    if ats == "jazzhr":
        return f"https://{slug}.applytojob.com"
    if ats == "teamtailor":
        return f"https://{slug}.teamtailor.com/jobs"
    return ""


def _split_workday_slug(slug: str) -> tuple[str, str]:
    parts = slug.split("|", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return slug, "External"


def probe_board(
    client: httpx.Client,
    ats_type: str,
    slug: str,
    *,
    region: str = "global",
) -> BoardProbeResult:
    ats = ats_type.lower()
    probes = {
        "ashby": _probe_ashby,
        "greenhouse": _probe_greenhouse,
        "smartrecruiters": _probe_smartrecruiters,
        "lever": lambda c, s: _probe_lever(c, s, region=region),
        "workable": _probe_workable,
        "recruitee": _probe_recruitee,
        "personio": _probe_personio,
        "workday": _probe_workday,
        "bamboohr": _probe_bamboohr,
        "breezy": _probe_breezy,
        "jazzhr": _probe_jazzhr,
        "teamtailor": _probe_teamtailor,
    }
    fn = probes.get(ats)
    if not fn:
        return BoardProbeResult(has_jobs=False)
    return fn(client, slug)


def _probe_ashby(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    data = r.json()
    jobs = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs, list):
        return BoardProbeResult(has_jobs=False)
    listed = [j for j in jobs if j.get("isListed") is not False]
    name = str(data.get("organizationName") or data.get("name") or "")
    return BoardProbeResult(has_jobs=len(listed) > 0, board_name=name)


def _probe_greenhouse(client: httpx.Client, slug: str) -> BoardProbeResult:
    jobs_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=false"
    try:
        r = _get(client, jobs_url)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    jobs = r.json().get("jobs")
    if not isinstance(jobs, list) or not jobs:
        return BoardProbeResult(has_jobs=False)
    board_name = ""
    try:
        meta = _get(client, f"https://boards-api.greenhouse.io/v1/boards/{slug}")
        if meta.status_code == 200:
            board_name = str(meta.json().get("name") or "")
    except httpx.RequestError:
        pass
    return BoardProbeResult(has_jobs=True, board_name=board_name)


def _probe_smartrecruiters(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    if int(r.json().get("totalFound") or 0) <= 0:
        return BoardProbeResult(has_jobs=False)
    name = ""
    try:
        ident = _get(client, f"https://api.smartrecruiters.com/v1/companies/{slug}")
        if ident.status_code == 200:
            name = str(ident.json().get("name") or "")
    except httpx.RequestError:
        pass
    return BoardProbeResult(has_jobs=True, board_name=name)


def _probe_lever(client: httpx.Client, slug: str, *, region: str = "global") -> BoardProbeResult:
    base = "https://api.eu.lever.co" if region == "eu" else "https://api.lever.co"
    url = f"{base}/v0/postings/{slug}?mode=json&limit=1"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    data = r.json()
    return BoardProbeResult(has_jobs=isinstance(data, list) and len(data) > 0)


def _probe_workable(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://apply.workable.com/api/v3/accounts/{slug}/jobs"
    with _WORKABLE_SEM:
        try:
            r = _get(client, url, follow_redirects=False)
        except httpx.RequestError:
            return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    data = r.json()
    jobs = data.get("jobs") if isinstance(data, dict) else data
    if isinstance(jobs, list) and jobs:
        name = str(data.get("name") or "") if isinstance(data, dict) else ""
        return BoardProbeResult(has_jobs=True, board_name=name)
    return BoardProbeResult(has_jobs=False)


def _probe_recruitee(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://{slug}.recruitee.com/api/offers/"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    data = r.json()
    offers = data.get("offers") if isinstance(data, dict) else None
    if isinstance(offers, list) and offers:
        return BoardProbeResult(has_jobs=True, board_name=str(data.get("name") or ""))
    return BoardProbeResult(has_jobs=False)


def _probe_personio(client: httpx.Client, slug: str) -> BoardProbeResult:
    for tld in ("de", "com"):
        url = f"https://{slug}.jobs.personio.{tld}/xml"
        try:
            r = _get(client, url, follow_redirects=False)
        except httpx.RequestError:
            continue
        if r.status_code != 200 or not r.text.strip():
            continue
        try:
            root = ET.fromstring(r.text)
        except ET.ParseError:
            continue
        positions = root.findall(".//position")
        if positions:
            return BoardProbeResult(has_jobs=True, extra={"tld": tld})
    return BoardProbeResult(has_jobs=False)


def _probe_workday(client: httpx.Client, slug: str) -> BoardProbeResult:
    tenant, site = _split_workday_slug(slug)
    for wd in ("wd1", "wd3", "wd5"):
        url = f"https://{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
        body = {"appliedFacets": {}, "limit": 5, "offset": 0, "searchText": ""}
        try:
            r = _post(client, url, json_body=body)
        except httpx.RequestError:
            continue
        if r.status_code != 200:
            continue
        data = r.json()
        total = int(data.get("total") or 0)
        if total > 0:
            return BoardProbeResult(has_jobs=True, extra={"wd_host": wd})
    return BoardProbeResult(has_jobs=False)


def _probe_bamboohr(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://{slug}.bamboohr.com/careers/list"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    try:
        data = r.json()
    except json.JSONDecodeError:
        return BoardProbeResult(has_jobs=False)
    jobs = data.get("result") if isinstance(data, dict) else None
    return BoardProbeResult(has_jobs=isinstance(jobs, list) and len(jobs) > 0)


def _probe_breezy(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://{slug}.breezy.hr/json"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    try:
        data = r.json()
    except json.JSONDecodeError:
        return BoardProbeResult(has_jobs=False)
    return BoardProbeResult(has_jobs=isinstance(data, list) and len(data) > 0)


def _probe_jazzhr(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://{slug}.applytojob.com/apply/jobs/json"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    try:
        data = r.json()
    except json.JSONDecodeError:
        return BoardProbeResult(has_jobs=False)
    jobs = data if isinstance(data, list) else data.get("jobs") if isinstance(data, dict) else None
    return BoardProbeResult(has_jobs=isinstance(jobs, list) and len(jobs) > 0)


def _probe_teamtailor(client: httpx.Client, slug: str) -> BoardProbeResult:
    url = f"https://{slug}.teamtailor.com/jobs.json"
    try:
        r = _get(client, url, follow_redirects=False)
    except httpx.RequestError:
        return BoardProbeResult(has_jobs=False)
    if r.status_code != 200:
        return BoardProbeResult(has_jobs=False)
    try:
        data = r.json()
    except json.JSONDecodeError:
        return BoardProbeResult(has_jobs=False)
    jobs = data.get("jobs") if isinstance(data, dict) else None
    return BoardProbeResult(has_jobs=isinstance(jobs, list) and len(jobs) > 0)
