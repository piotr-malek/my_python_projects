"""Discover mission-aligned employers and resolve public ATS board slugs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

ATS_ORDER = ("greenhouse", "smartrecruiters", "lever")

_GREENHOUSE_RE = re.compile(
    r"(?:boards(?:-api)?\.greenhouse\.io|job-boards\.greenhouse\.io)/([^/?#\s\"']+)",
    re.I,
)
_LEVER_RE = re.compile(r"jobs\.lever\.co/([^/?#\s\"']+)", re.I)
_SMARTRECRUITERS_RE = re.compile(
    r"(?:careers\.smartrecruiters\.com|api\.smartrecruiters\.com/v1/companies)/([^/?#\s\"']+)",
    re.I,
)

_SUFFIXES = (
    " inc",
    " inc.",
    " llc",
    " ltd",
    " ltd.",
    " limited",
    " gmbh",
    " corp",
    " corporation",
    " co.",
    " company",
    " foundation",
    " international",
    " plc",
    " ag",
    " sa",
    " bv",
    " ngo",
    " gmbh.",
)


@dataclass
class EmployerCandidate:
    company_name: str
    mission_category: str = "mission"
    website: str = ""
    discovery_source: str = ""
    ats_hint: tuple[str, str] | None = None  # (ats_type, slug)
    extra_slugs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AtsMatch:
    ats_type: str
    ats_slug: str
    ats_region: str = "global"
    careers_url: str = ""
    validated: bool = True
    board_display_name: str = ""


def parse_ats_from_text(text: str) -> tuple[str, str] | None:
    """Return (ats_type, slug) from a URL or HTML blob."""
    if not text:
        return None
    for pattern, ats in (
        (_GREENHOUSE_RE, "greenhouse"),
        (_SMARTRECRUITERS_RE, "smartrecruiters"),
        (_LEVER_RE, "lever"),
    ):
        m = pattern.search(text)
        if m:
            slug = m.group(1).split("/")[0].strip()
            if slug:
                return ats, slug
    return None


def slug_candidates(
    company_name: str,
    *,
    website: str = "",
    hints: list[str] | None = None,
) -> list[str]:
    """Generate plausible ATS board tokens (most likely first)."""
    seen: set[str] = set()
    out: list[str] = []

    def add(raw: str) -> None:
        s = raw.strip().strip("/")
        if not s or len(s) < 2:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(s)

    for h in hints or []:
        add(h)

    n = company_name.lower().strip()
    for suf in _SUFFIXES:
        if n.endswith(suf):
            n = n[: -len(suf)].strip()

    compact = re.sub(r"[^a-z0-9]", "", n)
    hyphen = re.sub(r"[^a-z0-9]+", "-", n).strip("-")
    underscored = re.sub(r"[^a-z0-9]+", "_", n).strip("_")

    for variant in (compact, hyphen, underscored):
        add(variant)

    words = [re.sub(r"[^a-z0-9]", "", w) for w in n.split() if w]
    words = [w for w in words if w]
    if len(words) >= 2:
        add("".join(words))
        add("-".join(words))
        add(words[0])
    elif words:
        add(words[0])

    if website:
        host = website.strip()
        if "://" not in host:
            host = f"https://{host}"
        try:
            netloc = urlparse(host).netloc.lower().replace("www.", "")
            base = netloc.split(".")[0]
            add(base)
        except Exception:  # noqa: BLE001
            pass

    return out


def _name_tokens(text: str) -> set[str]:
    out: set[str] = set()
    for t in re.findall(r"[a-z0-9]+", text.lower()):
        if len(t) >= 3:
            out.add(t)
        elif len(t) >= 2 and any(c.isdigit() for c in t):
            out.add(t)  # e.g. 2U
    return out


_GENERIC_TOKENS = frozenset(
    {
        "action",
        "against",
        "national",
        "community",
        "future",
        "essential",
        "forward",
        "founders",
        "coalition",
        "general",
        "digital",
        "global",
        "world",
        "open",
        "blue",
        "bird",
        "carbon",
        "david",
        "education",
        "energy",
        "health",
        "institute",
        "foundation",
        "group",
        "international",
        "federal",
        "european",
        "union",
        "commission",
        "network",
        "research",
        "center",
        "centre",
        "management",
        "policies",
        "institute",
        "forum",
        "giving",
        "policies",
        "building",
        "intelligence",
        "solutions",
        "technologies",
        "technology",
        "services",
        "partners",
        "company",
        "inc",
    }
)


def slug_aligns_with_company(slug: str, company_name: str) -> bool:
    """Slug token should cover a large share of the company name (not a short prefix)."""
    compact_company = re.sub(r"[^a-z0-9]", "", company_name.lower())
    compact_slug = re.sub(r"[^a-z0-9]", "", slug.lower())
    if not compact_slug or not compact_company:
        return False
    if compact_slug == compact_company:
        return len(compact_slug) >= 3
    if len(compact_slug) < 5:
        return False
    if compact_slug in compact_company:
        return len(compact_slug) >= max(6, int(len(compact_company) * 0.45))
    if compact_company in compact_slug:
        return len(compact_company) >= 6
    return False


def employer_names_align(company_name: str, board_name: str) -> bool:
    """Board title should share a distinctive token with the employer name."""
    company_tokens = _name_tokens(company_name)
    board_tokens = _name_tokens(board_name)
    if not company_tokens or not board_tokens:
        return False
    overlap = company_tokens & board_tokens
    if not overlap:
        return False
    distinctive = {t for t in overlap if t not in _GENERIC_TOKENS and len(t) >= 4}
    if distinctive:
        return True
    if len(overlap) >= 2 and all(len(t) >= 4 for t in overlap):
        return True
    compact_company = re.sub(r"[^a-z0-9]", "", company_name.lower())
    compact_board = re.sub(r"[^a-z0-9]", "", board_name.lower())
    if compact_company and compact_board:
        if compact_company == compact_board:
            return True
        if compact_company in compact_board or compact_board in compact_company:
            shorter = min(len(compact_company), len(compact_board))
            longer = max(len(compact_company), len(compact_board))
            return shorter >= 6 and shorter / longer >= 0.55
    return False


def careers_url(ats_type: str, slug: str) -> str:
    if ats_type == "greenhouse":
        return f"https://boards.greenhouse.io/{slug}"
    if ats_type == "lever":
        return f"https://jobs.lever.co/{slug}"
    if ats_type == "smartrecruiters":
        return f"https://careers.smartrecruiters.com/{slug}"
    return ""


def _greenhouse_board(client: httpx.Client, slug: str) -> tuple[bool, str]:
    """Return (has_open_jobs, board_display_name)."""
    jobs_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=false"
    try:
        r = client.get(jobs_url)
    except httpx.RequestError:
        return False, ""
    if r.status_code != 200:
        return False, ""
    jobs = r.json().get("jobs")
    if not isinstance(jobs, list) or len(jobs) == 0:
        return False, ""
    meta_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}"
    try:
        meta = client.get(meta_url)
    except httpx.RequestError:
        meta = None
    board_name = ""
    if meta and meta.status_code == 200:
        board_name = str(meta.json().get("name") or "")
    return True, board_name


def _smartrecruiters_board(client: httpx.Client, slug: str) -> tuple[bool, str]:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=1"
    try:
        r = client.get(url)
    except httpx.RequestError:
        return False, ""
    if r.status_code != 200:
        return False, ""
    if int(r.json().get("totalFound") or 0) <= 0:
        return False, ""
    try:
        ident = client.get(f"https://api.smartrecruiters.com/v1/companies/{slug}")
    except httpx.RequestError:
        ident = None
    name = ""
    if ident and ident.status_code == 200:
        name = str(ident.json().get("name") or "")
    return True, name


def _lever_board(client: httpx.Client, slug: str, *, region: str = "global") -> tuple[bool, str]:
    base = "https://api.eu.lever.co" if region == "eu" else "https://api.lever.co"
    url = f"{base}/v0/postings/{slug}?mode=json&limit=1"
    try:
        r = client.get(url)
    except httpx.RequestError:
        return False, ""
    if r.status_code != 200:
        return False, ""
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        return False, ""
    # Lever postings API does not expose employer display name — identity is checked via slug.
    return True, ""


def probe_slug(
    client: httpx.Client,
    slug: str,
    *,
    company_name: str = "",
    prefer_ats: str | None = None,
    try_eu_lever: bool = False,
    require_name_match: bool = True,
) -> AtsMatch | None:
    """Probe Greenhouse → SmartRecruiters → Lever for one slug; stop at first hit."""

    def _accept(board_name: str, ats: str, *, from_url_hint: bool) -> bool:
        if not require_name_match or not company_name:
            return True
        slug_ok = slug_aligns_with_company(slug, company_name)
        if ats == "lever":
            return slug_ok
        name_ok = bool(board_name) and employer_names_align(company_name, board_name)
        if from_url_hint:
            return slug_ok or name_ok
        if board_name and not name_ok:
            return False
        return name_ok or slug_ok

    from_url_hint = bool(prefer_ats)
    order = list(ATS_ORDER)
    if prefer_ats and prefer_ats in order:
        order = [prefer_ats] + [a for a in order if a != prefer_ats]

    for ats in order:
        if ats == "greenhouse":
            ok, board_name = _greenhouse_board(client, slug)
            if ok and _accept(board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats):
                return AtsMatch(
                    ats_type="greenhouse",
                    ats_slug=slug,
                    careers_url=careers_url("greenhouse", slug),
                    board_display_name=board_name,
                )
        if ats == "smartrecruiters":
            ok, board_name = _smartrecruiters_board(client, slug)
            if ok and _accept(board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats):
                return AtsMatch(
                    ats_type="smartrecruiters",
                    ats_slug=slug,
                    careers_url=careers_url("smartrecruiters", slug),
                    board_display_name=board_name,
                )
        if ats == "lever":
            ok, board_name = _lever_board(client, slug, region="global")
            if ok and _accept(board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats):
                return AtsMatch(
                    ats_type="lever",
                    ats_slug=slug,
                    careers_url=careers_url("lever", slug),
                    board_display_name=board_name,
                )
            if try_eu_lever:
                ok, board_name = _lever_board(client, slug, region="eu")
                if ok and _accept(board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats):
                    return AtsMatch(
                        ats_type="lever",
                        ats_slug=slug,
                        ats_region="eu",
                        careers_url=careers_url("lever", slug),
                        board_display_name=board_name,
                    )
    return None


def resolve_candidate(
    client: httpx.Client,
    candidate: EmployerCandidate,
    *,
    try_eu_lever: bool = False,
    max_slug_attempts: int = 0,
) -> AtsMatch | None:
    """Resolve ATS for one employer using hints then slug variants."""
    hints: list[str] = list(candidate.extra_slugs)
    prefer: str | None = None
    if candidate.ats_hint:
        prefer, hinted = candidate.ats_hint
        hints.insert(0, hinted)

    slugs = slug_candidates(
        candidate.company_name,
        website=candidate.website,
        hints=hints,
    )
    if max_slug_attempts > 0:
        slugs = slugs[:max_slug_attempts]

    for i, slug in enumerate(slugs):
        if len(slug) < 4 and not (candidate.ats_hint and i == 0):
            continue
        hit = probe_slug(
            client,
            slug,
            company_name=candidate.company_name,
            prefer_ats=prefer if i == 0 else None,
            try_eu_lever=try_eu_lever,
            require_name_match=True,
        )
        if hit:
            return hit
        prefer = None
    return None


def registry_row(
    candidate: EmployerCandidate,
    match: AtsMatch,
    *,
    notes: str = "",
) -> dict[str, str]:
    note_parts = [
        f"source={candidate.discovery_source}" if candidate.discovery_source else "",
        notes,
    ]
    return {
        "company_name": candidate.company_name,
        "mission_category": candidate.mission_category,
        "ats_type": match.ats_type,
        "ats_slug": match.ats_slug,
        "ats_region": match.ats_region,
        "careers_url": match.careers_url or careers_url(match.ats_type, match.ats_slug),
        "poll_enabled": "true",
        "notes": "; ".join(p for p in note_parts if p).strip("; "),
    }
