"""Classify employers by EU/remote footprint using structured ATS location fields only.

No description regexes — only board location / remote flags from ATS JSON.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

import httpx

from discovery.ats_registry import careers_url
from discovery.resolve import parse_ats_from_text

logger = logging.getLogger(__name__)

# Tokens that signal EU / Europe / EEA / UK / CH hire regions in structured location fields.
_EU_LOCATION_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.I)
    for p in (
        r"\beurope\b",
        r"\beu\b",
        r"\beea\b",
        r"\bem\s*ea\b",
        r"\bemea\b",
        r"\buk\b",
        r"\bunited kingdom\b",
        r"\bengland\b",
        r"\bscotland\b",
        r"\bwales\b",
        r"\bireland\b",
        r"\bdublin\b",
        r"\blondon\b",
        r"\bberlin\b",
        r"\bmunich\b",
        r"\bmünchen\b",
        r"\bhamburg\b",
        r"\bamsterdam\b",
        r"\broterdam\b",
        r"\butrecht\b",
        r"\bparis\b",
        r"\blyon\b",
        r"\bbrussels\b",
        r"\bbruxelles\b",
        r"\bzurich\b",
        r"\bzürich\b",
        r"\bgeneva\b",
        r"\bstockholm\b",
        r"\boslo\b",
        r"\bcopenhagen\b",
        r"\bhelsinki\b",
        r"\bwarsaw\b",
        r"\bwarszawa\b",
        r"\bprague\b",
        r"\bpraha\b",
        r"\blisbon\b",
        r"\blisboa\b",
        r"\bmadrid\b",
        r"\bbarcelona\b",
        r"\bvienna\b",
        r"\bwien\b",
        r"\bbudapest\b",
        r"\bbucharest\b",
        r"\bathens\b",
        r"\bportugal\b",
        r"\bspain\b",
        r"\bespaña\b",
        r"\bfrance\b",
        r"\bgermany\b",
        r"\bdeutschland\b",
        r"\bnetherlands\b",
        r"\bholland\b",
        r"\bbelgium\b",
        r"\baustr\w*\b",
        r"\bswitzerland\b",
        r"\bschweiz\b",
        r"\bsuisse\b",
        r"\bsweden\b",
        r"\bdenmark\b",
        r"\bfinland\b",
        r"\bnorway\b",
        r"\bpoland\b",
        r"\bitalia\b",
        r"\bitaly\b",
        r"\brome\b",
        r"\bmilan\b",
        r"\bmilano\b",
        r"\bczech\b",
        r"\bhungary\b",
        r"\bromania\b",
        r"\bgreece\b",
        r"\bluxembourg\b",
        r"\bestonia\b",
        r"\blatvia\b",
        r"\blithuania\b",
        r"\bslovakia\b",
        r"\bslovenia\b",
        r"\bcroatia\b",
        r"\bbulgaria\b",
        r"\bmalta\b",
        r"\bcyprus\b",
        r"\biceland\b",
        r"\bcet\b",
        r"\bcest\b",
        r"\bwarsaw\b",
    )
)

_US_ONLY_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.I)
    for p in (
        r"\bunited states\b",
        r"\bu\.?s\.?a\.?\b",
        r"\busa\b",
        r"\bus-only\b",
        r"\bus only\b",
        r"\bcalifornia\b",
        r"\bnew york\b",
        r"\btexas\b",
        r"\bwashington(?!,?\s*d\.?c)",
        r"\bsan francisco\b",
        r"\bseattle\b",
        r"\baustin\b",
        r"\bboston\b",
        r"\bchicago\b",
        r"\bdenver\b",
        r"\batlanta\b",
        r"\bmiami\b",
        r"\blos angeles\b",
    )
)

_REMOTE_RES = re.compile(
    r"\b(?:remote|work from home|wfh|distributed|anywhere|worldwide|work from anywhere)\b",
    re.I,
)


# A board must show at least this many postings before "no EU role today" counts as
# evidence that the employer never hires in the EU. Below it (or with an empty board)
# the verdict is "undecided" and the employer is kept — an employer that simply isn't
# hiring this week is not the same as a US-only employer, and demotion is permanent.
MIN_POSTINGS_FOR_DEMOTION = 4


@dataclass
class FootprintVerdict:
    company_name: str
    ats_type: str
    ats_slug: str
    ok: bool
    has_eu_or_remote: bool
    posting_count: int
    eu_or_remote_count: int
    sample_locations: list[str] = field(default_factory=list)
    reason: str = ""
    http_ok: bool = False
    # "eu_ok" | "demote" | "undecided"
    verdict: str = "undecided"

    @property
    def label(self) -> str:
        return f"{self.ats_type}:{self.ats_slug}"

    @property
    def should_keep(self) -> bool:
        """Keep unless there is positive evidence of a US-only/non-EU employer."""
        return self.verdict != "demote"


def location_is_eu_or_remote(location: str, *, is_remote: bool = False) -> bool:
    """True when a structured location string looks EU-eligible or remote."""
    text = (location or "").strip()
    if is_remote or (text and _REMOTE_RES.search(text)):
        return True
    if not text:
        return False
    if any(p.search(text) for p in _EU_LOCATION_RES):
        return True
    return False


def location_looks_us_only(location: str) -> bool:
    text = (location or "").strip()
    if not text:
        return False
    if location_is_eu_or_remote(text):
        return False
    return any(p.search(text) for p in _US_ONLY_RES)


def _locations_from_greenhouse(jobs: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for job in jobs:
        loc = job.get("location") or {}
        name = loc.get("name") if isinstance(loc, dict) else loc
        text = str(name or "").strip()
        remote = bool(_REMOTE_RES.search(text)) if text else False
        out.append((text, remote))
    return out


def _locations_from_lever(jobs: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for job in jobs:
        cats = job.get("categories") or {}
        loc = cats.get("location") if isinstance(cats, dict) else ""
        text = str(loc or job.get("workplaceType") or "").strip()
        remote = str(job.get("workplaceType") or "").lower() == "remote" or bool(
            _REMOTE_RES.search(text)
        )
        out.append((text, remote))
    return out


def _locations_from_ashby(jobs: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for job in jobs:
        loc = job.get("location") or job.get("locationName") or ""
        text = str(loc).strip() if loc else ""
        remote = bool(job.get("isRemote")) or bool(_REMOTE_RES.search(text))
        out.append((text, remote))
    return out


def _locations_from_workable(jobs: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for job in jobs:
        loc = job.get("location") or {}
        if isinstance(loc, dict):
            parts = [loc.get("city"), loc.get("country"), loc.get("region"), loc.get("location_str")]
            text = ", ".join(str(p) for p in parts if p)
        else:
            text = str(loc or "")
        remote = bool(job.get("remote")) or bool(_REMOTE_RES.search(text))
        out.append((text.strip(), remote))
    return out


def _locations_generic(jobs: list[dict[str, Any]]) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    for job in jobs:
        for key in ("location", "locations", "city", "country", "office"):
            val = job.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                text = "; ".join(str(x) for x in val)
            elif isinstance(val, dict):
                text = ", ".join(str(v) for v in val.values() if v)
            else:
                text = str(val)
            remote = bool(job.get("remote") or job.get("isRemote") or job.get("is_remote"))
            out.append((text.strip(), remote or bool(_REMOTE_RES.search(text))))
            break
        else:
            out.append(("", bool(job.get("remote") or job.get("isRemote"))))
    return out


def fetch_board_locations(
    client: httpx.Client,
    *,
    ats_type: str,
    ats_slug: str,
    ats_region: str = "global",
) -> tuple[list[tuple[str, bool]], bool, str]:
    """Return ([(location, is_remote), ...], http_ok, reason)."""
    ats = ats_type.lower().strip()
    slug = ats_slug.strip()
    if not slug:
        return [], False, "missing slug"

    try:
        if ats == "greenhouse":
            r = client.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            jobs = r.json().get("jobs") or []
            return _locations_from_greenhouse(jobs if isinstance(jobs, list) else []), True, ""

        if ats == "lever":
            host = (
                "https://api.eu.lever.co/v0/postings"
                if (ats_region or "").lower() == "eu"
                else "https://api.lever.co/v0/postings"
            )
            r = client.get(f"{host}/{slug}?mode=json&limit=100")
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            jobs = r.json()
            return _locations_from_lever(jobs if isinstance(jobs, list) else []), True, ""

        if ats == "ashby":
            r = client.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            data = r.json()
            jobs = data.get("jobs") if isinstance(data, dict) else data
            return _locations_from_ashby(jobs if isinstance(jobs, list) else []), True, ""

        if ats == "workable":
            r = client.get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}")
            if r.status_code >= 400:
                r = client.get(f"https://apply.workable.com/api/v3/accounts/{slug}/jobs")
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            data = r.json()
            jobs = data.get("jobs") if isinstance(data, dict) else data
            return _locations_from_workable(jobs if isinstance(jobs, list) else []), True, ""

        if ats == "smartrecruiters":
            r = client.get(
                f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
                params={"limit": 100},
            )
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            data = r.json()
            jobs = data.get("content") if isinstance(data, dict) else data
            locs: list[tuple[str, bool]] = []
            for job in jobs if isinstance(jobs, list) else []:
                loc = job.get("location") or {}
                if isinstance(loc, dict):
                    text = ", ".join(
                        str(loc.get(k) or "")
                        for k in ("city", "region", "country")
                        if loc.get(k)
                    )
                else:
                    text = str(loc or "")
                locs.append((text, bool(_REMOTE_RES.search(text))))
            return locs, True, ""

        if ats == "recruitee":
            r = client.get(f"https://{slug}.recruitee.com/api/offers/")
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            data = r.json()
            offers = data.get("offers") if isinstance(data, dict) else data
            locs = []
            for job in offers if isinstance(offers, list) else []:
                text = str(job.get("location") or job.get("city") or "")
                remote = bool(job.get("remote")) or bool(_REMOTE_RES.search(text))
                locs.append((text, remote))
            return locs, True, ""

        if ats == "personio":
            for host in (f"https://{slug}.jobs.personio.de", f"https://{slug}.jobs.personio.com"):
                r = client.get(f"{host}/xml")
                if r.status_code < 400:
                    # Minimal XML location scrape without description body.
                    texts = re.findall(
                        r"<office>([^<]*)</office>|<locations?>\s*<location>([^<]*)</location>",
                        r.text,
                        flags=re.I,
                    )
                    locs = []
                    for a, b in texts:
                        text = (a or b or "").strip()
                        locs.append((text, bool(_REMOTE_RES.search(text))))
                    return locs, True, ""
            return [], False, "personio http error"

        # Fallback: probe careers URL only (no locations) → treat as unknown.
        url = careers_url(ats, slug)
        if url:
            r = client.get(url, follow_redirects=True)
            if r.status_code >= 400:
                return [], False, f"http {r.status_code}"
            return [], True, "no structured locations for ats type"
        return [], False, f"unsupported ats {ats}"
    except Exception as exc:  # noqa: BLE001
        return [], False, str(exc)


def classify_locations(locations: Iterable[tuple[str, bool]]) -> tuple[bool, int, int, list[str]]:
    """Return (has_eu_or_remote, posting_count, eu_or_remote_count, sample_locations)."""
    locs = list(locations)
    eu_n = 0
    samples: list[str] = []
    for text, remote in locs:
        if location_is_eu_or_remote(text, is_remote=remote):
            eu_n += 1
            if text and text not in samples and len(samples) < 5:
                samples.append(text)
        elif text and len(samples) < 5 and text not in samples:
            samples.append(text)
    return eu_n > 0, len(locs), eu_n, samples


def evaluate_employer(
    client: httpx.Client,
    *,
    company_name: str,
    ats_type: str = "",
    ats_slug: str = "",
    ats_region: str = "global",
    job_board_url: str = "",
) -> FootprintVerdict:
    ats = (ats_type or "").strip().lower()
    slug = (ats_slug or "").strip()
    if (not ats or not slug) and job_board_url:
        parsed = parse_ats_from_text(job_board_url)
        if parsed:
            ats, slug = parsed

    base = FootprintVerdict(
        company_name=company_name,
        ats_type=ats,
        ats_slug=slug,
        ok=False,
        has_eu_or_remote=False,
        posting_count=0,
        eu_or_remote_count=0,
    )
    if not ats or not slug:
        base.reason = "no ats coordinates"
        base.verdict = "undecided"
        return base

    locs, http_ok, reason = fetch_board_locations(
        client, ats_type=ats, ats_slug=slug, ats_region=ats_region
    )
    base.http_ok = http_ok
    if not http_ok:
        # A 404 board is genuinely dead; any other failure is transient.
        base.reason = reason or "fetch failed"
        base.verdict = "demote" if "404" in (reason or "") else "undecided"
        return base

    has_eu, total, eu_n, samples = classify_locations(locs)
    base.posting_count = total
    base.eu_or_remote_count = eu_n
    base.sample_locations = samples
    base.has_eu_or_remote = has_eu
    if has_eu:
        base.ok = True
        base.verdict = "eu_ok"
        base.reason = f"{eu_n}/{total} postings EU/remote"
        return base
    if total == 0:
        # Empty board today says nothing about who they hire.
        base.reason = reason or "no postings (kept — no evidence)"
        base.verdict = "undecided"
        return base
    if total < MIN_POSTINGS_FOR_DEMOTION:
        base.reason = f"only {total} posting(s), no EU/remote (kept — weak evidence)"
        base.verdict = "undecided"
        return base
    # Enough postings to judge, and none are EU/remote → demote.
    base.verdict = "demote"
    base.reason = "us_only_dead_weight" if any(
        location_looks_us_only(t) for t, _ in locs
    ) else "no_eu_or_remote_postings"
    return base
