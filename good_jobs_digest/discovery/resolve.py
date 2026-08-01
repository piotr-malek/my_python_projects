"""Discover mission-aligned employers and resolve public ATS board slugs."""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

from discovery.ats_registry import (
    ATS_PROBE_ORDER,
    careers_url as _careers_url,
    parse_ats_from_text,
    probe_batches_for,
    probe_board,
)

# Backward-compatible alias
ATS_ORDER = ATS_PROBE_ORDER

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
            out.add(t)
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
        "forum",
        "giving",
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


def careers_url(ats_type: str, slug: str, *, region: str = "global") -> str:
    return _careers_url(ats_type, slug, region=region)


def probe_slug(
    client: httpx.Client,
    slug: str,
    *,
    company_name: str = "",
    prefer_ats: str | None = None,
    try_eu_lever: bool = False,
    require_name_match: bool = True,
    parallel_ats: bool = True,
    skip_ats: frozenset[str] | None = None,
    only_ats: frozenset[str] | None = None,
    batch_ats: bool = False,
) -> AtsMatch | None:
    """Probe ATS types for one slug; stop at first acceptable hit."""

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

    def _match_from_result(ats: str, result: Any, *, region: str = "global") -> AtsMatch | None:
        if not result.has_jobs:
            return None
        if not _accept(
            result.board_name,
            ats,
            from_url_hint=from_url_hint and ats == prefer_ats,
        ):
            return None
        if ats == "lever":
            return AtsMatch(
                ats_type="lever",
                ats_slug=slug,
                ats_region=region,
                careers_url=careers_url("lever", slug, region=region),
                board_display_name=result.board_name,
            )
        return AtsMatch(
            ats_type=ats,
            ats_slug=slug,
            careers_url=careers_url(ats, slug),
            board_display_name=result.board_name,
        )

    from_url_hint = bool(prefer_ats)
    order = list(ATS_PROBE_ORDER)
    if only_ats:
        order = [a for a in order if a in only_ats]
    if skip_ats:
        order = [a for a in order if a not in skip_ats]
    if prefer_ats and prefer_ats in order:
        order = [prefer_ats] + [a for a in order if a != prefer_ats]
    if not order:
        return None

    def _probe_one(ats: str) -> AtsMatch | None:
        with httpx.Client(timeout=15.0, follow_redirects=True) as thread_client:
            if ats == "lever" and try_eu_lever:
                for lever_region in ("global", "eu"):
                    result = probe_board(thread_client, ats, slug, region=lever_region)
                    hit = _match_from_result(ats, result, region=lever_region)
                    if hit:
                        return hit
                return None
            result = probe_board(thread_client, ats, slug)
            return _match_from_result(ats, result)

    def _run_parallel_batch(ats_list: list[str]) -> AtsMatch | None:
        if not ats_list:
            return None
        if len(ats_list) == 1:
            return _probe_one(ats_list[0])

        workers = min(12, len(ats_list))
        pool = ThreadPoolExecutor(max_workers=workers)
        futures = {pool.submit(_probe_one, ats): ats for ats in ats_list}
        try:
            matches: list[AtsMatch] = []
            for fut in as_completed(futures):
                try:
                    hit = fut.result()
                except Exception:  # noqa: BLE001
                    continue
                if hit:
                    matches.append(hit)
                    for pending in futures:
                        if pending is not fut and not pending.done():
                            pending.cancel()
                    pool.shutdown(wait=False, cancel_futures=True)
                    matches.sort(key=lambda m: order.index(m.ats_type))
                    return matches[0]
            return None
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

    if parallel_ats and len(order) > 1:
        waves = probe_batches_for(order) if batch_ats else [order]
        for wave in waves:
            hit = _run_parallel_batch(wave)
            if hit:
                return hit
        return None

    for ats in order:
        region = "global"
        if ats == "lever" and try_eu_lever:
            for lever_region in ("global", "eu"):
                result = probe_board(client, ats, slug, region=lever_region)
                if result.has_jobs and _accept(
                    result.board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats
                ):
                    return AtsMatch(
                        ats_type="lever",
                        ats_slug=slug,
                        ats_region=lever_region,
                        careers_url=careers_url("lever", slug),
                        board_display_name=result.board_name,
                    )
            continue
        result = probe_board(client, ats, slug, region=region)
        if result.has_jobs and _accept(
            result.board_name, ats, from_url_hint=from_url_hint and ats == prefer_ats
        ):
            return AtsMatch(
                ats_type=ats,
                ats_slug=slug,
                careers_url=careers_url(ats, slug),
                board_display_name=result.board_name,
            )
        time.sleep(0.15)
    return None


def resolve_candidate(
    client: httpx.Client,
    candidate: EmployerCandidate,
    *,
    try_eu_lever: bool = False,
    max_slug_attempts: int = 0,
    website_first: bool = True,
    parallel_ats: bool = True,
    skip_ats: frozenset[str] | None = None,
    only_ats: frozenset[str] | None = None,
    batch_ats: bool = False,
) -> AtsMatch | None:
    """Resolve ATS for one employer: website careers links, then slug variants."""
    if website_first:
        if not candidate.website.strip():
            from discovery.sources_curated import enrich_candidate_website

            enrich_candidate_website(client, candidate)
        if candidate.website.strip():
            from discovery.careers_discovery import discover_ats_from_website

            hit = discover_ats_from_website(
                client,
                candidate.website,
                company_name=candidate.company_name,
                try_eu_lever=try_eu_lever,
            )
            if hit:
                return hit

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
            parallel_ats=parallel_ats,
            skip_ats=skip_ats,
            only_ats=only_ats,
            batch_ats=batch_ats,
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
        "job_board_url": match.careers_url or careers_url(match.ats_type, match.ats_slug),
        "discovery_source": candidate.discovery_source or "",
        "poll_enabled": "true",
        "notes": "; ".join(p for p in note_parts if p).strip("; "),
    }
