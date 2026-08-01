"""Cross-source job deduplication by canonical key and source priority."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

# Lower index = higher priority (org ATS preferred over aggregators).
SOURCE_PRIORITY: tuple[str, ...] = (
    "greenhouse",
    "lever",
    "ashby",
    "workable",
    "recruitee",
    "personio",
    "workday",
    "bamboohr",
    "breezy",
    "jazzhr",
    "teamtailor",
    "smartrecruiters",
    "idealist",
    "animaladvocacycareers",
    "workonclimate",
    "impactpool",
    "techjobsforgood",
    "climatebase",
    "80000hours",
    "escapethecity",
    "reliefweb",
    "job_board",
)

_AGGREGATOR_ATS = frozenset({"job_board"})


def _norm_text(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _posted_week(hint: str | None) -> str:
    if not hint:
        return ""
    try:
        dt = datetime.fromisoformat(str(hint).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        iso = dt.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    except ValueError:
        return _norm_text(hint)[:10]


def _location_bucket(location: str | None) -> str:
    loc = _norm_text(location)
    if not loc:
        return ""
    # crude country/region bucket: last comma-separated segment often holds country
    parts = [p.strip() for p in loc.split(",") if p.strip()]
    return parts[-1] if parts else loc[:40]


def canonical_key(job: dict[str, Any]) -> str:
    """Hash key: org + title + location bucket + posted week."""
    parts = "|".join(
        [
            _norm_text(job.get("company_name")),
            _norm_text(job.get("title")),
            _location_bucket(job.get("location_text")),
            _posted_week(job.get("posted_at_hint") or job.get("posted_at")),
        ]
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()[:32]


def source_priority(source: str, ats_type: str | None = None) -> int:
    src = (source or "").lower()
    ats = (ats_type or "").lower()
    for i, name in enumerate(SOURCE_PRIORITY):
        if src == name or ats == name:
            return i
    if ats in _AGGREGATOR_ATS or src in _AGGREGATOR_ATS:
        return len(SOURCE_PRIORITY) + 5
    return len(SOURCE_PRIORITY) + 10


def merge_duplicate(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """Pick winner by source priority; prefer org apply URL when tied."""
    ex_pri = source_priority(str(existing.get("source") or ""), str(existing.get("ats_type") or ""))
    in_pri = source_priority(str(incoming.get("source") or ""), str(incoming.get("ats_type") or ""))
    if in_pri < ex_pri:
        winner, loser = incoming, existing
    elif ex_pri < in_pri:
        winner, loser = existing, incoming
    else:
        # same tier — prefer non-aggregator URL
        ex_url = str(existing.get("url") or "")
        in_url = str(incoming.get("url") or "")
        ex_agg = "reliefweb" in ex_url or "impactpool" in ex_url or "idealist" in ex_url
        in_agg = "reliefweb" in in_url or "impactpool" in in_url or "idealist" in in_url
        if ex_agg and not in_agg:
            winner, loser = incoming, existing
        elif in_agg and not ex_agg:
            winner, loser = existing, incoming
        else:
            winner, loser = existing, incoming
    merged = dict(winner)
    merged["canonical_job_id"] = canonical_key(winner)
    if loser.get("url") and loser["url"] != winner.get("url"):
        merged.setdefault("alternate_urls", [])
        alt = merged["alternate_urls"]
        if isinstance(alt, list):
            alt.append(loser["url"])
    return merged
