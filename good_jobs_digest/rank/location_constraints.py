"""Extract stated location and work-authorization constraints from job text."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# (regex, canonical region label)
_REGION_PATTERNS: list[tuple[str, str]] = [
    (r"\bunited states\b|\bu\.?\s*s\.?\b|\busa\b", "United States"),
    (r"\bunited kingdom\b|\b(?:^|\W)uk(?:$|\W)\b", "United Kingdom"),
    (r"\bcanada\b", "Canada"),
    (r"\beuropean union\b|\beu\b(?!\s*time)", "EU"),
    (r"\beurope\b|\beuropean\b|\bemea\b|\beea\b", "Europe"),
    (r"\bgermany\b|\bdeutschland\b", "Germany"),
    (r"\bfrance\b", "France"),
    (r"\bpoland\b", "Poland"),
    (r"\bspain\b", "Spain"),
    (r"\bnetherlands\b|\bholland\b", "Netherlands"),
    (r"\bireland\b", "Ireland"),
    (r"\baustralia\b", "Australia"),
    (r"\bindia\b", "India"),
    (r"\bsingapore\b", "Singapore"),
]

# Expansions for preference tokens like acceptable_hire_regions: [EU]
_ACCEPTABLE_EXPANSIONS: dict[str, frozenset[str]] = {
    "EU": frozenset(
        {
            "EU",
            "Europe",
            "Germany",
            "France",
            "Poland",
            "Spain",
            "Netherlands",
            "Ireland",
            "United Kingdom",
        }
    ),
    "Europe": frozenset(
        {
            "EU",
            "Europe",
            "Germany",
            "France",
            "Poland",
            "Spain",
            "Netherlands",
            "Ireland",
            "United Kingdom",
        }
    ),
}

_AUTH_PATTERNS: list[tuple[str, str]] = [
    (
        r"authorized to work in the united states|authorized to work in the u\.?s\.?",
        "Must be authorized to work in the United States",
    ),
    (r"for u\.?s\.?\s*-?\s*based candidates", "Compensation/benefits listed for U.S.-based candidates only"),
    (r"right to work in the united states|right to work in the u\.?s\.?", "Must have right to work in the United States"),
    (
        r"require.*(?:visa|immigration|employment authorization).{0,40}united states|united states.{0,40}(?:visa|immigration|employment authorization)",
        "US visa or employment authorization required",
    ),
    (r"must be (?:legally )?(?:based|located|residing) in ([^.?\n]{3,60})", "Must be based in specific location"),
    (r"only (?:open to|available to|considering) candidates (?:in|from|based in) ([^.?\n]{3,60})", "Candidates restricted by location"),
    (r"not eligible for visa sponsorship", "No visa sponsorship"),
]

_GLOBAL_REMOTE_MARKERS = (
    "worldwide",
    "anywhere",
    "global",
    "work from anywhere",
    "no location requirement",
    "location independent",
)


@dataclass
class LocationPolicy:
    acceptable_hire_regions: list[str] = field(default_factory=list)
    allow_unspecified_location: bool = True
    prefer_global_remote: bool = True


def location_policy_from_prefs(prefs: dict[str, Any]) -> LocationPolicy:
    loc = prefs.get("location") or {}
    raw = loc.get("acceptable_hire_regions") or []
    if isinstance(raw, list):
        regions = [str(x).strip() for x in raw if str(x).strip()]
    else:
        regions = []
    allow_unspecified = loc.get("allow_unspecified_location")
    prefer_global = loc.get("prefer_global_remote")
    return LocationPolicy(
        acceptable_hire_regions=regions,
        allow_unspecified_location=True if allow_unspecified is None else bool(allow_unspecified),
        prefer_global_remote=True if prefer_global is None else bool(prefer_global),
    )


@dataclass
class LocationConstraints:
    location_line: str | None = None
    stated_regions: list[str] = field(default_factory=list)
    auth_signals: list[str] = field(default_factory=list)
    remote_within_region: bool = False
    appears_global_remote: bool = False
    likely_region_mismatch: bool = False
    summary_lines: list[str] = field(default_factory=list)


def expand_acceptable_hire_regions(regions: list[str] | None) -> set[str]:
    """Expand preference tokens (e.g. EU) to canonical region labels."""
    if not regions:
        return set()
    out: set[str] = set()
    for raw in regions:
        token = str(raw).strip()
        if not token:
            continue
        expanded = _ACCEPTABLE_EXPANSIONS.get(token) or _ACCEPTABLE_EXPANSIONS.get(token.upper())
        if expanded:
            out.update(expanded)
        else:
            out.add(token)
    return out


def evaluate_hire_region_fit(
    constraints: LocationConstraints,
    acceptable_hire_regions: list[str] | None,
    *,
    allow_unspecified_location: bool = True,
) -> bool | None:
    """
    Return True if job regions match acceptable list, False if clearly incompatible, None if unclear.

    When allow_unspecified_location is True, postings with no stated hire country/region are treated
    as compatible. Global / work-from-anywhere roles are always compatible unless country-restricted.
    """
    acceptable = expand_acceptable_hire_regions(acceptable_hire_regions)
    if not acceptable:
        return None

    us_auth = any("united states" in a.lower() or "u.s." in a.lower() for a in constraints.auth_signals)
    if us_auth and "United States" not in acceptable:
        return False

    if constraints.appears_global_remote and not constraints.remote_within_region:
        return True

    if constraints.stated_regions:
        if any(r in acceptable for r in constraints.stated_regions):
            return True
        if constraints.remote_within_region or not any(r in acceptable for r in constraints.stated_regions):
            return False

    if allow_unspecified_location and not constraints.stated_regions:
        return True

    return None


def _find_regions(text: str) -> list[str]:
    blob = text.lower()
    found: list[str] = []
    for pattern, label in _REGION_PATTERNS:
        if re.search(pattern, blob, flags=re.I):
            if label not in found:
                found.append(label)
    return found


def _parse_location_line(location_text: str | None) -> tuple[str | None, list[str], bool]:
    loc = (location_text or "").strip()
    if not loc:
        return None, [], False

    remote_within = bool(re.search(r"\(\s*remote\s*\)|remote\s*[-–—]\s*|,\s*remote\b", loc, flags=re.I))
    regions = _find_regions(loc)

    paren = re.match(r"^(.+?)\s*\(\s*remote\s*\)\s*$", loc, flags=re.I)
    if paren:
        inner = paren.group(1).strip()
        inner_regions = _find_regions(inner)
        if inner_regions:
            regions = inner_regions
        elif inner:
            regions = [inner]
        remote_within = True

    return loc, regions, remote_within


def extract_location_constraints(
    *,
    title: str | None = None,
    location_text: str | None = None,
    description_text: str | None = None,
    acceptable_hire_regions: list[str] | None = None,
    allow_unspecified_location: bool = True,
    prefer_global_remote: bool = True,
) -> LocationConstraints:
    """Heuristic parse of where the employer expects the hire to be based."""
    title_s = title or ""
    desc = description_text or ""
    head = desc[:2500]

    loc_line, loc_regions, remote_within = _parse_location_line(location_text)
    desc_regions = _find_regions(head)
    title_regions = _find_regions(title_s)

    regions: list[str] = []
    for r in loc_regions + title_regions + desc_regions:
        if r not in regions:
            regions.append(r)

    auth: list[str] = []
    blob = f"{loc_line or ''}\n{head}".lower()
    for pattern, label in _AUTH_PATTERNS:
        if re.search(pattern, blob, flags=re.I):
            if label not in auth:
                auth.append(label)

    appears_global = any(m in blob for m in _GLOBAL_REMOTE_MARKERS)

    base = LocationConstraints(
        location_line=loc_line,
        stated_regions=regions,
        auth_signals=auth,
        remote_within_region=remote_within,
        appears_global_remote=appears_global,
    )
    fit = evaluate_hire_region_fit(
        base,
        acceptable_hire_regions,
        allow_unspecified_location=allow_unspecified_location,
    )
    base.likely_region_mismatch = fit is False
    base.summary_lines = _build_summary_lines(
        base,
        acceptable_hire_regions,
        fit,
        allow_unspecified_location=allow_unspecified_location,
        prefer_global_remote=prefer_global_remote,
    )
    return base


def _build_summary_lines(
    constraints: LocationConstraints,
    acceptable_hire_regions: list[str] | None,
    fit: bool | None,
    *,
    allow_unspecified_location: bool = True,
    prefer_global_remote: bool = True,
) -> list[str]:
    lines: list[str] = []
    if acceptable_hire_regions:
        expanded = ", ".join(sorted(expand_acceptable_hire_regions(acceptable_hire_regions)))
        lines.append(f"- Candidate acceptable hire region(s): {', '.join(acceptable_hire_regions)} ({expanded})")
    if allow_unspecified_location:
        lines.append("- Policy: unspecified hire location is acceptable (no country/region stated)")
    if prefer_global_remote:
        lines.append("- Policy: global / work-from-anywhere roles are preferred when available")

    if constraints.location_line:
        lines.append(f"- Stated location line: {constraints.location_line}")
    if constraints.stated_regions:
        lines.append(f"- Inferred hire region(s): {', '.join(constraints.stated_regions)}")
    if constraints.remote_within_region:
        lines.append(
            "- Remote type: country/region-restricted remote (NOT location-independent — "
            "candidate must typically be based in the stated country)"
        )
    elif constraints.appears_global_remote:
        lines.append("- Remote type: appears globally remote or location-flexible")
    if constraints.auth_signals:
        lines.append("- Work authorization / residency signals:")
        for a in constraints.auth_signals:
            lines.append(f"  - {a}")

    if fit is False:
        lines.append(
            "- Automated check: stated hire region does NOT match candidate acceptable regions — "
            "remote_ok should be FALSE"
        )
    elif fit is True:
        if prefer_global_remote and constraints.appears_global_remote:
            lines.append("- Strong fit: global / work-from-anywhere flexibility (preferred)")
        elif allow_unspecified_location and not constraints.stated_regions:
            lines.append("- Automated check: hire location unspecified — acceptable per candidate preferences")
        else:
            lines.append("- Automated check: hire region appears compatible with candidate preferences")

    return lines


def format_location_constraints_for_prompt(
    constraints: LocationConstraints,
    *,
    acceptable_hire_regions: list[str] | None = None,
    allow_unspecified_location: bool = True,
) -> str:
    if not constraints.summary_lines:
        acceptable_note = ""
        if acceptable_hire_regions:
            acceptable_note = f" Candidate acceptable hire regions: {', '.join(acceptable_hire_regions)}."
        unspecified_note = (
            " Unspecified location is acceptable per candidate preferences."
            if allow_unspecified_location
            else ""
        )
        return (
            "No explicit country/region constraint detected in location line or opening description."
            f"{acceptable_note}{unspecified_note}"
        )
    return "\n".join(constraints.summary_lines)


def location_constraints_from_job(
    row: dict[str, Any],
    *,
    policy: LocationPolicy | None = None,
    acceptable_hire_regions: list[str] | None = None,
    allow_unspecified_location: bool | None = None,
    prefer_global_remote: bool | None = None,
) -> LocationConstraints:
    if policy is not None:
        acceptable_hire_regions = policy.acceptable_hire_regions or acceptable_hire_regions
        if allow_unspecified_location is None:
            allow_unspecified_location = policy.allow_unspecified_location
        if prefer_global_remote is None:
            prefer_global_remote = policy.prefer_global_remote
    return extract_location_constraints(
        title=str(row.get("title") or ""),
        location_text=str(row.get("location_text") or "") or None,
        description_text=str(row.get("description_text") or ""),
        acceptable_hire_regions=acceptable_hire_regions,
        allow_unspecified_location=True if allow_unspecified_location is None else allow_unspecified_location,
        prefer_global_remote=True if prefer_global_remote is None else prefer_global_remote,
    )


def apply_location_guard(
    payload: Any,
    constraints: LocationConstraints,
    *,
    policy: LocationPolicy | None = None,
    acceptable_hire_regions: list[str] | None = None,
    allow_unspecified_location: bool | None = None,
) -> Any:
    """Post-LLM safety: force remote_ok=false when hire region conflicts with preferences."""
    if policy is not None:
        acceptable_hire_regions = policy.acceptable_hire_regions or acceptable_hire_regions
        if allow_unspecified_location is None:
            allow_unspecified_location = policy.allow_unspecified_location
    fit = evaluate_hire_region_fit(
        constraints,
        acceptable_hire_regions,
        allow_unspecified_location=True if allow_unspecified_location is None else allow_unspecified_location,
    )
    mismatch = fit is False or constraints.likely_region_mismatch
    if not mismatch:
        return payload

    if getattr(payload, "remote_ok", None) is True:
        payload.remote_ok = False
        gaps = list(getattr(payload, "risks_or_gaps", None) or [])
        regions = ", ".join(acceptable_hire_regions or []) or "candidate preferences"
        note = f"Hire region incompatible with acceptable regions ({regions})"
        if note not in gaps:
            gaps.insert(0, note)
            payload.risks_or_gaps = gaps[:5]
    return payload
