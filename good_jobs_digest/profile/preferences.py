"""Load and render structured candidate preferences for LLM scoring."""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

ROLE_LABELS = {
    "analytics_engineer": "Analytics engineer (dbt, metrics, data modeling, BI platforms)",
    "ai_ml_engineer": "AI / ML engineer (modeling, MLOps, LLM systems, experimentation)",
    "data_engineer": "Data engineer (pipelines, warehousing, orchestration, platform)",
    "data_integration_etl": "Data integration / ETL engineer",
    "pure_devops_or_sre": "Pure DevOps/SRE/platform ops without analytics/ML work",
}

SENIORITY_LABELS = {
    "intern": "Intern",
    "graduate": "Graduate / new grad",
    "junior": "Junior",
    "mid": "Mid-level",
    "medium": "Mid-level",
    "senior": "Senior",
    "staff": "Staff",
    "principal": "Principal",
    "lead": "Lead (IC)",
    "manager": "Engineering manager",
    "director": "Director+",
    "vp": "VP+",
    "head_of": "Head of / C-level",
    "c_level": "C-level",
}


def _default_preferences_path() -> Path:
    return Path(__file__).resolve().parent / "preferences.yaml"


def load_preferences(path: Path | None = None) -> dict[str, Any]:
    p = path or _default_preferences_path()
    if not p.exists():
        return {}
    if yaml is None:
        raise RuntimeError("PyYAML is required: pip install pyyaml")
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}
    return data


def _is_set(value: Any) -> bool:
    """True when a preference value is present (null/empty → omit from prompt)."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def _fmt_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "no" if value is False else "yes"
    return str(value)


def _section(title: str, body: str) -> str | None:
    body = body.strip()
    if not body:
        return None
    return f"## {title}\n{body}"


def _role_list(key: str, items: list[str] | None) -> str:
    if not items:
        return ""
    lines = [f"  {key}:"]
    for item in items:
        label = ROLE_LABELS.get(item, item.replace("_", " "))
        lines.append(f"    - {label}")
    return "\n".join(lines)


def _seniority_block(seniority: dict[str, Any]) -> str:
    if not seniority:
        return ""
    parts: list[str] = []
    target = seniority.get("target")
    if _is_set(target):
        parts.append(f"  Target: {SENIORITY_LABELS.get(str(target), target)}")
    for key in ("acceptable", "too_junior", "too_senior"):
        vals = seniority.get(key) or []
        if vals:
            labels = ", ".join(SENIORITY_LABELS.get(str(v), str(v)) for v in vals)
            parts.append(f"  {key.replace('_', ' ').title()}: {labels}")
    return "\n".join(parts)


def _dict_bullets(mapping: dict[str, Any], fields: tuple[tuple[str, str], ...]) -> str:
    lines: list[str] = []
    for yaml_key, label in fields:
        value = mapping.get(yaml_key)
        if not _is_set(value):
            continue
        if isinstance(value, list):
            lines.append(f"- {label}: {', '.join(str(v) for v in value)}")
        else:
            lines.append(f"- {label}: {_fmt_scalar(value)}")
    return "\n".join(lines)


def render_scoring_context(prefs: dict[str, Any]) -> str:
    """Render structured preferences as a scoring rubric block for LLM prompts."""
    if not prefs:
        return "(No structured preferences file — fill profile/preferences.yaml)"

    sections: list[str] = []

    role = prefs.get("role_focus") or {}
    role_parts = [
        _role_list("Primary (score role_relevance 80+ if strong match)", role.get("primary")),
        _role_list("Secondary (60–79 if good fit)", role.get("secondary")),
        _role_list("Avoid (cap role_relevance at 40 unless description clearly differs)", role.get("avoid")),
    ]
    role_body = "\n".join(p for p in role_parts if p)
    if block := _section("Role focus", role_body):
        sections.append(block)

    if _is_set(prefs.get("role_shape")):
        if block := _section("Ideal role shape", str(prefs["role_shape"]).strip()):
            sections.append(block)

    if block := _section("Seniority", _seniority_block(prefs.get("seniority") or {})):
        sections.append(block)

    skills = prefs.get("skills") or {}
    if block := _section(
        "Skills & stack",
        _dict_bullets(
            skills,
            (
                ("core_strengths", "Core strengths (boost candidate_fit when job needs these)"),
                ("want_to_use", "Want to use (small boost if present)"),
                ("avoid_or_weak", "Avoid or weak (lower candidate_fit if central to role)"),
                ("languages", "Working languages"),
                ("years_experience", "Years of experience"),
            ),
        ),
    ):
        sections.append(block)

    work = prefs.get("work_arrangement") or {}
    if block := _section(
        "Work arrangement",
        _dict_bullets(
            work,
            (
                ("remote", "Remote"),
                ("hybrid_max_days_per_week", "Max hybrid days/week"),
                ("relocation", "Relocation"),
                ("travel_max_percent", "Max travel %"),
            ),
        ),
    ):
        sections.append(block)

    loc = prefs.get("location") or {}
    if block := _section(
        "Location & timezone",
        _dict_bullets(
            loc,
            (
                ("based_in", "Based in"),
                ("timezone", "Timezone"),
                ("required_overlap", "Required overlap"),
                ("acceptable_hire_regions", "Acceptable hire region(s)"),
                ("allow_unspecified_location", "Unspecified location OK"),
                ("prefer_global_remote", "Prefer global/anywhere remote"),
                ("unacceptable", "Unacceptable"),
            ),
        ),
    ):
        sections.append(block)

    comp = prefs.get("compensation") or {}
    if block := _section(
        "Compensation",
        _dict_bullets(
            comp,
            (
                ("currency", "Currency"),
                ("minimum_annual_gross", "Minimum annual gross"),
                ("contract_types", "Contract types"),
                ("notes", "Notes"),
            ),
        ),
    ):
        sections.append(block)

    mission = prefs.get("mission") or {}
    if block := _section(
        "Mission & industry",
        _dict_bullets(
            mission,
            (
                ("excited_about", "Excited about (boost mission_alignment)"),
                ("neutral", "Neutral"),
                ("deal_breakers", "Deal-breakers (mission_alignment ≤ 20)"),
            ),
        ),
    ):
        sections.append(block)

    org = prefs.get("organization") or {}
    if block := _section(
        "Organization",
        _dict_bullets(
            org,
            (
                ("preferred", "Preferred"),
                ("acceptable", "Acceptable"),
                ("avoid", "Avoid"),
            ),
        ),
    ):
        sections.append(block)

    dbs = [d for d in (prefs.get("deal_breakers") or []) if _is_set(d)]
    if block := _section("Hard deal-breakers", "\n".join(f"- {d}" for d in dbs)):
        sections.append(block)

    return "\n\n".join(sections) if sections else "(No preferences set)"


def digest_remote_only(prefs: dict[str, Any] | None, *, default: bool = True) -> bool:
    """Whether the email digest should include only jobs scored remote_ok=true."""
    if not prefs:
        return default
    digest = prefs.get("digest")
    if isinstance(digest, dict) and "remote_only" in digest:
        return bool(digest["remote_only"])
    return default


def _profile_has_content(text: str) -> bool:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return True
    return False


def build_scoring_input(
    *,
    preferences_path: Path | None = None,
    profile_path: Path | None = None,
) -> str:
    """Combined text injected into scorer prompts: structured prefs + optional freeform profile."""
    prefs = load_preferences(preferences_path)
    structured = render_scoring_context(prefs)
    parts = [
        "### Structured requirements (primary scoring rubric)\n",
        structured,
    ]
    p = profile_path or Path(__file__).resolve().parent / "profile.md"
    if p.exists():
        extra = p.read_text(encoding="utf-8").strip()
        if extra and _profile_has_content(extra):
            parts.extend(["\n### Additional notes (secondary)\n", extra])
    return "\n".join(parts)


def main() -> None:
    """CLI: print rendered scoring context."""
    print(build_scoring_input())


if __name__ == "__main__":
    main()
