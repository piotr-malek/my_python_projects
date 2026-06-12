"""Cheap title/keyword gate before LLM scoring."""

from __future__ import annotations

# Titles with "engineer" but none of these qualifiers are too broad (e.g. software engineer).
_ENGINEER_ROLE_QUALIFIERS = frozenset(
    {
        "data",
        "analytics",
        "analytic",
        "ai ",
        " ai",
        "artificial intelligence",
        "machine learning",
        " ml ",
        "ml ",
        "integration",
        "integrations",
        "etl",
        "pipeline",
        "platform engineer",
    }
)


def _engineer_without_role_family(title: str) -> bool:
    """Reject generic *engineer* titles that are not data/analytics/AI/integration roles."""
    if "engineer" not in title:
        return False
    return not any(q in title for q in _ENGINEER_ROLE_QUALIFIERS)


def prefilter_title(title: str, *, include_keywords: list[str], exclude_keywords: list[str]) -> bool:
    t = (title or "").lower().strip()
    if not t:
        return False
    for ex in exclude_keywords:
        if ex and ex in t:
            return False
    matched = False
    for kw in include_keywords:
        if kw and kw in t:
            matched = True
            break
    if not matched:
        return False
    if _engineer_without_role_family(t):
        return False
    return True
