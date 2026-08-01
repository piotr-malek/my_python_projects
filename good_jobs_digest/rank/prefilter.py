"""Cheap title-only gate before LLM scoring.

Deterministic filtering happens on the position title alone; everything else
(location, remote, timezone, seniority nuance) is judged by the LLM scorer.

Rules, in order:
1. Hard excludes (word-boundary): always reject (intern, recruiter, sales, ...).
2. Discipline excludes (word-boundary): reject only when the title has NO
   data/AI role-family qualifier — so "Data Software Engineer" and
   "AI Software Engineer" pass while a bare "Software Engineer" does not.
3. Seniority excludes (word-boundary): reject titles outside the target level
   (senior/staff/lead/... and graduate/trainee/...), configurable via env.
4. Include: title must match a target keyword (word-boundary), or contain a
   role-family qualifier together with an engineer/engineering token.

No ML/machine-learning qualifiers on purpose — ML roles are not targeted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

# Role-family qualifiers marking a title as data/analytics/AI engineering.
_ROLE_FAMILY_RES: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p)
    for p in (
        r"\bdata\b",
        r"\banalytics?\b",
        r"\bai\b",
        r"\bartificial intelligence\b",
        r"\betl\b",
        r"\belt\b",
        r"\bpipelines?\b",
        r"\bintegrations?\b",
        r"\bplatform\b",
    )
)

_ENGINEER_TOKEN_RE = re.compile(r"\bengineer(?:ing)?\b")

# Exclude keywords that describe an engineering discipline which may legitimately
# co-occur with a data/AI qualifier. They reject only qualifier-less titles.
_DISCIPLINE_EXCLUDES = frozenset(
    {
        "software engineer",
        "software developer",
        "frontend engineer",
        "front-end engineer",
        "backend engineer",
        "back-end engineer",
        "full stack",
        "fullstack",
        "devops",
        "site reliability",
        "sre",
        "product engineer",
        "qa engineer",
        "quality engineer",
        "mobile engineer",
        "ios engineer",
        "android engineer",
    }
)


@lru_cache(maxsize=1024)
def _word_re(keyword: str) -> re.Pattern[str]:
    return re.compile(r"\b" + re.escape(keyword) + r"\b")


def _has_role_family(title: str) -> bool:
    return any(p.search(title) for p in _ROLE_FAMILY_RES)


@dataclass(frozen=True)
class TitleVerdict:
    passed: bool
    reason: str | None = None


def evaluate_title(
    title: str,
    *,
    include_keywords: list[str],
    exclude_keywords: list[str],
    seniority_exclude_keywords: Iterable[str] = (),
) -> TitleVerdict:
    """Apply the title gate; returns pass/fail plus a short reason on failure."""
    t = (title or "").lower().strip()
    if not t:
        return TitleVerdict(False, "empty title")

    role_family = _has_role_family(t)

    for ex in exclude_keywords:
        if not ex or not _word_re(ex).search(t):
            continue
        if ex in _DISCIPLINE_EXCLUDES and role_family:
            continue  # e.g. "Data Software Engineer" keeps its data qualifier
        return TitleVerdict(False, f"excluded keyword: {ex}")

    for kw in seniority_exclude_keywords:
        if kw and _word_re(kw).search(t):
            return TitleVerdict(False, f"seniority: {kw}")

    matched = any(kw and _word_re(kw).search(t) for kw in include_keywords)
    if not matched and role_family and _ENGINEER_TOKEN_RE.search(t):
        matched = True
    if not matched:
        return TitleVerdict(False, "no target role keyword")

    return TitleVerdict(True, None)


def prefilter_title(
    title: str,
    *,
    include_keywords: list[str],
    exclude_keywords: list[str],
    seniority_exclude_keywords: Iterable[str] = (),
) -> bool:
    return evaluate_title(
        title,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        seniority_exclude_keywords=seniority_exclude_keywords,
    ).passed
