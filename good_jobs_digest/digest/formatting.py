"""Format job rows for digest email (markdown)."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from typing import Any


def _norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


# A trailing " - Berlin, Germany" is the same opening as " - Cork, Ireland"; a
# trailing " - GTM" or " - Analytics Platform" is a different role. Both shapes are
# common in real titles, so the location strip has to be narrow: a comma, at most
# four words, and no word that could name a role or a team.
_TITLE_TAIL_RE = re.compile(r"\s[-–—]\s([^-–—]+)$")
_ROLE_TAIL_WORDS = frozenset(
    {
        "ai", "analyst", "analytics", "backend", "bi", "cloud", "data", "dbt",
        "delivery", "developer", "devops", "engineer", "engineering", "etl", "frontend",
        "infra", "infrastructure", "intelligence", "lead", "manager", "ml", "ops",
        "operations", "platform", "principal", "product", "reporting", "research",
        "science", "security", "senior", "services", "software", "staff", "systems",
        "team", "tech", "warehouse", "web",
    }
)


def strip_location_suffix(title: str) -> str:
    """Drop a trailing ' - City, Country' so per-city reposts collapse together."""
    match = _TITLE_TAIL_RE.search(title)
    if not match:
        return title
    tail = match.group(1).strip()
    if "," not in tail:
        return title
    words = re.findall(r"[A-Za-z]+", tail.lower())
    if not words or len(words) > 4 or any(w in _ROLE_TAIL_WORDS for w in words):
        return title
    return title[: match.start()].strip() or title


def _dedupe_groups(jobs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group postings that are the same opening.

    Two rows belong together if they share a canonical_job_id (cross-source
    duplicate, possibly with differing company spellings) *or* the same company
    and title once a trailing location is stripped. Both tests are needed:
    canonical_job_id hashes company + title + location + week, so per-location
    reposts each get their own — which is why keying on it alone let four
    Speechify postings and three PVcase postings through as separate openings.
    """
    parent: dict[Any, Any] = {}

    def find(key: Any) -> Any:
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(a: Any, b: Any) -> None:
        root_a, root_b = find(a), find(b)
        if root_a != root_b:
            parent[root_a] = root_b

    for i, job in enumerate(jobs):
        node = ("row", i)
        company = _norm_key(str(job.get("company_name") or ""))
        title = _norm_key(strip_location_suffix(str(job.get("title") or "")))
        union(node, ("title", company, title))
        ckey = str(job.get("canonical_job_id") or "").strip()
        if ckey:
            union(node, ("canonical", ckey))

    groups: dict[Any, list[dict[str, Any]]] = {}
    for i, job in enumerate(jobs):
        groups.setdefault(find(("row", i)), []).append(job)
    return list(groups.values())


def dedupe_by_company_title(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per opening, keeping the highest combined_score.

    Collapsed rows are counted in `duplicate_count` so the digest can say how
    many postings a line stands for rather than hiding them.
    """
    out: list[dict[str, Any]] = []
    for group in _dedupe_groups(jobs):
        best = max(group, key=lambda j: float(j.get("combined_score") or 0))
        if len(group) > 1:
            best = {**best, "duplicate_count": len(group)}
        out.append(best)
    out.sort(key=lambda x: (-float(x.get("combined_score") or 0), str(x.get("company_name") or "")))
    return out


def parse_job_datetime(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def effective_posted_datetime(job: dict[str, Any]) -> datetime | None:
    """Prefer ATS/board publish date; fall back to when we first ingested the job."""
    return parse_job_datetime(job.get("posted_at")) or parse_job_datetime(job.get("first_seen_at"))


def is_new_since(job: dict[str, Any], cutoff: datetime) -> bool:
    dt = effective_posted_datetime(job)
    return dt is not None and dt >= cutoff


def _llm_payload(job: dict[str, Any]) -> dict[str, Any]:
    raw = job.get("llm_json")
    if not raw:
        return {}
    try:
        return json.loads(raw) if isinstance(raw, str) else dict(raw)
    except Exception:
        return {}


def _truncate_sentences(text: str, *, max_sentences: int = 2, max_chars: int = 320) -> str:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", text)
    out: list[str] = []
    length = 0
    for part in parts:
        chunk = part.strip()
        if not chunk:
            continue
        if not chunk.endswith((".", "!", "?")):
            chunk += "."
        if out and length + len(chunk) + 1 > max_chars:
            break
        out.append(chunk)
        length += len(chunk) + 1
        if len(out) >= max_sentences:
            break
    if not out:
        snippet = text[:max_chars].rstrip()
        if len(text) > max_chars:
            snippet += "…"
        return snippet
    return " ".join(out)


def _role_blurb(job: dict[str, Any], llm: dict[str, Any]) -> str:
    summary = (llm.get("one_line_summary") or "").strip()
    if summary:
        return _truncate_sentences(summary, max_sentences=2, max_chars=240)
    desc = (job.get("description_text") or "").strip()
    if not desc:
        return ""
    lowered = desc.lower()
    for marker in (
        "requirements:",
        "qualifications:",
        "what you'll do",
        "what you will",
        "responsibilities:",
        "about the role",
    ):
        idx = lowered.find(marker)
        if idx > 60:
            desc = desc[:idx].strip()
            break
    return _truncate_sentences(desc, max_sentences=2, max_chars=240)


_ROLE_SECTION_MARKERS = (
    "about the role",
    "what you'll do",
    "what you will",
    "responsibilities:",
    "requirements:",
    "qualifications:",
    "the role",
    "your role",
)

_ABOUT_SECTION_MARKERS = (
    "about the company",
    "about us",
    "who we are",
    "about our company",
    "about the organization",
    "about the organisation",
    "company overview",
    "our mission",
    "why join us",
)


def _strip_md_noise(text: str) -> str:
    return re.sub(r"\*\*", "", text).strip()


def _slice_until_role_section(text: str) -> str:
    lowered = text.lower()
    end = len(text)
    for marker in _ROLE_SECTION_MARKERS:
        idx = lowered.find(marker)
        if idx != -1 and idx < end:
            end = idx
    return text[:end].strip()


def _extract_about_section(desc: str) -> str:
    """Pull prose from an explicit About-the-company section, if present."""
    cleaned = _strip_md_noise(desc)
    lowered = cleaned.lower()
    for marker in _ABOUT_SECTION_MARKERS:
        idx = lowered.find(marker)
        if idx == -1:
            continue
        start = idx + len(marker)
        chunk = _slice_until_role_section(cleaned[start:]).strip(" -–—:\n")
        if len(chunk) >= 40:
            return _truncate_sentences(chunk, max_sentences=2, max_chars=200)
    return ""


def _opening_org_paragraph(desc: str, company: str) -> str:
    """Use the opening paragraph only when it reads like org/mission context, not the role."""
    cleaned = _strip_md_noise(desc)
    head = _slice_until_role_section(cleaned)
    if not head or len(head) < 40:
        return ""

    first_para = re.split(r"\n\s*\n", head, maxsplit=1)[0].strip()
    first_para = re.sub(r"\s+", " ", first_para)
    lowered = first_para.lower()

    if any(
        lowered.startswith(p)
        for p in (
            "you will",
            "you'll",
            "we're looking",
            "we are looking",
            "the ideal candidate",
            "as a ",
        )
    ):
        return ""

    company_l = company.lower()
    org_signals = (
        "mission",
        "nonprofit",
        "non-profit",
        "ngo",
        "founded",
        "organization",
        "organisation",
        "we are a",
        "we're a",
        "our company",
        "our team",
    )
    has_signal = (company_l and company_l in lowered) or any(s in lowered for s in org_signals)
    if not has_signal:
        return ""

    return _truncate_sentences(first_para, max_sentences=2, max_chars=200)


def _company_blurb(job: dict[str, Any]) -> str:
    company = (job.get("company_name") or "").strip()
    desc = (job.get("description_text") or "").strip()
    if not desc:
        return ""

    about = _extract_about_section(desc)
    if about:
        return about

    if company:
        return _opening_org_paragraph(desc, company)

    return ""


def format_score(job: dict[str, Any]) -> str | None:
    score = job.get("combined_score")
    if score is None:
        return None
    try:
        return f"{float(score):.1f}"
    except (TypeError, ValueError):
        return None


def job_bullet_line(job: dict[str, Any]) -> str:
    """Single compact bullet: linked title, company, score, brief role + org blurbs."""
    title = (job.get("title") or "Untitled role").strip()
    company = (job.get("company_name") or "Unknown company").strip()
    url = (job.get("url") or "").strip()
    llm = _llm_payload(job)

    duplicates = int(job.get("duplicate_count") or 0)
    if duplicates > 1:
        # This line stands for several cities, so naming one of them misleads;
        # the link still points at the highest-scoring posting.
        title = strip_location_suffix(title)

    if url:
        head = f"[**{title}**]({url}) at **{company}**"
    else:
        head = f"**{title}** at **{company}**"

    score = format_score(job)
    if score:
        head += f" · Score **{score}**"

    # Say what was collapsed instead of silently dropping it: the same role posted
    # per city is one opening, but knowing there are five is useful.
    if duplicates > 1:
        head += f" · {duplicates} postings"

    role = _role_blurb(job, llm)
    org = _company_blurb(job)

    body_parts: list[str] = []
    if role:
        body_parts.append(role)
    if org and org.lower() not in (role or "").lower():
        body_parts.append(org)

    if body_parts:
        return f"- {head} — {' '.join(body_parts)}"
    return f"- {head}"


def job_block_lines(job: dict[str, Any]) -> list[str]:
    """One markdown bullet per job (compact digest block)."""
    return [job_bullet_line(job)]
