"""Hacker News "Ask HN: Who is hiring?" via the Algolia HN API.

Two steps: find the newest monthly thread, then read its top-level comments. Each
comment is one posting, conventionally "Company | Role | Location | REMOTE | ...",
though roughly 10-20% are free prose and simply won't parse — those still get
passed through with the whole comment as the description and let the title gate
drop them.

Note the same author posts "Who wants to be hired?" (candidates, not jobs) within
the same second and with an adjacent id, so threads must be matched on title, not
recency. Early in a month the newest thread is still last month's.
"""

from __future__ import annotations

import html
import re
from typing import Any

from pipelines.job_boards.sources.http import json_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
ITEM_URL = "https://hn.algolia.com/api/v1/items/{object_id}"
HIRING_TITLE_PREFIX = "ask hn: who is hiring?"

_TAG_RE = re.compile(r"<[^>]+>")
# Posters separate fields with pipes, em/en dashes or bullets.
_FIELD_SPLIT_RE = re.compile(r"\s*(?:\||—|–|•)\s*")
_URL_RE = re.compile(r"https?://", re.I)


def _plain_text(raw: str) -> str:
    """HN comment text is live HTML plus entity-escaped content."""
    if not raw:
        return ""
    text = raw.replace("<p>", "\n\n")
    text = _TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"[ \t]{2,}", " ", text).strip()


def split_fields(first_line: str) -> list[str]:
    return [p.strip() for p in _FIELD_SPLIT_RE.split(first_line) if p.strip()]


def looks_like_posting(first_line: str, text: str) -> bool:
    """Filter out replies and commentary that sit alongside real postings.

    A posting reliably opens with a delimited header ("Company | Role | ...") or
    at least carries a link; a reply like "89k for a sr data engineer in NYC?" has
    neither. Being strict here keeps junk out of the employer mission gate.
    """
    if len(split_fields(first_line)) >= 3:
        return True
    if len(split_fields(first_line)) == 2 and _URL_RE.search(text):
        return True
    return False


def find_latest_thread(client: Any) -> dict[str, Any] | None:
    resp = client.get(
        SEARCH_URL,
        params={"tags": "story,author_whoishiring", "hitsPerPage": 20},
    )
    resp.raise_for_status()
    hits = resp.json().get("hits") or []
    for hit in hits:  # already newest-first
        title = str(hit.get("title") or "").strip().lower()
        if title.startswith(HIRING_TITLE_PREFIX):
            return hit
    return None


def fetch_jobs(*, max_comments: int = 400, **_kwargs: object) -> JobBoardFetchResult:
    try:
        with json_client(timeout=60.0) as client:
            thread = find_latest_thread(client)
            if thread is None:
                return JobBoardFetchResult(
                    source="hn_whoishiring",
                    ok=False,
                    method="api",
                    job_count=0,
                    error="no 'Who is hiring?' thread found",
                    notes=SEARCH_URL,
                )
            object_id = str(thread.get("objectID") or "")
            resp = client.get(ITEM_URL.format(object_id=object_id))
            resp.raise_for_status()
            item = resp.json()
    except Exception as exc:  # noqa: BLE001
        return JobBoardFetchResult(
            source="hn_whoishiring",
            ok=False,
            method="api",
            job_count=0,
            error=str(exc),
            notes=SEARCH_URL,
        )

    thread_title = str(thread.get("title") or "")
    jobs: list[dict[str, Any]] = []
    skipped = 0
    for child in (item.get("children") or [])[:max_comments]:
        if not isinstance(child, dict):
            continue
        text = _plain_text(str(child.get("text") or ""))
        if not text:
            continue  # deleted comment
        first_line = text.split("\n", 1)[0].strip()
        if not looks_like_posting(first_line, text):
            skipped += 1
            continue
        parts = split_fields(first_line)
        jobs.append(
            {
                "id": child.get("id"),
                "author": child.get("author"),
                "created_at": child.get("created_at"),
                "text": text,
                "first_line": first_line,
                "parts": parts,
                "thread_title": thread_title,
                "thread_id": object_id,
            }
        )

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="hn_whoishiring",
        ok=True,
        method="api",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=f"{thread_title} ({len(jobs)} postings, {skipped} non-postings skipped)",
        jobs=jobs,
    )
