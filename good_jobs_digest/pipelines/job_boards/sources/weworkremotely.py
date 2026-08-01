"""We Work Remotely RSS category feeds.

WWR has no data/analytics category — data roles are scattered across the
programming feeds (and are frequently miscategorised), so we poll the
engineering-ish feeds and let the title prefilter sort it out. The HTML site is
Cloudflare-guarded, but the .rss paths serve any client.

Items carry custom <region>/<country> elements that state hireable regions
explicitly, which makes this one of the better EU signals available.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

from pipelines.job_boards.sources.http import browser_client, polite_sleep
from pipelines.job_boards.sources.types import JobBoardFetchResult

FEED_URL = "https://weworkremotely.com/categories/{slug}.rss"
# Verified live; feeds where data/AI roles actually appear.
DEFAULT_CATEGORIES = (
    "remote-back-end-programming-jobs",
    "remote-full-stack-programming-jobs",
    "remote-devops-sysadmin-jobs",
    "remote-programming-jobs",
    "all-other-remote-jobs",
)

_ITEM_FIELDS = ("title", "region", "country", "state", "skills", "category", "type", "pubDate", "link", "guid")


def _text(item: ET.Element, tag: str) -> str:
    node = item.find(tag)
    return (node.text or "").strip() if node is not None and node.text else ""


def _parse_feed(xml_text: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    out: list[dict[str, Any]] = []
    for item in root.iter("item"):
        row = {field: _text(item, field) for field in _ITEM_FIELDS}
        desc = item.find("description")
        row["description"] = (desc.text or "") if desc is not None else ""
        if row["title"] and (row["link"] or row["guid"]):
            out.append(row)
    return out


def fetch_jobs(
    *,
    categories: tuple[str, ...] | list[str] = DEFAULT_CATEGORIES,
    **_kwargs: object,
) -> JobBoardFetchResult:
    jobs: list[dict[str, Any]] = []
    notes: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()

    with browser_client(timeout=45.0) as client:
        for slug in categories:
            url = FEED_URL.format(slug=slug)
            try:
                resp = client.get(url)
                resp.raise_for_status()
                items = _parse_feed(resp.text)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{slug}: {exc}")
                continue
            fresh = 0
            for item in items:
                key = item.get("guid") or item.get("link") or ""
                if key and key in seen:
                    continue
                if key:
                    seen.add(key)
                item["wwr_category_slug"] = slug
                jobs.append(item)
                fresh += 1
            notes.append(f"{slug}={fresh}")
            polite_sleep(0.4)

    if not jobs and errors:
        return JobBoardFetchResult(
            source="weworkremotely",
            ok=False,
            method="rss",
            job_count=0,
            error="; ".join(errors[:3]),
            notes="; ".join(notes),
        )

    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="weworkremotely",
        ok=True,
        method="rss",
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes="; ".join(notes + ([f"errors: {len(errors)}"] if errors else [])),
        jobs=jobs,
    )
