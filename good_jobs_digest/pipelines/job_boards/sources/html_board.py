"""Generic HTML job board scraper for nonprofit/regional listings."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import Any
from urllib.parse import urljoin

from pipelines.job_boards.sources.http import browser_client

logger = logging.getLogger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def scrape_html_board(
    *,
    source_id: str,
    list_url: str,
    mission_category: str,
    card_pattern: str | None = None,
) -> list[dict[str, Any]]:
    """Best-effort job card extraction from listing HTML."""
    try:
        with browser_client() as client:
            resp = client.get(list_url, headers=_BROWSER_HEADERS)
            resp.raise_for_status()
            html = resp.text
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s fetch failed: %s", source_id, exc)
        return []

    jobs: list[dict[str, Any]] = []
    seen: set[str] = set()

    if card_pattern:
        pattern = re.compile(card_pattern, re.I | re.S)
        for m in pattern.finditer(html):
            title = _clean(m.group("title") if "title" in m.groupdict() else m.group(1))
            company = _clean(m.group("company")) if "company" in m.groupdict() else ""
            url = _clean(m.group("url")) if "url" in m.groupdict() else ""
            if url and not url.startswith("http"):
                url = urljoin(list_url, url)
            if not title:
                continue
            key = f"{title}|{company}|{url}"
            if key in seen:
                continue
            seen.add(key)
            jid = hashlib.sha256(key.encode()).hexdigest()[:16]
            jobs.append(
                {
                    "id": jid,
                    "title": title,
                    "company_name": company or "Unknown",
                    "url": url or list_url,
                    "description": title,
                    "location": _clean(m.group("location")) if "location" in m.groupdict() else "",
                    "mission_category": mission_category,
                    "source": source_id,
                }
            )
        return jobs

    # fallback: h2/h3 + nearby links
    for m in re.finditer(
        r'<a[^>]+href="([^"]+)"[^>]*>\s*(?:<[^>]+>\s*)*([^<]{5,120})\s*</a>',
        html,
        re.I,
    ):
        url, title = m.group(1), _clean(m.group(2))
        if not title or any(x in title.lower() for x in ("login", "sign up", "cookie")):
            continue
        if not url.startswith("http"):
            url = urljoin(list_url, url)
        key = f"{title}|{url}"
        if key in seen:
            continue
        seen.add(key)
        jid = hashlib.sha256(key.encode()).hexdigest()[:16]
        jobs.append(
            {
                "id": jid,
                "title": title,
                "company_name": "Unknown",
                "url": url,
                "description": title,
                "location": "",
                "mission_category": mission_category,
                "source": source_id,
            }
        )
    logger.info("%s: scraped %s listings", source_id, len(jobs))
    return jobs


def _clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()
