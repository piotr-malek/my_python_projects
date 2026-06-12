"""Tech Jobs for Good — Django HTML (Cloudflare may block datacenter IPs)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from pipelines.job_boards.sources.http import DEFAULT_HEADERS, browser_client
from pipelines.job_boards.sources.types import JobBoardFetchResult

BASE_URL = "https://techjobsforgood.com"
JOBS_URL = f"{BASE_URL}/jobs/?sort_by=date"
JOB_URL = f"{BASE_URL}/jobs/{{job_id}}/"


def _looks_blocked(html: str, status_code: int) -> bool:
    if status_code == 403:
        return True
    lowered = html.lower()
    return "cloudflare" in lowered and (
        "blocked" in lowered or "attention required" in lowered
    )


def _fetch_with_httpx() -> tuple[str, int, str]:
    with browser_client() as client:
        resp = client.get(JOBS_URL)
        return resp.text, resp.status_code, "httpx"


def _fetch_with_cloudscraper() -> tuple[str, int, str]:
    import cloudscraper  # type: ignore[import-untyped]

    scraper = cloudscraper.create_scraper()
    resp = scraper.get(JOBS_URL, headers=DEFAULT_HEADERS, timeout=30)
    return resp.text, resp.status_code, "cloudscraper"


def _fetch_with_curl_cffi() -> tuple[str, int, str]:
    from curl_cffi import requests  # type: ignore[import-untyped]

    resp = requests.get(
        JOBS_URL,
        impersonate="chrome120",
        timeout=30,
    )
    return resp.text, resp.status_code, "curl_cffi"


def _fetch_with_playwright() -> tuple[str, int, str]:
    from playwright.sync_api import sync_playwright

    # Prefer user-level browser cache when sandbox cache is empty.
    os.environ.setdefault(
        "PLAYWRIGHT_BROWSERS_PATH",
        str(Path.home() / "Library/Caches/ms-playwright"),
    )
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        resp = page.goto(JOBS_URL, wait_until="domcontentloaded", timeout=90_000)
        page.wait_for_timeout(5_000)
        html = page.content()
        status = resp.status if resp else 0
        browser.close()
    return html, status, "playwright"


def _attempt_fetchers() -> tuple[str, int, str]:
    strategies: list[tuple[str, Callable[[], tuple[str, int, str]]]] = [
        ("httpx", _fetch_with_httpx),
        ("cloudscraper", _fetch_with_cloudscraper),
        ("curl_cffi", _fetch_with_curl_cffi),
        ("playwright", _fetch_with_playwright),
    ]
    errors: list[str] = []
    for name, fn in strategies:
        try:
            html, status, method = fn()
            if not _looks_blocked(html, status):
                return html, status, method
            errors.append(f"{name}: blocked (HTTP {status})")
        except ImportError:
            errors.append(f"{name}: optional dependency not installed")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")
    raise RuntimeError("; ".join(errors))


def _fetch_wayback_snapshot() -> tuple[str, int, str]:
    """Archived HTML (development fallback when live site blocks bots)."""
    import httpx

    url = (
        "https://web.archive.org/web/20230115120000id_/"
        "https://techjobsforgood.com/jobs/?sort_by=date"
    )
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return resp.text, resp.status_code, "wayback:2023-01-15"


def fetch_jobs_from_wayback(*, limit: int = 20) -> JobBoardFetchResult:
    html, _status, method = _fetch_wayback_snapshot()
    jobs = _parse_listing_cards(html)[:limit]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="techjobsforgood",
        ok=bool(jobs),
        method=method,
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes="Archived snapshot only — not live listings.",
    )


def _parse_listing_cards(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: list[dict[str, Any]] = []
    cards = soup.select("div.ui.card")
    if not cards:
        # Fallback: any job links on the page
        for link in soup.find_all("a", href=True):
            m = re.search(r"/jobs/(\d+)/?", link["href"])
            if not m:
                continue
            title = link.get_text(" ", strip=True)
            if len(title) < 8 or title.lower() == "view job":
                continue
            jobs.append(
                {
                    "id": m.group(1),
                    "title": title,
                    "url": urljoin(BASE_URL, link["href"]),
                }
            )
        return _dedupe(jobs)

    for card in cards:
        link = card.select_one('a[href*="/jobs/"]')
        if not link:
            continue
        m = re.search(r"/jobs/(\d+)/?", link.get("href", ""))
        if not m:
            continue
        text_lines = [ln.strip() for ln in card.get_text("\n").split("\n") if ln.strip()]
        title = text_lines[0] if text_lines else None
        company = text_lines[1] if len(text_lines) > 1 else None
        location = text_lines[2] if len(text_lines) > 2 else None
        jobs.append(
            {
                "id": m.group(1),
                "title": title,
                "company_name": company,
                "location": location,
                "card_text": text_lines,
                "url": urljoin(BASE_URL, f"/jobs/{m.group(1)}/"),
            }
        )
    return _dedupe(jobs)


def _dedupe(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for job in jobs:
        jid = str(job.get("id", ""))
        if not jid or jid in seen:
            continue
        seen.add(jid)
        out.append(job)
    return out


def fetch_job_detail(job_id: int | str) -> dict[str, Any]:
    url = JOB_URL.format(job_id=job_id)
    with browser_client() as client:
        resp = client.get(url)
        resp.raise_for_status()
        html = resp.text
        status = resp.status_code
    if _looks_blocked(html, status):
        raise RuntimeError("Tech Jobs for Good blocked the job detail request (Cloudflare)")
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1")
    meta = soup.select_one('meta[name="description"]')
    return {
        "id": str(job_id),
        "title": title.get_text(strip=True) if title else None,
        "meta_description": meta.get("content") if meta else None,
        "text": soup.get_text("\n", strip=True)[:4000],
        "url": url,
    }


def fetch_jobs(
    *,
    limit: int = 20,
    allow_wayback_fallback: bool = False,
    **_kwargs: object,
) -> JobBoardFetchResult:
    try:
        html, status, method = _attempt_fetchers()
    except Exception as exc:  # noqa: BLE001
        if allow_wayback_fallback:
            return fetch_jobs_from_wayback(limit=limit)
        return JobBoardFetchResult(
            source="techjobsforgood",
            ok=False,
            method="none",
            job_count=0,
            error=str(exc),
            notes=(
                f"{JOBS_URL} — Cloudflare blocks many automated clients. "
                "Try from a residential IP, export a cf_clearance cookie, install "
                "cloudscraper / curl_cffi / playwright, or pass --tjfg-wayback."
            ),
        )

    jobs = _parse_listing_cards(html)[:limit]
    sample = jobs[0] if jobs else {}
    return JobBoardFetchResult(
        source="techjobsforgood",
        ok=bool(jobs),
        method=method,
        job_count=len(jobs),
        available_fields=sorted(sample.keys()) if sample else [],
        sample_job=sample,
        notes=(
            "Listing cards include title, company, location, tags, salary when shown. "
            "Use /jobs/{id}/ for full description when fetch succeeds."
        ),
    )
