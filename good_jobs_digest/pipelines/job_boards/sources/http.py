"""HTTP helpers for job-board probes."""

from __future__ import annotations

import time
from typing import Any

import httpx

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# Backwards-compatible alias
DEFAULT_HEADERS = BROWSER_HEADERS

JSON_HEADERS = {
    **BROWSER_HEADERS,
    "Accept": "application/json",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
}


def browser_client(timeout: float = 30.0, **kwargs: Any) -> httpx.Client:
    return httpx.Client(
        headers=DEFAULT_HEADERS,
        timeout=timeout,
        follow_redirects=True,
        **kwargs,
    )


def json_client(timeout: float = 30.0, **kwargs: Any) -> httpx.Client:
    return httpx.Client(
        headers=JSON_HEADERS,
        timeout=timeout,
        follow_redirects=True,
        **kwargs,
    )


def polite_sleep(seconds: float = 0.5) -> None:
    if seconds > 0:
        time.sleep(seconds)
