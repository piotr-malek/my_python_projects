"""Throttled HTTP with browser-like headers and optional proxy rotation."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from pipelines.job_boards.sources.http import BROWSER_HEADERS, JSON_HEADERS
from pipelines.job_boards.sources.proxy_pool import ProxyEndpoint, ProxyPool

logger = logging.getLogger(__name__)


class ResilientHttp:
    """Sequential client: polite delay, retries, proxy fallback when blocked."""

    def __init__(
        self,
        *,
        delay_ms: int = 1500,
        proxy_pool: ProxyPool | None = None,
        timeout: float = 45.0,
    ):
        self._delay_ms = max(0, int(delay_ms))
        self._proxy_pool = proxy_pool
        self._timeout = timeout
        self._last_end = 0.0

    def _throttle(self) -> None:
        if self._delay_ms <= 0:
            return
        gap = self._delay_ms / 1000.0
        now = time.monotonic()
        wait_s = gap - (now - self._last_end)
        if wait_s > 0:
            time.sleep(wait_s)

    def _mark_done(self) -> None:
        self._last_end = time.monotonic()

    @staticmethod
    def looks_blocked(html: str, status_code: int) -> bool:
        if status_code == 403 or status_code == 429:
            return True
        lowered = (html or "").lower()
        if "cloudflare" in lowered and (
            "blocked" in lowered
            or "attention required" in lowered
            or "sorry, you have been blocked" in lowered
        ):
            return True
        return False

    def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        try_direct: bool = True,
        try_proxies: bool = True,
        proxy_only: bool = False,
    ) -> httpx.Response:
        """GET with direct attempt first, then rotate proxies until success."""
        hdrs = dict(headers or BROWSER_HEADERS)
        errors: list[str] = []

        if try_direct and not proxy_only:
            try:
                self._throttle()
                with httpx.Client(
                    headers=hdrs,
                    timeout=self._timeout,
                    follow_redirects=True,
                ) as client:
                    resp = client.get(url)
                self._mark_done()
                if not self.looks_blocked(resp.text, resp.status_code):
                    return resp
                errors.append(f"direct: blocked (HTTP {resp.status_code})")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"direct: {exc}")

        if try_proxies and self._proxy_pool:
            for ep in self._proxy_pool.cycle():
                try:
                    self._throttle()
                    with httpx.Client(
                        proxy=ep.url,
                        headers=hdrs,
                        timeout=self._timeout,
                        follow_redirects=True,
                    ) as client:
                        resp = client.get(url)
                    self._mark_done()
                    if not self.looks_blocked(resp.text, resp.status_code):
                        logger.debug("OK via proxy %s for %s", ep.label(), url[:60])
                        return resp
                    errors.append(f"{ep.label()}: blocked (HTTP {resp.status_code})")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{ep.label()}: {exc}")

        raise RuntimeError(f"All fetch attempts failed for {url}: {'; '.join(errors)}")

    def post_json(
        self,
        url: str,
        *,
        body: dict[str, Any] | str,
        extra_headers: dict[str, str] | None = None,
        try_direct: bool = True,
        try_proxies: bool = False,
    ) -> httpx.Response:
        hdrs = {**JSON_HEADERS, **(extra_headers or {})}
        content = body if isinstance(body, str) else __import__("json").dumps(body)
        errors: list[str] = []

        if try_direct:
            try:
                self._throttle()
                with httpx.Client(
                    headers=hdrs,
                    timeout=self._timeout,
                    follow_redirects=True,
                ) as client:
                    resp = client.post(url, content=content)
                self._mark_done()
                if resp.status_code < 500:
                    return resp
                errors.append(f"direct: HTTP {resp.status_code}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"direct: {exc}")

        if try_proxies and self._proxy_pool:
            for ep in self._proxy_pool.cycle():
                try:
                    self._throttle()
                    with httpx.Client(
                        proxy=ep.url,
                        headers=hdrs,
                        timeout=self._timeout,
                        follow_redirects=True,
                    ) as client:
                        resp = client.post(url, content=content)
                    self._mark_done()
                    if resp.status_code < 500:
                        return resp
                    errors.append(f"{ep.label()}: HTTP {resp.status_code}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{ep.label()}: {exc}")

        raise RuntimeError(f"POST failed for {url}: {'; '.join(errors)}")
