"""HTTP helpers and throttling for ATS connectors."""

from __future__ import annotations

import logging
import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class ThrottledHttp:
    """Sequential httpx wrapper with ~global delay between requests."""

    def __init__(self, delay_ms: int):
        self._delay_ms = max(0, int(delay_ms))
        self._client = httpx.Client(timeout=90.0, follow_redirects=True)
        self._last_request_end: float = 0.0

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _sleep_throttle(self) -> None:
        if self._delay_ms <= 0:
            return
        gap = self._delay_ms / 1000.0
        now = time.monotonic()
        wait_s = gap - (now - self._last_request_end)
        if wait_s > 0:
            time.sleep(wait_s)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def get(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        self._sleep_throttle()
        try:
            r = self._client.get(url, headers=headers)
        finally:
            self._last_request_end = time.monotonic()
        return r
