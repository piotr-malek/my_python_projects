"""Thread-safe HTTP client with per-host rate limiting for parallel ATS ingest."""

from __future__ import annotations

import logging
import threading
import time
from urllib.parse import urlparse

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class HostRateLimitedHttp:
    """Shared client safe for ThreadPoolExecutor; throttle per hostname."""

    def __init__(self, delay_ms: int = 150):
        self._delay_ms = max(0, int(delay_ms))
        self._client = httpx.Client(timeout=90.0, follow_redirects=True)
        self._locks: dict[str, threading.Lock] = {}
        self._last_end: dict[str, float] = {}
        self._global_lock = threading.Lock()

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _host_lock(self, host: str) -> threading.Lock:
        with self._global_lock:
            if host not in self._locks:
                self._locks[host] = threading.Lock()
            return self._locks[host]

    def _throttle(self, host: str) -> None:
        if self._delay_ms <= 0:
            return
        gap = self._delay_ms / 1000.0
        lock = self._host_lock(host)
        with lock:
            now = time.monotonic()
            wait_s = gap - (now - self._last_end.get(host, 0.0))
            if wait_s > 0:
                time.sleep(wait_s)
            self._last_end[host] = time.monotonic()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
    def get(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        host = urlparse(url).netloc or "default"
        self._throttle(host)
        try:
            return self._client.get(url, headers=headers)
        finally:
            with self._host_lock(host):
                self._last_end[host] = time.monotonic()
