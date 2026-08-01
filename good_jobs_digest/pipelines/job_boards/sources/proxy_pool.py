"""Rotate Webshare HTTP proxies (host:port:user:pass per line)."""

from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProxyEndpoint:
    host: str
    port: str
    username: str
    password: str

    @property
    def url(self) -> str:
        return f"http://{self.username}:{self.password}@{self.host}:{self.port}"

    def label(self) -> str:
        return f"{self.host}:{self.port}"


WEBSHARE_API_URL = "https://proxy.webshare.io/api/v2/proxy/list/"


def _fetch_via_api(api_key: str, *, page_size: int = 100) -> list[str]:
    """Proxy lines from the Webshare API. Preferred over the download URL: it
    reflects the current allocation and marks dead endpoints as invalid."""
    import httpx

    lines: list[str] = []
    resp = httpx.get(
        WEBSHARE_API_URL,
        params={"mode": "direct", "page": 1, "page_size": page_size},
        headers={"Authorization": f"Token {api_key}"},
        timeout=60,
    )
    resp.raise_for_status()
    for item in resp.json().get("results") or []:
        if item.get("valid") is False:
            continue
        host, port = item.get("proxy_address"), item.get("port")
        user, password = item.get("username"), item.get("password")
        if host and port and user and password:
            lines.append(f"{host}:{port}:{user}:{password}")
    return lines


def _fetch_via_url(url: str) -> list[str]:
    import httpx

    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return [
        ln.strip() for ln in resp.text.splitlines() if ln.strip() and not ln.startswith("#")
    ]


def refresh_proxy_file(
    path: Path,
    url: str = "",
    *,
    api_key: str = "",
    max_age_hours: float = 24.0,
    force: bool = False,
) -> bool:
    """Re-download the proxy list when the cached file is missing or stale.

    Webshare rotates free proxies without notice, so a file that was correct
    yesterday is often dead today. Uses the API key when available (it filters out
    endpoints Webshare has already marked invalid) and falls back to the plain
    download URL. Returns True if the file was rewritten.
    """
    if not api_key and not url:
        return False
    if not force and path.is_file() and path.stat().st_size > 0:
        age_hours = (time.time() - path.stat().st_mtime) / 3600.0
        if age_hours < max_age_hours:
            return False
    lines: list[str] = []
    try:
        if api_key:
            lines = _fetch_via_api(api_key)
        if not lines and url:
            lines = _fetch_via_url(url)
    except Exception as exc:  # noqa: BLE001
        # Never fail ingest over proxies; boards just degrade to direct requests.
        logger.warning("Could not refresh Webshare proxies (%s) — using cached file", exc)
        return False

    if not lines:
        logger.warning("Webshare returned an empty proxy list — keeping the old file")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Refreshed %s Webshare proxies into %s", len(lines), path)
    return True


class ProxyPool:
    """Round-robin pool loaded from a local file, refreshed from Webshare on demand."""

    def __init__(
        self,
        path: Path | None,
        *,
        refresh_url: str = "",
        api_key: str = "",
        max_age_hours: float = 24.0,
    ):
        self._path = path
        self._proxies: list[ProxyEndpoint] = []
        self._index = 0
        self._lock = threading.Lock()
        if path and (refresh_url or api_key):
            refresh_proxy_file(
                path, refresh_url, api_key=api_key, max_age_hours=max_age_hours
            )
        if path and path.is_file():
            self._proxies = load_proxy_file(path)
            logger.info("Loaded %s Webshare proxies from %s", len(self._proxies), path)
        elif path:
            logger.warning(
                "Proxy file missing: %s (set WEBSHARE_PROXY_LIST_URL to fetch it automatically)",
                path,
            )

    @property
    def count(self) -> int:
        return len(self._proxies)

    def __bool__(self) -> bool:
        return bool(self._proxies)

    def cycle(self) -> Iterator[ProxyEndpoint]:
        """Yield each proxy once (shuffled), then stop."""
        items = list(self._proxies)
        random.shuffle(items)
        yield from items

    def next(self) -> ProxyEndpoint | None:
        if not self._proxies:
            return None
        with self._lock:
            ep = self._proxies[self._index % len(self._proxies)]
            self._index += 1
        return ep


def load_proxy_file(path: Path) -> list[ProxyEndpoint]:
    out: list[ProxyEndpoint] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":")
        if len(parts) < 4:
            continue
        host, port, user = parts[0], parts[1], parts[2]
        password = ":".join(parts[3:])
        out.append(ProxyEndpoint(host=host, port=port, username=user, password=password))
    return out
