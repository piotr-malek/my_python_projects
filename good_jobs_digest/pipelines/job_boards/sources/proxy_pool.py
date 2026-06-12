"""Rotate Webshare HTTP proxies (host:port:user:pass per line)."""

from __future__ import annotations

import logging
import random
import threading
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


class ProxyPool:
    """Round-robin pool loaded from a local file."""

    def __init__(self, path: Path | None):
        self._path = path
        self._proxies: list[ProxyEndpoint] = []
        self._index = 0
        self._lock = threading.Lock()
        if path and path.is_file():
            self._proxies = load_proxy_file(path)
            logger.info("Loaded %s Webshare proxies from %s", len(self._proxies), path)
        elif path:
            logger.warning("Proxy file missing: %s", path)

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
