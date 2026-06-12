"""Environment helpers shared across config and BigQuery."""

from __future__ import annotations

from pathlib import Path


def strip_env_path(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'":
        s = s[1:-1]
    return s.strip() or None


def normalize_google_credentials_env(root: Path) -> None:
    """Resolve relative GOOGLE_APPLICATION_CREDENTIALS against project root."""
    import os

    raw = strip_env_path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    if not raw:
        return
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (root / p).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(p)
