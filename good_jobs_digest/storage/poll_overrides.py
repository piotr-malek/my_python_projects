"""Persist per-company poll_enabled overrides when ATS returns 404."""

from __future__ import annotations

import json
from pathlib import Path


def overrides_path(root: Path) -> Path:
    return root / "data" / "poll_overrides.json"


def load_overrides(path: Path) -> dict[str, bool]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return {str(k): bool(v) for k, v in data.items()}


def set_poll_disabled(path: Path, ats_type: str, ats_slug: str) -> None:
    data = load_overrides(path)
    key = f"{ats_type.lower()}:{ats_slug}"
    data[key] = False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
