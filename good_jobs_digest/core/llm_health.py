"""Today's LLM reachability, shared between pipeline stages.

`check-llm`, `score` and `digest` run as separate processes in one CI job, so a
provider outage seen by the first has to reach the last somehow. This is that
channel: one small JSON file next to the usage ledger, scoped to the current day
so yesterday's outage never colours today's digest.

It exists because a provider failure used to mean an empty inbox. The preflight
aborted the run, nothing was emailed, and five days of silence looked exactly
like five quiet days. Now the run continues, the jobs go out unscored, and the
digest footer says why.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _path(settings: Any) -> Path:
    usage = getattr(settings, "LLM_USAGE_PATH", None)
    if usage:
        return Path(usage).with_name("llm_health.json")
    return Path("data/llm_health.json")


def record(settings: Any, *, ok: bool, detail: str = "") -> None:
    """Note whether the LLM answered. Never raises — this is only ever a hint."""
    path = _path(settings)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"date": date.today().isoformat(), "ok": bool(ok), "detail": detail[:300]}
            ),
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001
        # Deliberately broad: this is a hint for the footer, and no failure to
        # write a hint may ever take down a digest run. A bad path raises
        # ValueError rather than OSError, which an OSError-only guard missed.
        logger.debug("could not persist llm health: %s", exc)


def read(settings: Any) -> dict[str, Any] | None:
    """Today's record, or None when there isn't one (or it is stale)."""
    try:
        data = json.loads(_path(settings).read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - see record(): a hint must never raise
        return None
    if str(data.get("date")) != date.today().isoformat():
        return None
    return {"ok": bool(data.get("ok")), "detail": str(data.get("detail") or "")}
