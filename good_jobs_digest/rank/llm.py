"""Gemini Flash JSON client with free-tier guardrails.

Everything the pipeline sends to an LLM goes through here: job scoring
(`rank/scorer.py`) and the employer mission screen (`discovery/mission_filter.py`).

Staying free is enforced twice over:

1. Use an AI Studio API key on a project with **billing disabled**. Google cannot
   charge such a key — over-quota requests simply return 429.
2. Belt and braces in code: a requests-per-minute throttle plus a persisted
   daily request budget (`data/gemini_usage.json`). When the budget is spent the
   client stops issuing calls and the affected jobs stay unscored, to be picked
   up on the next run rather than silently dropped.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class BudgetExhausted(RuntimeError):
    """Raised when the configured daily request budget is used up."""


def _extract_json_object(text: str) -> str:
    if not text:
        return "{}"
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text
    return text[start : end + 1]


class UsageLedger:
    """Persisted per-day request counter, shared across runs on one machine."""

    def __init__(self, path: Path, daily_budget: int):
        self._path = path
        self._budget = daily_budget
        self._lock = threading.Lock()
        self._day, self._count = self._load()

    def _load(self) -> tuple[str, int]:
        today = date.today().isoformat()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return today, 0
        if str(data.get("date")) != today:
            return today, 0
        return today, int(data.get("count") or 0)

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps({"date": self._day, "count": self._count}), encoding="utf-8"
            )
        except OSError as exc:  # pragma: no cover - disk issues shouldn't kill a run
            logger.debug("could not persist gemini usage: %s", exc)

    @property
    def used(self) -> int:
        return self._count

    @property
    def remaining(self) -> int:
        if self._budget <= 0:
            return 1_000_000
        return max(0, self._budget - self._count)

    def reserve(self) -> None:
        """Account for one request; raises BudgetExhausted when none are left."""
        with self._lock:
            today = date.today().isoformat()
            if today != self._day:  # rolled past midnight mid-run
                self._day, self._count = today, 0
            if self._budget > 0 and self._count >= self._budget:
                raise BudgetExhausted(
                    f"daily Gemini request budget of {self._budget} is spent"
                )
            self._count += 1
            self._save()


class _RateLimiter:
    """Simple requests-per-minute throttle."""

    def __init__(self, rpm: int):
        self._min_interval = 60.0 / rpm if rpm > 0 else 0.0
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self._min_interval
        if sleep_for > 0:
            time.sleep(sleep_for)


class GeminiClient:
    """Thin wrapper returning parsed JSON objects, or None on failure."""

    def __init__(self, settings: Any):
        self._model = getattr(settings, "GEMINI_MODEL", "gemini-3.5-flash-lite")
        # Google retires pinned model ids without warning (2.5-flash-lite started
        # 404ing for new keys). For an unattended daily run, failing over beats
        # silently producing no digest at all.
        self._fallbacks = [
            m
            for m in getattr(settings, "GEMINI_MODEL_FALLBACKS", ())
            if m and m != self._model
        ]
        self._max_output_tokens = int(getattr(settings, "GEMINI_MAX_OUTPUT_TOKENS", 4096))
        self._max_retries = int(getattr(settings, "GEMINI_MAX_RETRIES", 3))
        api_key = getattr(settings, "GEMINI_API_KEY", "") or os.getenv("GEMINI_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "GEMINI_API_KEY is not set — create one at https://aistudio.google.com/apikey "
                "on a project WITHOUT billing enabled so it can never incur charges."
            )
        try:
            from google import genai
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("google-genai is required: pip install google-genai") from exc

        self._genai = genai
        self._client = genai.Client(api_key=api_key)
        self._limiter = _RateLimiter(int(getattr(settings, "GEMINI_RPM", 15)))
        self._ledger = UsageLedger(
            Path(getattr(settings, "GEMINI_USAGE_PATH", "data/gemini_usage.json")),
            int(getattr(settings, "GEMINI_DAILY_REQUEST_BUDGET", 800)),
        )

    @property
    def usage(self) -> UsageLedger:
        return self._ledger

    def generate_json(
        self,
        prompt: str,
        *,
        schema: dict[str, Any] | None = None,
        temperature: float = 0.1,
        max_output_tokens: int | None = None,
    ) -> dict[str, Any] | None:
        """Return a parsed JSON object, or None if the model never produced one.

        Raises BudgetExhausted when the daily budget is gone, so callers can stop
        early instead of hammering a quota that will only return 429s.
        """
        from google.genai import types

        config: dict[str, Any] = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens or self._max_output_tokens,
            "response_mime_type": "application/json",
        }
        if schema:
            config["response_schema"] = schema

        delay = 2.0
        # Model swaps shouldn't eat the retry budget meant for transient errors.
        for attempt in range(1, self._max_retries + len(self._fallbacks) + 1):
            self._ledger.reserve()  # propagates BudgetExhausted
            self._limiter.wait()
            try:
                response = self._client.models.generate_content(
                    model=self._model,
                    contents=prompt,
                    config=types.GenerateContentConfig(**config),
                )
                text = (getattr(response, "text", "") or "").strip()
                if not text:
                    logger.info("gemini returned empty text (attempt %s)", attempt)
                    continue
                parsed = json.loads(_extract_json_object(text))
                if isinstance(parsed, dict):
                    return parsed
                logger.info("gemini returned non-object JSON (attempt %s)", attempt)
            except json.JSONDecodeError as exc:
                logger.info("gemini JSON parse failed (attempt %s): %s", attempt, exc)
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                if ("404" in message or "NOT_FOUND" in message) and self._fallbacks:
                    retired, self._model = self._model, self._fallbacks.pop(0)
                    logger.warning(
                        "Gemini model %r unavailable — falling back to %r. "
                        "Update GEMINI_MODEL to silence this.",
                        retired,
                        self._model,
                    )
                    continue
                transient = any(
                    token in message
                    for token in ("429", "RESOURCE_EXHAUSTED", "503", "UNAVAILABLE", "500")
                )
                logger.warning("gemini error (attempt %s): %s", attempt, message[:200])
                if not transient:
                    break
                time.sleep(delay)
                delay *= 2
        return None
