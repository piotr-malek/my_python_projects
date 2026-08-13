"""Mistral JSON client with free-tier guardrails.

Everything the pipeline sends to an LLM goes through here: job scoring
(`rank/scorer.py`) and the employer mission screen (`discovery/mission_filter.py`).

Mistral La Plateforme's free "Experiment" tier needs no card and no prepaid
credit, which is the whole reason this pipeline runs on it: the workload is
roughly 2M tokens a month, far under the allowance, and no provider that bills
by prepayment is worth a minimum top-up that would last years at this rate.

Three guardrails:

1. A requests-per-minute throttle (`LLM_RPM`).
2. A persisted daily request budget (`LLM_DAILY_REQUEST_BUDGET`, counted in
   `data/llm_usage.json`). When it is spent the client stops issuing calls.
3. At most one retry per call (`LLM_MAX_RETRIES`). Retries used to nest — two
   temperatures per call, several HTTP attempts each, and a batch that split in
   half on failure — so one bad batch of 8 jobs could burn 30 requests.

Jobs that fail anyway are emailed unscored rather than dropped, so none of these
limits can lose an opening.
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

PROVIDER_NAME = "mistral"


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
            logger.debug("could not persist llm usage: %s", exc)

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
                raise BudgetExhausted(f"daily request budget of {self._budget} is spent")
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


class MistralClient:
    """Chat completions with schema-constrained JSON, one retry, and a budget."""

    provider = PROVIDER_NAME

    def __init__(self, settings: Any, http: Any | None = None):
        self._model = getattr(settings, "MISTRAL_MODEL", "mistral-medium-latest")
        self._max_output_tokens = int(getattr(settings, "LLM_MAX_OUTPUT_TOKENS", 4096))
        self._max_retries = max(0, int(getattr(settings, "LLM_MAX_RETRIES", 1)))
        self._limiter = _RateLimiter(int(getattr(settings, "LLM_RPM", 8)))
        self._ledger = UsageLedger(
            Path(getattr(settings, "LLM_USAGE_PATH", "data/llm_usage.json")),
            int(getattr(settings, "LLM_DAILY_REQUEST_BUDGET", 300)),
        )
        api_key = getattr(settings, "MISTRAL_API_KEY", "") or os.getenv("MISTRAL_API_KEY", "")
        if not api_key:
            raise RuntimeError(
                "MISTRAL_API_KEY is not set — create one at https://console.mistral.ai/. "
                "The free Experiment tier needs no card and covers this pipeline many "
                "times over."
            )
        self._base_url = str(getattr(settings, "MISTRAL_BASE_URL", "https://api.mistral.ai/v1"))
        if http is not None:
            self._http = http
        else:
            import httpx

            self._http = httpx.Client(
                base_url=self._base_url,
                timeout=float(getattr(settings, "LLM_TIMEOUT_SECONDS", 120)),
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

    @property
    def usage(self) -> UsageLedger:
        return self._ledger

    @property
    def model(self) -> str:
        return self._model

    def _request(
        self,
        *,
        prompt: str,
        schema: dict[str, Any] | None,
        temperature: float,
        max_output_tokens: int,
    ) -> str:
        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_output_tokens,
        }
        if schema:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "structured_output",
                    "schema": schema,
                    "strict": True,
                },
            }
        else:
            payload["response_format"] = {"type": "json_object"}

        response = self._http.post("/chat/completions", json=payload)
        # Surface the status so _is_transient sees it; 429 and 5xx earn the retry.
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")
        body = response.json()
        choices = body.get("choices") or []
        if not choices:
            return ""
        return str(choices[0].get("message", {}).get("content") or "")

    @staticmethod
    def _is_transient(message: str) -> bool:
        return any(
            token in message
            for token in ("429", "500", "502", "503", "504", "rate limit", "timeout")
        )

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
        # One retry and no more: every attempt spends a request from the daily
        # budget, and callers treat a failure as "leave this unscored" rather
        # than something to escalate.
        retries_left = self._max_retries
        attempt = 0
        temp = temperature
        tokens = max_output_tokens or self._max_output_tokens

        def _retry(reason: str, *, sleep_for: float = 0.0) -> bool:
            """Consume one retry; the second try runs at temperature 0."""
            nonlocal retries_left, temp
            if retries_left <= 0:
                logger.warning("mistral gave up after %s attempt(s): %s", attempt, reason)
                return False
            # Logged loudly on purpose: a retry that succeeds is invisible
            # otherwise, and a run where *every* call retries silently doubles
            # both the request count and the wall clock.
            logger.warning("mistral retrying at temperature 0 — %s", reason)
            retries_left -= 1
            temp = 0.0
            if sleep_for:
                time.sleep(sleep_for)
            return True

        while True:
            attempt += 1
            self._ledger.reserve()  # propagates BudgetExhausted
            self._limiter.wait()
            try:
                text = (
                    self._request(
                        prompt=prompt,
                        schema=schema,
                        temperature=temp,
                        max_output_tokens=tokens,
                    )
                    or ""
                ).strip()
                if not text:
                    if _retry(f"empty text (attempt {attempt})"):
                        continue
                    return None
                parsed = json.loads(_extract_json_object(text))
                if isinstance(parsed, dict):
                    return parsed
                if _retry(f"non-object JSON (attempt {attempt})"):
                    continue
                return None
            except json.JSONDecodeError as exc:
                if _retry(f"JSON parse failed (attempt {attempt}): {exc}"):
                    continue
                return None
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                logger.warning("mistral error (attempt %s): %s", attempt, message[:200])
                if not self._is_transient(message):
                    return None
                if _retry(f"transient error: {message[:120]}", sleep_for=2.0):
                    continue
                return None


def make_llm_client(settings: Any) -> MistralClient:
    """Single place the pipeline builds its LLM client."""
    client = MistralClient(settings)
    logger.info("LLM: %s (%s)", client.provider, client.model)
    return client
