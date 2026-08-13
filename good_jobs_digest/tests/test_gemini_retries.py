"""One retry per call, and no more.

Retries used to nest: two temperatures per call in the scorer, up to five HTTP
attempts each in the client, and a failed batch splitting in half and recursing.
A single unlucky batch of 8 jobs could spend 30 of the 300 daily requests, which
is what happened on 2026-08-13. Each test below pins one layer of that shut.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from config import Settings
from rank.llm import GeminiClient

_PAYLOAD = {"scores": [{"role_relevance": 80}]}


class _FakeModels:
    """Replays a scripted sequence of responses/exceptions per call."""

    def __init__(self, script: list[object]):
        self._script = list(script)
        self.calls: list[dict[str, object]] = []

    def generate_content(self, *, model, contents, config):  # noqa: ANN001
        self.calls.append({"model": model, "temperature": config.temperature})
        item = self._script.pop(0) if self._script else RuntimeError("503 UNAVAILABLE")
        if isinstance(item, Exception):
            raise item
        return SimpleNamespace(text=item)


def _client(tmp_path, script: list[object], monkeypatch) -> tuple[GeminiClient, _FakeModels]:
    monkeypatch.setattr("rank.llm.time.sleep", lambda _s: None)  # no transient backoff wait
    settings = Settings()
    settings.GEMINI_API_KEY = "test-key"
    settings.GEMINI_RPM = 0  # no throttle in tests
    settings.GEMINI_USAGE_PATH = tmp_path / "usage.json"
    settings.GEMINI_DAILY_REQUEST_BUDGET = 100
    settings.GEMINI_MAX_RETRIES = 1
    settings.GEMINI_MODEL = "model-a"
    settings.GEMINI_MODEL_FALLBACKS = ("model-b", "model-c")
    client = GeminiClient(settings)
    fake = _FakeModels(script)
    client._client = SimpleNamespace(models=fake)
    return client, fake


def test_transient_failures_cost_two_requests(tmp_path, monkeypatch):
    client, fake = _client(tmp_path, [RuntimeError("503 UNAVAILABLE")], monkeypatch)
    assert client.generate_json("prompt") is None
    assert len(fake.calls) == 2  # first attempt + the one retry, then give up
    assert client.usage.used == 2


def test_retry_switches_to_temperature_zero(tmp_path, monkeypatch):
    client, fake = _client(tmp_path, ["not json at all", json.dumps(_PAYLOAD)], monkeypatch)
    assert client.generate_json("prompt", temperature=0.15) == _PAYLOAD
    assert [c["temperature"] for c in fake.calls] == [0.15, 0.0]


def test_empty_response_gets_one_retry_only(tmp_path, monkeypatch):
    client, fake = _client(tmp_path, ["", ""], monkeypatch)
    assert client.generate_json("prompt") is None
    assert len(fake.calls) == 2


def test_non_transient_error_is_not_retried(tmp_path, monkeypatch):
    """A 400 will fail identically on a second try — don't pay for it."""
    client, fake = _client(tmp_path, [RuntimeError("400 INVALID_ARGUMENT")], monkeypatch)
    assert client.generate_json("prompt") is None
    assert len(fake.calls) == 1
    assert client.usage.used == 1


def test_model_fallback_does_not_consume_the_retry(tmp_path, monkeypatch):
    """A retired model id is a config problem, not a flaky call: swapping models
    must still leave the call its one real retry."""
    client, fake = _client(
        tmp_path,
        [
            RuntimeError("404 NOT_FOUND: model-a"),
            RuntimeError("503 UNAVAILABLE"),
            json.dumps(_PAYLOAD),
        ],
        monkeypatch,
    )
    assert client.generate_json("prompt") == _PAYLOAD
    assert [c["model"] for c in fake.calls] == ["model-a", "model-b", "model-b"]


def test_zero_retries_means_a_single_attempt(tmp_path, monkeypatch):
    client, fake = _client(tmp_path, [RuntimeError("503 UNAVAILABLE")], monkeypatch)
    client._max_retries = 0
    assert client.generate_json("prompt") is None
    assert len(fake.calls) == 1
