"""Mistral client: schema translation, request shape, and error handling.

A schema Mistral rejects means a 400 on every scoring call and a digest full of
unscored jobs, so the strict-mode requirements are pinned here: closed objects,
every property required, type unions for optionals, and none of the validation
keywords strict mode refuses.
"""

from __future__ import annotations

import json

import pytest

from config import Settings
from discovery.mission_filter import MISSION_SCORE_BATCH_JSON_SCHEMA
from rank.llm import MistralClient, make_llm_client
from rank.scorer import BATCH_SCORE_JSON_SCHEMA, SCORE_JSON_SCHEMA


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = payload if isinstance(payload, str) else json.dumps(payload)

    def json(self) -> object:
        return self._payload if not isinstance(self._payload, str) else json.loads(self._payload)


class _FakeHttp:
    """Stands in for httpx.Client; replays scripted responses per POST."""

    def __init__(self, script: list[object]):
        self._script = list(script)
        self.posts: list[dict] = []

    def post(self, url: str, *, json: dict) -> _FakeResponse:  # noqa: A002
        self.posts.append({"url": url, "payload": json})
        item = self._script.pop(0) if self._script else _FakeResponse("upstream boom", 503)
        if isinstance(item, Exception):
            raise item
        return item


def _content(text: str) -> _FakeResponse:
    return _FakeResponse({"choices": [{"message": {"content": text}}]})


def _client(tmp_path, script: list[object], monkeypatch) -> tuple[MistralClient, _FakeHttp]:
    monkeypatch.setattr("rank.llm.time.sleep", lambda _s: None)
    settings = Settings()
    settings.MISTRAL_API_KEY = "test-key"
    settings.MISTRAL_MODEL = "ministral-14b-latest"
    settings.LLM_RPM = 0
    settings.LLM_USAGE_PATH = tmp_path / "usage.json"
    settings.LLM_DAILY_REQUEST_BUDGET = 100
    settings.LLM_MAX_RETRIES = 1
    http = _FakeHttp(script)
    return MistralClient(settings, http=http), http


# --- schema shape ---


def test_schemas_are_authored_in_strict_form():
    """Strict structured output needs closed objects, every property required, and
    a type union for the optional field. Range keywords must be absent — strict
    mode rejects them, and JobScorePayload checks the ranges anyway."""
    for schema in (SCORE_JSON_SCHEMA, BATCH_SCORE_JSON_SCHEMA, MISSION_SCORE_BATCH_JSON_SCHEMA):
        blob = json.dumps(schema)
        assert "minimum" not in blob and "maximum" not in blob and "minItems" not in blob
        assert "nullable" not in blob

    assert SCORE_JSON_SCHEMA["additionalProperties"] is False
    assert set(SCORE_JSON_SCHEMA["required"]) == set(SCORE_JSON_SCHEMA["properties"])
    assert SCORE_JSON_SCHEMA["properties"]["extracted_salary"]["type"] == ["string", "null"]

    item = MISSION_SCORE_BATCH_JSON_SCHEMA["properties"]["results"]["items"]
    assert item["additionalProperties"] is False
    assert set(item["required"]) == set(item["properties"])


# --- request shape ---------------------------------------------------------


def test_request_sends_strict_json_schema(tmp_path, monkeypatch):
    client, http = _client(tmp_path, [_content('{"ok": true}')], monkeypatch)
    assert client.generate_json("prompt", schema=SCORE_JSON_SCHEMA) == {"ok": True}

    payload = http.posts[0]["payload"]
    assert http.posts[0]["url"] == "/chat/completions"
    assert payload["model"] == "ministral-14b-latest"
    assert payload["messages"] == [{"role": "user", "content": "prompt"}]
    fmt = payload["response_format"]
    assert fmt["type"] == "json_schema"
    assert fmt["json_schema"]["strict"] is True
    assert fmt["json_schema"]["schema"]["properties"]["extracted_salary"]["type"] == [
        "string",
        "null",
    ]


def test_request_without_schema_falls_back_to_json_object(tmp_path, monkeypatch):
    client, http = _client(tmp_path, [_content('{"ok": 1}')], monkeypatch)
    client.generate_json("prompt")
    assert http.posts[0]["payload"]["response_format"] == {"type": "json_object"}


def test_max_tokens_is_passed_through(tmp_path, monkeypatch):
    client, http = _client(tmp_path, [_content('{"ok": 1}')], monkeypatch)
    client.generate_json("prompt", max_output_tokens=6112)
    assert http.posts[0]["payload"]["max_tokens"] == 6112


# --- failure handling ------------------------------------------------------


def test_rate_limit_is_retried_once_at_temperature_zero(tmp_path, monkeypatch):
    client, http = _client(
        tmp_path,
        [_FakeResponse("rate limited", 429), _content('{"ok": true}')],
        monkeypatch,
    )
    assert client.generate_json("prompt", temperature=0.15) == {"ok": True}
    assert [p["payload"]["temperature"] for p in http.posts] == [0.15, 0.0]
    assert client.usage.used == 2


def test_persistent_rate_limit_gives_up_after_one_retry(tmp_path, monkeypatch):
    client, http = _client(
        tmp_path,
        [_FakeResponse("rate limited", 429), _FakeResponse("rate limited", 429)],
        monkeypatch,
    )
    assert client.generate_json("prompt") is None
    assert len(http.posts) == 2  # not a storm
    assert client.usage.used == 2


def test_bad_request_is_not_retried(tmp_path, monkeypatch):
    """A 400 means the schema or model id is wrong — a second identical call
    cannot fix it, and would only spend budget."""
    client, http = _client(tmp_path, [_FakeResponse("invalid schema", 400)], monkeypatch)
    assert client.generate_json("prompt", schema=SCORE_JSON_SCHEMA) is None
    assert len(http.posts) == 1


def test_empty_choices_are_treated_as_no_answer(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, [_FakeResponse({"choices": []})] * 2, monkeypatch)
    assert client.generate_json("prompt") is None


def test_prose_wrapped_json_is_still_parsed(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, [_content('Here you go:\n{"ok": true}\nhope that helps')], monkeypatch)
    assert client.generate_json("prompt") == {"ok": True}


def test_missing_api_key_is_a_clear_error(monkeypatch):
    # The client falls back to the ambient env, so a real key in .env would
    # otherwise make this pass vacuously.
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    settings = Settings()
    settings.MISTRAL_API_KEY = ""
    with pytest.raises(RuntimeError, match="MISTRAL_API_KEY"):
        MistralClient(settings, http=_FakeHttp([]))


# --- provider selection ----------------------------------------------------


def test_factory_builds_a_mistral_client(tmp_path, monkeypatch):
    settings = Settings()
    settings.MISTRAL_API_KEY = "test-key"
    settings.LLM_USAGE_PATH = tmp_path / "usage.json"
    monkeypatch.setattr("httpx.Client", lambda **_kw: _FakeHttp([]))
    client = make_llm_client(settings)
    assert client.provider == "mistral"
    assert client.model == "ministral-14b-latest"
