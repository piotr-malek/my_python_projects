"""Parallel / batch scoring helpers (no live Ollama)."""

from __future__ import annotations

from unittest.mock import patch

from config import Settings
from normalize.schema import JobScorePayload
from rank.scorer import JobScorer, _loads_json_response
from tests.stub_llm import StubLLM


def _row(job_id: int) -> dict:
    return {
        "id": job_id,
        "company_name": "A",
        "mission_category": "climate",
        "title": "Data Engineer",
        "location_text": "Remote",
        "is_remote": True,
        "salary_text": None,
        "description_text": "build pipelines",
    }


def _payload(**overrides) -> dict:
    base = {
        "role_relevance": 80,
        "mission_alignment": 70,
        "candidate_fit": 75,
        "remote_ok": True,
        "eu_hire_ok": True,
        "timezone_ok": True,
        "seniority_ok": True,
        "role_ok": True,
        "fit_reasons": [],
        "extracted_salary": None,
        "top_requirements": ["python"],
        "risks_or_gaps": [],
        "one_line_summary": "good fit",
    }
    base.update(overrides)
    return base


def test_score_chunk_batch_parses_array():
    settings = Settings()
    settings.LLM_SCORE_BATCH_SIZE = 2
    scorer = JobScorer(settings, llm=StubLLM())
    rows = [_row(1), _row(2)]
    fake = {"scores": [_payload(), _payload(role_relevance=85, one_line_summary="strong ml")]}
    with patch.object(scorer, "_call_llm", return_value=fake):
        results = scorer._score_chunk(rows, "profile text")
    assert len(results) == 2
    assert results[0][0] == 1 and results[0][1] is not None
    assert results[1][0] == 2 and results[1][1] is not None
    assert results[0][1].role_relevance == 80  # type: ignore[union-attr]


def test_loads_json_response_repairs_trailing_commas():
    raw = '{"scores": [{"role_relevance": 1},],}'
    parsed = _loads_json_response(raw)
    assert len(parsed["scores"]) == 1


def test_score_chunk_does_not_split_on_batch_failure():
    """A failed batch costs exactly one call: no halving, no per-job re-ask.

    The jobs come back unscored and reach the digest's unscored section instead.
    """
    settings = Settings()
    scorer = JobScorer(settings, llm=StubLLM())
    rows = [_row(1), _row(2)]
    good = JobScorePayload.model_validate(_payload())

    with patch.object(scorer, "_call_llm", return_value=None) as call:
        with patch.object(scorer, "score_job", return_value=good) as single:
            results = scorer._score_chunk(rows, "profile")

    assert results == [(1, None), (2, None)]
    assert call.call_count == 1
    assert single.call_count == 0


def test_score_chunk_leaves_unusable_row_unscored():
    """A row the batch mangled is not re-asked on its own — that used to cost
    one extra call per malformed row."""
    settings = Settings()
    scorer = JobScorer(settings, llm=StubLLM())
    rows = [_row(1), _row(2)]
    fake = {"scores": [_payload(), {"role_relevance": "not a number"}]}

    with patch.object(scorer, "_call_llm", return_value=fake):
        with patch.object(scorer, "score_job") as single:
            results = scorer._score_chunk(rows, "profile")

    assert results[0][1] is not None
    assert results[1] == (2, None)
    assert single.call_count == 0


def test_call_llm_makes_one_request():
    """The scorer no longer retries at a second temperature; the client owns the
    single retry, so a call here is exactly one generate_json."""
    stub = StubLLM([None])
    scorer = JobScorer(Settings(), llm=stub)
    assert scorer.score_job(_row(1), "profile") is None
    assert len(stub.prompts) == 1
