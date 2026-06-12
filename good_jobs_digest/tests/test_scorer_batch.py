"""Parallel / batch scoring helpers (no live Ollama)."""

from __future__ import annotations

from unittest.mock import patch

from config import Settings
from normalize.schema import JobScorePayload
from rank.scorer import JobScorer, _loads_json_response


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
        "extracted_salary": None,
        "top_requirements": ["python"],
        "risks_or_gaps": [],
        "one_line_summary": "good fit",
    }
    base.update(overrides)
    return base


def test_score_chunk_batch_parses_array():
    settings = Settings()
    settings.OLLAMA_SCORE_BATCH_SIZE = 2
    scorer = JobScorer(settings)
    rows = [_row(1), _row(2)]
    fake = {"scores": [_payload(), _payload(role_relevance=85, one_line_summary="strong ml")]}
    with patch.object(scorer, "_call_ollama", return_value=fake):
        results = scorer._score_chunk(rows, "profile text")
    assert len(results) == 2
    assert results[0][0] == 1 and results[0][1] is not None
    assert results[1][0] == 2 and results[1][1] is not None
    assert results[0][1].role_relevance == 80  # type: ignore[union-attr]


def test_loads_json_response_repairs_trailing_commas():
    raw = '{"scores": [{"role_relevance": 1},],}'
    parsed = _loads_json_response(raw)
    assert len(parsed["scores"]) == 1


def test_score_chunk_splits_on_batch_failure():
    settings = Settings()
    scorer = JobScorer(settings)
    rows = [_row(1), _row(2)]
    good = JobScorePayload.model_validate(_payload())

    with patch.object(scorer, "_call_ollama", return_value=None):
        with patch.object(scorer, "score_job", return_value=good):
            results = scorer._score_chunk(rows, "profile")

    assert len(results) == 2
    assert results[0][1] is not None
    assert results[1][1] is not None
