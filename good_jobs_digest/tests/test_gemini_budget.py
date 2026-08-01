"""Free-tier guardrails: the daily budget must be a hard stop, and it must persist."""

from __future__ import annotations

import json

import pytest

from config import Settings
from normalize.schema import JobScorePayload
from rank.llm import BudgetExhausted, UsageLedger
from rank.scorer import JobScorer
from tests.stub_llm import StubLLM


def test_ledger_counts_and_blocks(tmp_path):
    ledger = UsageLedger(tmp_path / "usage.json", daily_budget=3)
    for _ in range(3):
        ledger.reserve()
    assert ledger.used == 3
    assert ledger.remaining == 0
    with pytest.raises(BudgetExhausted):
        ledger.reserve()


def test_ledger_persists_across_instances(tmp_path):
    path = tmp_path / "usage.json"
    first = UsageLedger(path, daily_budget=5)
    first.reserve()
    first.reserve()
    # A second run on the same day must not get a fresh allowance.
    second = UsageLedger(path, daily_budget=5)
    assert second.used == 2
    assert second.remaining == 3


def test_ledger_resets_on_a_new_day(tmp_path):
    path = tmp_path / "usage.json"
    path.write_text(json.dumps({"date": "2000-01-01", "count": 999}), encoding="utf-8")
    ledger = UsageLedger(path, daily_budget=10)
    assert ledger.used == 0


def test_zero_budget_means_unlimited(tmp_path):
    ledger = UsageLedger(tmp_path / "usage.json", daily_budget=0)
    for _ in range(50):
        ledger.reserve()
    assert ledger.remaining > 0


class _BudgetBlownLLM:
    def generate_json(self, prompt, **kwargs):
        raise BudgetExhausted("spent")


def _row(job_id: int) -> dict:
    return {
        "id": job_id,
        "company_name": "A",
        "title": "Data Engineer",
        "location_text": "Remote",
        "is_remote": True,
        "description_text": "pipelines",
    }


def test_scorer_stops_cleanly_when_budget_is_gone():
    scorer = JobScorer(Settings(), llm=_BudgetBlownLLM())
    results = scorer._score_chunk([_row(1)], "profile")
    # No score, no crash — the job stays unscored and is retried on the next run.
    assert results == [(1, None)]
    assert scorer.budget_exhausted is True


def test_scorer_makes_no_further_calls_after_exhaustion():
    stub = StubLLM([{"irrelevant": True}])
    scorer = JobScorer(Settings(), llm=stub)
    scorer.budget_exhausted = True
    assert scorer.score_job(_row(1), "profile") is None
    assert stub.prompts == []  # never called the API again


def test_scorer_parses_a_normal_response():
    payload = {
        "role_relevance": 80,
        "mission_alignment": 70,
        "candidate_fit": 75,
        "remote_ok": True,
        "eu_hire_ok": True,
        "timezone_ok": True,
        "seniority_ok": True,
        "fit_reasons": [],
        "extracted_salary": None,
        "top_requirements": [],
        "risks_or_gaps": [],
        "one_line_summary": "ok",
    }
    scorer = JobScorer(Settings(), llm=StubLLM([payload]))
    out = scorer.score_job(_row(1), "profile")
    assert isinstance(out, JobScorePayload)
    assert out.eu_hire_ok is True
