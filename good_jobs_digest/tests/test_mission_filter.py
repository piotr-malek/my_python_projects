"""Mission employer filter (mocked Ollama)."""

from __future__ import annotations

from unittest.mock import patch

from config import Settings
from discovery.mission_filter import EmployerMissionFilter
from tests.stub_llm import StubLLM


def test_filter_employers_keeps_above_threshold():
    settings = Settings()
    settings.MISSION_APPROVE_MIN_SCORE = 50
    filt = EmployerMissionFilter(settings, llm=StubLLM())
    employers = [
        {
            "company_name": "GiveWell",
            "job_board_url": "https://boards.greenhouse.io/givewell",
            "mission_category": "effective_altruism",
            "discovery_source": "80000hours",
        },
        {
            "company_name": "Borderline B Corp",
            "job_board_url": "https://boards.greenhouse.io/borderline",
            "mission_category": "bcorp",
            "discovery_source": "bcorp",
        },
        {
            "company_name": "Lyft",
            "job_board_url": "https://boards.greenhouse.io/lyft",
            "mission_category": "mobility",
            "discovery_source": "seeds",
        },
    ]
    fake = {
        "results": [
            {
                "company_name": "GiveWell",
                "mission_score": 95,
                "purpose_driven": True,
                "reason": "Effective altruism grantmaker",
                "mission_type": "nonprofit",
            },
            {
                "company_name": "Borderline B Corp",
                "mission_score": 55,
                "purpose_driven": False,
                "reason": "Some social angle",
                "mission_type": "bcorp",
            },
            {
                "company_name": "Lyft",
                "mission_score": 15,
                "purpose_driven": False,
                "reason": "Commercial ride-hailing",
                "mission_type": "commercial",
            },
        ]
    }
    with patch.object(filt, "_call_llm", return_value=fake):
        kept = filt.filter_employers(employers)
    assert len(kept) == 2
    names = {r["company_name"] for r in kept}
    assert names == {"GiveWell", "Borderline B Corp"}


def _employers(n: int) -> list[dict[str, str]]:
    return [
        {
            "company_name": f"Co {i}",
            "job_board_url": f"https://boards.greenhouse.io/co{i}",
            "mission_category": "climate",
            "discovery_source": "seeds",
        }
        for i in range(n)
    ]


def test_failed_batch_is_not_retried_per_employer():
    """A failed batch of 20 used to fan out into 20 single calls. Nothing is
    cached on failure, so these employers are simply rescored on the next run."""
    stub = StubLLM([None])
    filt = EmployerMissionFilter(Settings(), llm=stub)
    assert filt._score_chunk(_employers(20)) == []
    assert len(stub.prompts) == 1


def test_count_mismatch_is_not_retried_per_employer():
    stub = StubLLM(
        [
            {
                "results": [
                    {
                        "company_name": "Co 0",
                        "mission_score": 90,
                        "purpose_driven": True,
                        "reason": "climate",
                        "mission_type": "nonprofit",
                    }
                ]
            }
        ]
    )
    filt = EmployerMissionFilter(Settings(), llm=stub)
    assert filt._score_chunk(_employers(3)) == []  # 1 result for 3 employers
    assert len(stub.prompts) == 1
