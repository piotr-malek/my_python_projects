"""Mission employer scoring (mocked Ollama)."""

from __future__ import annotations

from unittest.mock import patch

from config import Settings
from discovery.mission_filter import EmployerMissionFilter


def test_score_employers_returns_all_with_scores():
    settings = Settings()
    filt = EmployerMissionFilter(settings)
    employers = [
        {
            "company_name": "GiveWell",
            "job_board_url": "https://boards.greenhouse.io/givewell",
            "mission_category": "effective_altruism",
            "discovery_source": "80000hours",
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
                "company_name": "Lyft",
                "mission_score": 15,
                "purpose_driven": False,
                "reason": "Commercial ride-hailing",
                "mission_type": "commercial",
            },
        ]
    }
    with patch.object(filt, "_call_ollama", return_value=fake):
        scored = filt.score_employers(employers)
    assert len(scored) == 2
    by_name = {r["company_name"]: r for r in scored}
    assert by_name["GiveWell"]["mission_score"] == "95"
    assert by_name["GiveWell"]["purpose_driven"] == "true"
    assert by_name["Lyft"]["mission_score"] == "15"
    assert by_name["Lyft"]["purpose_driven"] == "false"
