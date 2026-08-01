"""Unit tests for EU footprint classification and new board normalizers."""

from __future__ import annotations

from discovery.eu_footprint import (
    MIN_POSTINGS_FOR_DEMOTION,
    FootprintVerdict,
    classify_locations,
    location_is_eu_or_remote,
    location_looks_us_only,
)
from normalize.boards import (
    normalize_arbeitnow,
    normalize_himalayas,
    normalize_jobicy,
    normalize_remotive,
)
from rank.employer_mission_gate import filter_jobs_by_employer_mission
from storage.repository import JobRepository


def test_location_eu_and_remote():
    assert location_is_eu_or_remote("Berlin, Germany")
    assert location_is_eu_or_remote("Remote — Europe")
    assert location_is_eu_or_remote("San Francisco", is_remote=True)
    assert not location_is_eu_or_remote("San Francisco, CA")
    assert location_looks_us_only("New York, NY")
    assert not location_looks_us_only("Remote Europe")


def test_classify_locations_eu_ok():
    has_eu, total, eu_n, _ = classify_locations(
        [("Berlin", False), ("New York", False), ("Remote", True)]
    )
    assert has_eu
    assert total == 3
    assert eu_n == 2


def test_classify_locations_us_only():
    has_eu, total, eu_n, _ = classify_locations(
        [("San Francisco, CA", False), ("New York, NY", False)]
    )
    assert not has_eu
    assert total == 2
    assert eu_n == 0


def test_normalize_remotive_passes_required_location():
    norm = normalize_remotive(
        {
            "id": 1,
            "title": "Data Engineer",
            "company_name": "Acme",
            "url": "https://remotive.com/x",
            "description": "<p>Build pipelines</p>",
            "candidate_required_location": "Europe",
            "category": "Data",
            "publication_date": "2026-07-01",
        }
    )
    assert norm["source"] == "remotive"
    assert norm["is_remote"] is True
    assert "Europe" in (norm["location_text"] or "")
    assert "candidate_required_location" in norm["description_text"]


def test_normalize_arbeitnow():
    norm = normalize_arbeitnow(
        {
            "slug": "data-engineer-berlin",
            "title": "Data Engineer",
            "company_name": "SumUp",
            "description": "<p>ETL</p>",
            "location": "Berlin, Germany",
            "remote": False,
            "url": "https://www.arbeitnow.com/x",
            "created_at": "2026-07-01",
        }
    )
    assert norm["source"] == "arbeitnow"
    assert norm["location_text"] == "Berlin, Germany"
    assert norm["is_remote"] is False


def test_normalize_jobicy_and_himalayas():
    j = normalize_jobicy(
        {
            "id": 9,
            "jobTitle": "Analytics Engineer",
            "companyName": "Voyage",
            "jobDescription": "<p>dbt</p>",
            "jobGeo": "France",
            "url": "https://jobicy.com/x",
        }
    )
    assert j["source"] == "jobicy"
    assert j["location_text"] == "France"

    h = normalize_himalayas(
        {
            "title": "Platform Engineer",
            "companyName": "Zoetis",
            "companySlug": "zoetis",
            "description": "<p>x</p>",
            "locationRestrictions": ["United States"],
            "timezoneRestrictions": [-5, 1],
        }
    )
    assert h["source"] == "himalayas"
    assert "United States" in (h["location_text"] or "")
    assert "timezoneRestrictions" in h["description_text"]


def test_employer_mission_gate_skips_curated_and_uses_cache(tmp_path):
    db = tmp_path / "t.db"
    repo = JobRepository(db)
    repo.init_db()
    repo.upsert_employer_mission(
        company_name="Good Org",
        mission_pass=True,
        mission_score=80,
        reason="mission",
    )
    repo.upsert_employer_mission(
        company_name="Bad Corp",
        mission_pass=False,
        mission_score=10,
        reason="for-profit",
    )

    class _Settings:
        EMPLOYER_MISSION_GATE_ENABLED = True
        MISSION_APPROVE_MIN_SCORE = 50

    jobs = [
        {"id": 1, "company_name": "Good Org", "ats_type": "job_board", "source": "remotive"},
        {"id": 2, "company_name": "Bad Corp", "ats_type": "job_board", "source": "remotive"},
        {"id": 3, "company_name": "Curated Co", "ats_type": "greenhouse", "source": "greenhouse"},
    ]
    kept = filter_jobs_by_employer_mission(jobs, repo=repo, settings=_Settings())
    ids = {j["id"] for j in kept}
    assert ids == {1, 3}


def test_empty_board_is_undecided_not_demoted():
    """An employer with no postings today is not evidence of a US-only employer."""
    v = FootprintVerdict(
        company_name="X", ats_type="greenhouse", ats_slug="x",
        ok=False, has_eu_or_remote=False, posting_count=0, eu_or_remote_count=0,
        verdict="undecided",
    )
    assert v.should_keep


def test_demote_verdict_is_dropped():
    v = FootprintVerdict(
        company_name="X", ats_type="greenhouse", ats_slug="x",
        ok=False, has_eu_or_remote=False, posting_count=12, eu_or_remote_count=0,
        verdict="demote",
    )
    assert not v.should_keep


def test_min_postings_threshold_is_conservative():
    # Demotion requires a real sample; 1-3 US postings must not condemn an employer.
    assert MIN_POSTINGS_FOR_DEMOTION >= 4
