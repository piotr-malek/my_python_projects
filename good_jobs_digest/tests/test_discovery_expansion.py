"""Tests for ATS registry, dedup, and employer mining."""

from __future__ import annotations

from discovery.ats_registry import careers_url, parse_ats_from_text, CURATED_ATS_TYPES
from discovery.employer_candidates import mine_employer_candidate, load_unprobed_candidates
from normalize.dedup import canonical_key, merge_duplicate, source_priority


def test_parse_ashby_url():
    parsed = parse_ats_from_text("https://jobs.ashbyhq.com/rethink-priorities")
    assert parsed == ("ashby", "rethink-priorities")


def test_parse_workable_url():
    parsed = parse_ats_from_text("https://apply.workable.com/acme-corp/j/ABC123")
    assert parsed is not None
    assert parsed[0] == "workable"


def test_careers_url_ashby():
    assert careers_url("ashby", "acme") == "https://jobs.ashbyhq.com/acme"


def test_curated_ats_types_includes_tier1():
    for ats in ("ashby", "workable", "recruitee", "personio", "workday"):
        assert ats in CURATED_ATS_TYPES


def test_canonical_key_stable():
    job = {
        "company_name": "Acme",
        "title": "Engineer",
        "location_text": "London, UK",
        "posted_at_hint": "2026-06-01T00:00:00+00:00",
    }
    assert canonical_key(job) == canonical_key(job)


def test_source_priority_prefers_org_ats():
    assert source_priority("greenhouse", "greenhouse") < source_priority("reliefweb", "job_board")


def test_merge_duplicate_prefers_ats():
    existing = {
        "source": "reliefweb",
        "ats_type": "job_board",
        "ats_slug": "reliefweb",
        "source_job_id": "1",
        "company_name": "IRC",
        "title": "Program Officer",
        "url": "https://reliefweb.int/job/1",
        "location_text": "Kenya",
        "description_text": "desc",
        "posted_at_hint": "2026-06-01",
    }
    incoming = {
        "source": "greenhouse",
        "ats_type": "greenhouse",
        "ats_slug": "irc",
        "source_job_id": "99",
        "company_name": "IRC",
        "title": "Program Officer",
        "url": "https://boards.greenhouse.io/irc/jobs/99",
        "location_text": "Kenya",
        "description_text": "desc",
        "posted_at_hint": "2026-06-01",
    }
    winner = merge_duplicate(existing, incoming)
    assert winner["source"] == "greenhouse"


def test_mine_employer_candidate(tmp_path):
    pool = tmp_path / "pool.jsonl"
    norm = {
        "company_name": "Watershed",
        "url": "https://boards.greenhouse.io/watershed",
        "mission_category": "climate",
        "source": "climatebase",
        "description_text": "engineer",
        "title": "Engineer",
    }
    mine_employer_candidate(norm, discovery_source="climatebase", pool_path=pool)
    rows = load_unprobed_candidates(pool)
    assert len(rows) == 1
    assert rows[0].company_name == "Watershed"
    assert rows[0].ats_hint == ("greenhouse", "watershed")
