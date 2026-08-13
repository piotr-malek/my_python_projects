"""Digest formatting and deduplication."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from digest.builder import build_markdown_digest
from digest.formatting import (
    dedupe_by_company_title,
    effective_posted_datetime,
    is_new_since,
    job_bullet_line,
    job_block_lines,
    strip_location_suffix,
)
from mail.markdown_html import markdown_to_html


def _yesterday_utc_cutoff() -> datetime:
    y = date.today() - timedelta(days=1)
    return datetime.combine(y, datetime.min.time(), tzinfo=timezone.utc)


def test_dedupe_keeps_highest_score():
    jobs = [
        {"company_name": "Wiki", "title": "Manager", "combined_score": 90.0, "url": "http://a"},
        {"company_name": "Wiki", "title": "Manager", "combined_score": 92.0, "url": "http://b"},
    ]
    out = dedupe_by_company_title(jobs)
    assert len(out) == 1
    assert out[0]["url"] == "http://b"


def test_job_bullet_compact_with_score():
    job = {
        "title": "Data Engineer",
        "company_name": "WaterAid",
        "url": "https://example.com/job",
        "combined_score": 88.5,
        "mission_category": "global health / humanitarian",
        "description_text": (
            "WaterAid is an international NGO working on clean water and sanitation. "
            "You will build BigQuery pipelines on GCP for fundraising analytics."
        ),
        "llm_json": '{"one_line_summary": "Remote analytics engineering role at a humanitarian NGO."}',
    }
    line = job_bullet_line(job)
    assert line.startswith("- ")
    assert "[**Data Engineer**](https://example.com/job)" in line
    assert "**WaterAid**" in line
    assert "Score **88.5**" in line
    assert "focuses on" not in line
    assert "international NGO" in line or "humanitarian NGO" in line


def test_company_blurb_skips_mission_category_only():
    line = job_bullet_line(
        {
            "title": "Engineer",
            "company_name": "Acme",
            "url": "https://example.com/x",
            "combined_score": 80.0,
            "mission_category": "climate",
            "description_text": "You will build data pipelines and work with stakeholders.",
            "llm_json": '{"one_line_summary": "Data engineering role."}',
        }
    )
    assert "focuses on climate" not in line
    assert "is hiring for this role" not in line


def test_company_blurb_uses_about_section():
    line = job_bullet_line(
        {
            "title": "Engineer",
            "company_name": "Armada",
            "url": "https://example.com/x",
            "combined_score": 80.0,
            "mission_category": "climate",
            "description_text": (
                "About the Company\n\n"
                "Armada delivers modular AI infrastructure for edge deployments worldwide. "
                "About the Role\n\nYou will support customers."
            ),
            "llm_json": '{"one_line_summary": "Customer engineering role."}',
        }
    )
    assert "modular AI infrastructure" in line
    assert "focuses on" not in line


def test_job_block_single_bullet():
    job = {
        "title": "Analytics Engineer",
        "company_name": "Acme",
        "url": "https://example.com/x",
        "combined_score": 72.0,
        "llm_json": '{"one_line_summary": "Strong analytics fit."}',
    }
    lines = job_block_lines(job)
    assert len(lines) == 1
    assert lines[0].startswith("- ")


def test_markdown_link_renders_in_html():
    line = job_bullet_line(
        {
            "title": "Data Engineer",
            "company_name": "Acme",
            "url": "https://example.com/job",
            "combined_score": 80.0,
            "llm_json": '{"one_line_summary": "Good fit."}',
        }
    )
    html = markdown_to_html(line)
    assert '<a href="https://example.com/job"><strong>Data Engineer</strong></a>' in html


def test_is_new_since_prefers_posted_at_over_first_seen():
    cutoff = _yesterday_utc_cutoff()
    old_first_seen = (cutoff - timedelta(days=30)).isoformat()
    recent_posted = cutoff.isoformat()
    job = {
        "posted_at": recent_posted,
        "first_seen_at": old_first_seen,
    }
    assert is_new_since(job, cutoff)
    dt = effective_posted_datetime(job)
    assert dt is not None and dt >= cutoff


def test_build_digest_counts_after_dedupe():
    rows = [
        {
            "company_name": "Co",
            "title": "A",
            "combined_score": 80,
            "first_seen_at": "2026-05-01T00:00:00+00:00",
            "url": "http://x",
        },
        {
            "company_name": "Co",
            "title": "A",
            "combined_score": 85,
            "first_seen_at": "2026-05-01T00:00:00+00:00",
            "url": "http://y",
        },
    ]
    md = build_markdown_digest(rows, [])
    assert "**1** openings" in md
    assert "Score **85.0**" in md


def test_dedupe_merges_per_location_reposts():
    """Four Speechify postings of one role — one per city, each with its own
    canonical_job_id — are one opening, not four digest lines."""
    cities = ["The Hague, Netherlands", "Rotterdam, Netherlands", "Utrecht, Netherlands", "Cork, Ireland"]
    jobs = [
        {
            "company_name": "Speechify",
            "title": f"Software Engineer, Data Infrastructure & Acquisition - {city}",
            "canonical_job_id": f"canon{i}",
            "combined_score": 70.0 + i,
            "url": f"http://s/{i}",
        }
        for i, city in enumerate(cities)
    ]
    out = dedupe_by_company_title(jobs)
    assert len(out) == 1
    assert out[0]["combined_score"] == 73.0  # highest survives
    assert out[0]["duplicate_count"] == 4
    assert "4 postings" in job_bullet_line(out[0])


def test_dedupe_merges_same_title_across_locations():
    """Identical title, different location → different canonical_job_id. Keying on
    canonical alone was letting these through as separate openings."""
    jobs = [
        {"company_name": "PVcase", "title": "Platform Data Engineer",
         "canonical_job_id": "a1", "combined_score": 80.0, "url": "http://p/1"},
        {"company_name": "PVcase", "title": "Platform Data Engineer",
         "canonical_job_id": "b2", "combined_score": 81.0, "url": "http://p/2"},
        {"company_name": "PVcase", "title": "Platform Data Engineer",
         "canonical_job_id": "c3", "combined_score": 79.0, "url": "http://p/3"},
    ]
    out = dedupe_by_company_title(jobs)
    assert len(out) == 1
    assert out[0]["url"] == "http://p/2"
    assert out[0]["duplicate_count"] == 3


def test_dedupe_keeps_distinct_roles_that_share_a_title_stem():
    """A dash tail is usually a specialism, not a city — these must stay separate."""
    jobs = [
        {"company_name": "Faire", "title": "Senior Analytics Engineer - GTM", "combined_score": 80.0},
        {"company_name": "Faire", "title": "Senior Analytics Engineer - Marketplace", "combined_score": 81.0},
        {"company_name": "Faire", "title": "Data Engineer - Analytics Platform", "combined_score": 82.0},
    ]
    assert len(dedupe_by_company_title(jobs)) == 3


def test_location_strip_ignores_role_qualifiers_with_commas():
    # Comma present, but the words name a role — must not be stripped.
    assert strip_location_suffix(
        "Impact Manager - Reporting, Data and Analytics (Acumen East)"
    ) == "Impact Manager - Reporting, Data and Analytics (Acumen East)"
    # No comma: a single-word tail is never treated as a location.
    assert strip_location_suffix("Platform Engineer, FDE - NYC") == "Platform Engineer, FDE - NYC"
    # City, country: stripped.
    assert strip_location_suffix("Data Engineer - Munich, Germany") == "Data Engineer"


def test_dedupe_still_merges_cross_source_by_canonical():
    """Company spellings differ between boards; canonical_job_id is what ties
    those together, so that path has to keep working."""
    jobs = [
        {"company_name": "Speechify", "title": "Data Engineer",
         "canonical_job_id": "same", "combined_score": 70.0, "url": "http://a"},
        {"company_name": "Speechify Inc.", "title": "Data Engineer (Remote)",
         "canonical_job_id": "same", "combined_score": 75.0, "url": "http://b"},
    ]
    out = dedupe_by_company_title(jobs)
    assert len(out) == 1
    assert out[0]["url"] == "http://b"


def test_single_posting_has_no_count_suffix():
    job = {"company_name": "Co", "title": "Data Engineer", "combined_score": 80.0, "url": "http://u"}
    out = dedupe_by_company_title([job])
    assert "duplicate_count" not in out[0]
    assert "postings" not in job_bullet_line(out[0])


def test_merged_line_drops_the_city_from_the_title():
    jobs = [
        {"company_name": "Speechify", "title": "Software Engineer, Platform - Eindhoven, Netherlands",
         "canonical_job_id": "a", "combined_score": 75.0, "url": "http://s/1"},
        {"company_name": "Speechify", "title": "Software Engineer, Platform - Cork, Ireland",
         "canonical_job_id": "b", "combined_score": 70.0, "url": "http://s/2"},
    ]
    line = job_bullet_line(dedupe_by_company_title(jobs)[0])
    assert "Software Engineer, Platform**](http://s/1)" in line
    assert "Eindhoven" not in line
    assert "2 postings" in line
