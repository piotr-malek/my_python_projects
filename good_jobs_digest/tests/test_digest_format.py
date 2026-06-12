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
