"""Tests for location constraint extraction."""

from normalize.schema import JobScorePayload
from rank.location_constraints import (
    apply_location_guard,
    evaluate_hire_region_fit,
    expand_acceptable_hire_regions,
    extract_location_constraints,
    format_location_constraints_for_prompt,
    location_constraints_from_job,
)

EU_ACCEPTABLE = ["EU"]


def test_us_remote_location_line_flags_incompatible():
    c = extract_location_constraints(
        title="AI Factory Customer Engineer",
        location_text="United States (Remote)",
        description_text=(
            "For U.S. Based candidates: starting salary listed. "
            "Are you currently authorized to work in the United States?"
        ),
        acceptable_hire_regions=EU_ACCEPTABLE,
    )
    assert "United States" in c.stated_regions
    assert c.remote_within_region is True
    assert c.likely_region_mismatch is True
    text = format_location_constraints_for_prompt(c, acceptable_hire_regions=EU_ACCEPTABLE)
    assert "country/region-restricted remote" in text
    assert "remote_ok should be FALSE" in text
    assert "acceptable hire region(s): EU" in text


def test_eu_remote_compatible():
    c = extract_location_constraints(
        title="Data Engineer",
        location_text="Remote (EU)",
        description_text="Fully remote within European time zones.",
        acceptable_hire_regions=EU_ACCEPTABLE,
    )
    assert c.likely_region_mismatch is False
    assert evaluate_hire_region_fit(c, EU_ACCEPTABLE) is True


def test_expand_eu_includes_member_regions():
    expanded = expand_acceptable_hire_regions(["EU"])
    assert "Germany" in expanded
    assert "United States" not in expanded


def test_apply_location_guard_forces_remote_ok_false():
    c = extract_location_constraints(
        location_text="United States (Remote)",
        description_text="Authorized to work in the United States",
        acceptable_hire_regions=EU_ACCEPTABLE,
    )
    payload = JobScorePayload(
        role_relevance=90,
        mission_alignment=80,
        candidate_fit=85,
        remote_ok=True,
        one_line_summary="Looks good",
    )
    out = apply_location_guard(payload, c, acceptable_hire_regions=EU_ACCEPTABLE)
    assert out.remote_ok is False
    assert any("acceptable regions" in g for g in out.risks_or_gaps)


def test_unspecified_remote_location_acceptable():
    c = extract_location_constraints(
        title="Analytics Engineer",
        location_text="Remote",
        description_text="Build data pipelines with dbt and BigQuery.",
        acceptable_hire_regions=EU_ACCEPTABLE,
        allow_unspecified_location=True,
    )
    assert c.likely_region_mismatch is False
    assert evaluate_hire_region_fit(c, EU_ACCEPTABLE, allow_unspecified_location=True) is True
    text = format_location_constraints_for_prompt(c, acceptable_hire_regions=EU_ACCEPTABLE)
    assert "unspecified" in text.lower()


def test_global_remote_preferred_and_compatible():
    c = extract_location_constraints(
        title="Data Engineer",
        location_text="Remote",
        description_text="Work from anywhere. Fully distributed team.",
        acceptable_hire_regions=EU_ACCEPTABLE,
        prefer_global_remote=True,
    )
    assert c.appears_global_remote is True
    assert c.likely_region_mismatch is False
    text = format_location_constraints_for_prompt(c, acceptable_hire_regions=EU_ACCEPTABLE)
    assert "work-from-anywhere" in text.lower() or "global" in text.lower()


def test_location_constraints_from_job_row():
    row = {
        "title": "Analytics Engineer",
        "location_text": "United States (Remote)",
        "description_text": "Authorized to work in the United States required.",
    }
    c = location_constraints_from_job(row, acceptable_hire_regions=EU_ACCEPTABLE)
    assert c.likely_region_mismatch is True
