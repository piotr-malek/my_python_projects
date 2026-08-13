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


def _payload(**overrides) -> JobScorePayload:
    base = dict(
        role_relevance=85,
        mission_alignment=80,
        candidate_fit=80,
        remote_ok=True,
        eu_hire_ok=True,
        timezone_ok=True,
        seniority_ok=True,
        role_ok=True,
        one_line_summary="Looks good",
    )
    base.update(overrides)
    return JobScorePayload(**base)


def test_guard_corrects_eu_hire_ok_not_only_remote_ok():
    """The model set eu_hire_ok=true on a US-only posting; the guard owns the fix,
    because eu_hire_ok is what the digest gates on when remote-only is off."""
    c = extract_location_constraints(
        location_text="United States (Remote)",
        description_text="Must be authorized to work in the United States.",
        acceptable_hire_regions=EU_ACCEPTABLE,
    )
    out = apply_location_guard(_payload(), c, acceptable_hire_regions=EU_ACCEPTABLE)
    assert out.remote_ok is False
    assert out.eu_hire_ok is False
    assert any("posting states" in g for g in out.risks_or_gaps)


def test_guard_reports_which_gates_it_corrected():
    from rank.location_constraints import LocationPolicy, guard_job_payload

    job = {
        "title": "AI Success Engineer",
        "location_text": "Singapore",
        "description_text": "Based in Singapore, supporting APAC customers.",
    }
    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    out, corrected = guard_job_payload(_payload(), job, policy=policy)
    assert set(corrected) == {"remote_ok", "eu_hire_ok"}
    assert out.eu_hire_ok is False


def test_guard_leaves_eu_jobs_untouched():
    from rank.location_constraints import LocationPolicy, guard_job_payload

    job = {
        "title": "Analytics Engineer",
        "location_text": "Remote (EU)",
        "description_text": "Fully remote within the EU, CET overlap required.",
    }
    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    out, corrected = guard_job_payload(_payload(), job, policy=policy)
    assert corrected == []
    assert out.remote_ok is True and out.eu_hire_ok is True
    assert out.risks_or_gaps == []


def test_guard_does_not_resurrect_a_false_gate():
    """It only ever tightens: a gate the model already set false stays false and
    is not reported as a correction."""
    from rank.location_constraints import LocationPolicy, guard_job_payload

    job = {"title": "Data Engineer", "location_text": "Sydney, Australia", "description_text": "Sydney office."}
    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    out, corrected = guard_job_payload(_payload(remote_ok=False), job, policy=policy)
    assert out.remote_ok is False
    assert corrected == ["eu_hire_ok"]


def test_guard_allows_unrecognised_locations():
    """Relaxed on purpose: an unlisted country or an unfamiliar city is allowed
    through rather than dropped. Extra non-EU matches are cheaper than a missed
    EU opening."""
    from rank.location_constraints import LocationPolicy, guard_job_payload

    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    for location in ("CH, remote, Sankt Gallen", "Reykjavik", "Belgrade, Serbia", ""):
        out, corrected = guard_job_payload(
            _payload(), {"title": "Data Engineer", "location_text": location, "description_text": "Remote role."},
            policy=policy,
        )
        assert corrected == [], location
        assert out.remote_ok is True and out.eu_hire_ok is True


def test_guard_allows_a_posting_that_names_any_acceptable_region():
    """Multi-location postings hire in every location listed, so one EU office is
    enough — 'Munich, BY, DE; Delaware, US' must not be read as US-only."""
    from rank.location_constraints import LocationPolicy, guard_job_payload

    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    job = {
        "title": "Senior Data Engineer",
        "location_text": "Munich, BY, DE; Aachen, NRW, DE; Delaware, US",
        "description_text": "Join us and build the grid.",
    }
    out, corrected = guard_job_payload(_payload(), job, policy=policy)
    assert corrected == []
    assert out.eu_hire_ok is True


def test_guard_fires_on_listed_non_eu_destinations():
    from rank.location_constraints import LocationPolicy, guard_job_payload

    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    for location in ("Singapore", "Sydney, Australia", "Remote - USA", "India - Pune", "Minsk, Belarus"):
        _, corrected = guard_job_payload(
            _payload(), {"title": "Data Engineer", "location_text": location, "description_text": "Remote."},
            policy=policy,
        )
        assert "eu_hire_ok" in corrected, location


def test_pronoun_us_is_not_a_country():
    """'join us' / 'about us' used to tag every posting as United States."""
    from rank.location_constraints import LocationPolicy, guard_job_payload

    policy = LocationPolicy(acceptable_hire_regions=["EU"])
    job = {
        "title": "Analytics Engineer",
        "location_text": "London",
        "description_text": "Come join us! About us: we build things. Work with us.",
    }
    _, corrected = guard_job_payload(_payload(), job, policy=policy)
    assert corrected == []
