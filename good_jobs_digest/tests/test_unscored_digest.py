"""Jobs the LLM could not score still get emailed.

Missing a good opening is worse than emailing one without a score, so a failed
scoring attempt is recorded rather than silently dropping the job.
"""

from __future__ import annotations

from datetime import date

from digest.builder import build_markdown_digest
from storage.repository import JobRepository, content_hash

NOW = "2026-08-13T06:30:00+00:00"


def _job(repo: JobRepository, source_job_id: str, *, description: str = "pipelines") -> int:
    jid, _ = repo.upsert_job(
        company_name="Co",
        mission_category="climate",
        ats_type="job_board",
        ats_slug="indeed",
        source="indeed",
        source_job_id=source_job_id,
        title="Analytics Engineer",
        url=f"http://u/{source_job_id}",
        location_text="Remote, EU",
        is_remote=True,
        salary_text=None,
        description_text=description,
        chash=content_hash(description),
        now_iso=NOW,
    )
    repo.set_prefilter(jid, True)
    return jid


def test_failed_score_reaches_the_digest(tmp_path):
    repo = JobRepository(tmp_path / "u.db")
    repo.init_db()
    jid = _job(repo, "1")

    # Before any attempt it is just unscored — it will be scored later this run.
    assert repo.jobs_unscored_for_digest() == []

    repo.record_score_failure(jid)
    rows = repo.jobs_unscored_for_digest()
    assert [int(r["id"]) for r in rows] == [jid]

    # Once emailed it does not come back.
    repo.mark_digest_included([jid])
    assert repo.jobs_unscored_for_digest() == []


def test_successful_score_clears_the_failure(tmp_path):
    repo = JobRepository(tmp_path / "cleared.db")
    repo.init_db()
    jid = _job(repo, "2")
    repo.record_score_failure(jid)
    repo.save_score(
        jid,
        relevance=80,
        mission=80,
        fit=80,
        remote_ok=True,
        combined=80.0,
        llm_payload={"one_line_summary": "ok"},
        eu_hire_ok=True,
        timezone_ok=True,
        seniority_ok=True,
        role_ok=True,
    )
    assert repo.jobs_unscored_for_digest() == []
    assert len(repo.jobs_for_digest(min_combined=0, remote_only=False)) == 1


def test_attempts_cap_stops_burning_requests(tmp_path):
    repo = JobRepository(tmp_path / "attempts.db")
    repo.init_db()
    jid = _job(repo, "3")

    repo.record_score_failure(jid)
    assert len(repo.jobs_needing_score(max_attempts=2)) == 1  # one more go
    repo.record_score_failure(jid)
    assert repo.jobs_needing_score(max_attempts=2) == []  # out of goes
    assert len(repo.jobs_needing_score()) == 1  # cap is opt-in

    # A rewritten description is new information: it earns a fresh set of attempts.
    _job(repo, "3", description="totally new description")
    assert len(repo.jobs_needing_score(max_attempts=2)) == 1
    assert repo.jobs_unscored_for_digest() == []


def test_unscored_limit_is_respected(tmp_path):
    repo = JobRepository(tmp_path / "limit.db")
    repo.init_db()
    for i in range(5):
        repo.record_score_failure(_job(repo, f"n{i}"))
    assert len(repo.jobs_unscored_for_digest(limit=2)) == 2
    assert len(repo.jobs_unscored_for_digest()) == 5


def test_digest_renders_unscored_section():
    unscored = [
        {
            "company_name": "Co",
            "title": "Analytics Engineer",
            "url": "http://u/1",
            "combined_score": None,
            "is_remote": True,
            "description_text": "We are a nonprofit building climate data pipelines.",
            "llm_json": None,
        }
    ]
    md = build_markdown_digest([], [], digest_date=date(2026, 8, 13), unscored_rows=unscored)
    assert "Unscored" in md
    assert "[**Analytics Engineer**](http://u/1)" in md
    assert "Score" not in md  # no score to show, and none invented
    # The summary line must not claim there is nothing to look at.
    assert "No new scored openings to send" in md
    assert "could not be scored" in md


def test_scored_and_unscored_are_both_summarised():
    scored = [
        {
            "id": 1,
            "company_name": "Alpha",
            "title": "Data Engineer",
            "url": "http://a/1",
            "combined_score": 85.5,
            "is_remote": True,
            "llm_json": "{}",
        }
    ]
    unscored = [
        {
            "id": 2,
            "company_name": "Beta",
            "title": "Analytics Engineer",
            "url": "http://b/2",
            "combined_score": None,
            "is_remote": True,
            "llm_json": None,
        }
    ]
    md = build_markdown_digest([], scored, digest_date=date(2026, 8, 13), unscored_rows=unscored)
    assert "**1** openings not sent in a previous digest" in md
    assert "Plus **1** that could not be scored" in md
    assert "Score **85.5**" in md


def test_digest_without_unscored_is_unchanged():
    md = build_markdown_digest([], [], digest_date=date(2026, 8, 13))
    assert "Unscored" not in md
