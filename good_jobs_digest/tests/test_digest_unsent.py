"""Digest includes only jobs not yet emailed."""

from __future__ import annotations

from digest.builder import build_markdown_digest
from digest.selection import exclude_already_sent, job_identity_key
from storage.repository import JobRepository, content_hash


def test_job_identity_key_stable():
    assert job_identity_key(
        {"source": "greenhouse", "ats_slug": "Acme", "source_job_id": "1"}
    ) == ("greenhouse", "acme", "1")


def test_exclude_already_sent_filters_bq_keys():
    rows = [
        {"source": "greenhouse", "ats_slug": "a", "source_job_id": "1", "title": "T"},
        {"source": "greenhouse", "ats_slug": "b", "source_job_id": "2", "title": "T2"},
    ]
    sent = {("greenhouse", "a", "1")}
    out = exclude_already_sent(rows, sent)
    assert len(out) == 1
    assert out[0]["source_job_id"] == "2"


def test_jobs_for_digest_unsent_only(tmp_path):
    repo = JobRepository(tmp_path / "d.db")
    repo.init_db()
    now = "2026-05-28T12:00:00+00:00"
    jid, _ = repo.upsert_job(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="99",
        title="Data Engineer",
        url="http://u",
        location_text=None,
        is_remote=True,
        salary_text=None,
        description_text="x",
        chash=content_hash("x"),
        now_iso=now,
    )
    repo.set_prefilter(jid, True)
    repo.save_score(
        jid,
        relevance=80,
        mission=80,
        fit=80,
        remote_ok=True,
        combined=80.0,
        llm_payload={"one_line_summary": "ok"},
    )
    all_rows = repo.jobs_for_digest(min_combined=50, remote_only=False, unsent_only=False)
    assert len(all_rows) == 1
    unsent = repo.jobs_for_digest(min_combined=50, remote_only=False, unsent_only=True)
    assert len(unsent) == 1
    repo.mark_digest_included([jid])
    unsent_after = repo.jobs_for_digest(min_combined=50, remote_only=False, unsent_only=True)
    assert len(unsent_after) == 0


def test_jobs_for_digest_min_combined_zero_includes_low_scores(tmp_path):
    repo = JobRepository(tmp_path / "low.db")
    repo.init_db()
    now = "2026-05-28T12:00:00+00:00"
    jid, _ = repo.upsert_job(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="1",
        title="Data Engineer",
        url="http://u",
        location_text=None,
        is_remote=True,
        salary_text=None,
        description_text="x",
        chash=content_hash("x"),
        now_iso=now,
    )
    repo.set_prefilter(jid, True)
    repo.save_score(
        jid,
        relevance=10,
        mission=10,
        fit=10,
        remote_ok=False,
        combined=10.0,
        llm_payload={"one_line_summary": "weak"},
    )
    assert len(repo.jobs_for_digest(min_combined=0, remote_only=False)) == 1
    assert len(repo.jobs_for_digest(min_combined=50, remote_only=False)) == 0


def test_build_digest_unsent_wording():
    rows = [
        {
            "company_name": "Co",
            "title": "Data Engineer",
            "combined_score": 80,
            "url": "http://u",
            "is_remote": True,
            "llm_json": "{}",
        }
    ]
    md = build_markdown_digest(rows, [])
    assert "not sent in a previous digest" in md
    assert "New since yesterday" not in md
