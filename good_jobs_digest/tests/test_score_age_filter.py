from datetime import datetime, timedelta, timezone

from storage.repository import JobRepository, content_hash


def _days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat()


def _insert_job(repo: JobRepository, *, posted_at: str | None, first_seen: str) -> int:
    jid, _ = repo.upsert_job(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id=posted_at or first_seen,
        title="T",
        url="http://u",
        location_text=None,
        is_remote=False,
        salary_text=None,
        description_text="desc",
        chash=content_hash("desc"),
        now_iso=first_seen,
        posted_at=posted_at,
    )
    repo.set_prefilter(jid, True)
    return jid


def test_jobs_needing_score_respects_max_age_days(tmp_path):
    repo = JobRepository(tmp_path / "age.db")
    repo.init_db()
    recent_posted = _days_ago(10)
    first_seen = _days_ago(2)
    _insert_job(repo, posted_at=recent_posted, first_seen=first_seen)
    _insert_job(repo, posted_at=_days_ago(200), first_seen=first_seen)

    all_rows = repo.jobs_needing_score()
    assert len(all_rows) == 2

    recent = repo.jobs_needing_score(max_age_days=30)
    assert len(recent) == 1
    assert recent[0]["posted_at"] == recent_posted
