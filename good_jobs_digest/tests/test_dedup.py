from storage.repository import JobRepository, content_hash


def test_dedup_same_job_updates_last_seen(tmp_path):
    db = tmp_path / "t.db"
    repo = JobRepository(db)
    repo.init_db()
    desc = "hello world"
    h = content_hash(desc)
    now = "2026-05-01T10:00:00+00:00"
    jid1, c1 = repo.upsert_job(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="1",
        title="T",
        url="http://u",
        location_text=None,
        is_remote=True,
        salary_text=None,
        description_text=desc,
        chash=h,
        now_iso=now,
    )
    assert c1 is True
    jid2, c2 = repo.upsert_job(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="1",
        title="T",
        url="http://u",
        location_text=None,
        is_remote=True,
        salary_text=None,
        description_text=desc,
        chash=h,
        now_iso="2026-05-02T10:00:00+00:00",
    )
    assert jid1 == jid2
    assert c2 is False


def test_content_change_clears_scores(tmp_path):
    db = tmp_path / "t2.db"
    repo = JobRepository(db)
    repo.init_db()
    base = dict(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="2",
        title="T",
        url="http://u",
        location_text=None,
        is_remote=False,
        salary_text=None,
    )
    repo.upsert_job(
        **base,
        description_text="v1",
        chash=content_hash("v1"),
        now_iso="2026-05-01T10:00:00+00:00",
    )
    rows = repo.jobs_needing_score()
    assert len(rows) == 0
    repo.set_prefilter(1, True)
    repo.save_score(
        1,
        relevance=80,
        mission=80,
        fit=80,
        remote_ok=True,
        combined=80.0,
        llm_payload={"x": 1},
    )
    repo.upsert_job(
        **base,
        description_text="v2-changed",
        chash=content_hash("v2-changed"),
        now_iso="2026-05-03T10:00:00+00:00",
    )
    import sqlite3

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT relevance_score, last_scored_at FROM jobs WHERE id=1").fetchone()
    conn.close()
    assert row["relevance_score"] is None
    assert row["last_scored_at"] is None


def test_scored_jobs_not_requeued_until_content_changes(tmp_path):
    db = tmp_path / "t3.db"
    repo = JobRepository(db)
    repo.init_db()
    base = dict(
        company_name="Co",
        mission_category=None,
        ats_type="greenhouse",
        ats_slug="co",
        source="greenhouse",
        source_job_id="3",
        title="Data Engineer",
        url="http://u",
        location_text=None,
        is_remote=False,
        salary_text=None,
    )
    repo.upsert_job(**base, description_text="v1", chash=content_hash("v1"), now_iso="2026-05-01T10:00:00+00:00")
    repo.set_prefilter(1, True)
    repo.save_score(1, relevance=80, mission=80, fit=80, remote_ok=True, combined=80.0, llm_payload={})
    assert len(repo.jobs_needing_score()) == 0
    repo.upsert_job(
        **base,
        description_text="v1",
        chash=content_hash("v1"),
        now_iso="2026-05-02T10:00:00+00:00",
    )
    assert len(repo.jobs_needing_score()) == 0
    repo.upsert_job(
        **base,
        description_text="v2",
        chash=content_hash("v2"),
        now_iso="2026-05-03T10:00:00+00:00",
    )
    assert len(repo.jobs_needing_score()) == 1
