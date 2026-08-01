"""posted_at hints arrive as ISO, Unix epoch or RFC-822 depending on the board."""

from __future__ import annotations

from storage.repository import JobRepository, content_hash

_parse = JobRepository._parse_posted_at_hint


def test_iso_hint_preserved():
    assert _parse("2026-07-01T05:10:04+00:00") == "2026-07-01T05:10:04+00:00"


def test_naive_iso_gets_utc():
    assert _parse("2026-07-01T05:10:04").endswith("+00:00")


def test_epoch_seconds():
    assert _parse("1785566725") == "2026-08-01T06:45:25+00:00"


def test_epoch_milliseconds():
    assert _parse("1785566725000") == "2026-08-01T06:45:25+00:00"


def test_rfc822_from_rss():
    assert _parse("Tue, 01 Jul 2026 10:00:00 +0000") == "2026-07-01T10:00:00+00:00"


def test_unparseable_becomes_none():
    """Junk must not be stored: SQLite datetime() returns NULL for it, which would
    silently drop the row from the age-filtered scoring query."""
    assert _parse("not-a-date") is None
    assert _parse("") is None
    assert _parse(None) is None


def test_epoch_job_survives_age_filter(tmp_path):
    repo = JobRepository(tmp_path / "epoch.db")
    repo.init_db()
    jid, _ = repo.upsert_job(
        company_name="SumUp",
        mission_category=None,
        ats_type="job_board",
        ats_slug="arbeitnow",
        source="arbeitnow",
        source_job_id="de-1",
        title="Data Engineer",
        url="http://u",
        location_text="Berlin",
        is_remote=True,
        salary_text=None,
        description_text="etl",
        chash=content_hash("etl"),
        posted_at="1785566725",  # Arbeitnow sends epoch ints
    )
    repo.set_prefilter(jid, True)
    stored = repo.get_job(jid)["posted_at"]
    assert stored and stored.startswith("2026-")
    # The whole point: an epoch date must not hide the job from scoring.
    assert len(repo.jobs_needing_score(max_age_days=36500)) == 1
