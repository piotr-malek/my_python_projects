"""Per-source run stats and the digest health footer."""

from __future__ import annotations

from datetime import date

from digest.builder import build_markdown_digest
from storage.repository import JobRepository


def _repo(tmp_path) -> JobRepository:
    repo = JobRepository(tmp_path / "health.db")
    repo.init_db()
    return repo


def test_record_and_fetch_latest_stats(tmp_path):
    repo = _repo(tmp_path)
    repo.record_source_stats(
        [{"source": "arbeitnow", "fetched": 575, "passed": 14}], run_at="2026-08-01T06:00:00+00:00"
    )
    repo.record_source_stats(
        [
            {"source": "arbeitnow", "fetched": 600, "passed": 20},
            {"source": "climatebase", "fetched": 0, "passed": 0, "error": "403"},
        ],
        run_at="2026-08-02T06:00:00+00:00",
    )
    latest = repo.latest_source_stats()
    assert {r["source"] for r in latest} == {"arbeitnow", "climatebase"}
    assert {r["source"]: r["fetched"] for r in latest}["arbeitnow"] == 600


def test_stats_upsert_within_a_run(tmp_path):
    repo = _repo(tmp_path)
    run = "2026-08-01T06:00:00+00:00"
    repo.record_source_stats([{"source": "jobicy", "fetched": 1, "passed": 0}], run_at=run)
    repo.record_source_stats([{"source": "jobicy", "fetched": 50, "passed": 3}], run_at=run)
    rows = repo.latest_source_stats()
    assert len(rows) == 1
    assert rows[0]["fetched"] == 50


def test_no_stats_is_not_an_error(tmp_path):
    assert _repo(tmp_path).latest_source_stats() == []


def test_footer_flags_silent_sources(tmp_path):
    repo = _repo(tmp_path)
    repo.record_source_stats(
        [
            {"source": "weworkremotely", "fetched": 368, "passed": 16},
            {"source": "climatebase", "fetched": 0, "passed": 0, "error": "403 Forbidden"},
            {"source": "remotive", "fetched": 34, "passed": 0},
        ],
        run_at="2026-08-01T06:00:00+00:00",
    )
    md = build_markdown_digest(
        [], [], digest_date=date(2026, 8, 1),
        source_stats=repo.latest_source_stats(),
        llm_usage={"used": 63, "budget": 800},
    )
    assert "Run health" in md
    # A markdown table would collapse into one line of pipes in both the plain-text
    # part and the HTML converter, so the footer must not use one.
    assert "|---" not in md
    # Summary line, then the sources that actually contributed.
    assert "**16 matches** from 402 postings across 3 sources." in md
    assert "- **weworkremotely** — 16 of 368" in md
    # Fetched-but-no-match is healthy and mentioned inline...
    assert "No matches from remotive (fetched normally)." in md
    # ...while fetched-nothing is called out separately as possible breakage.
    assert "nothing came back" in md
    assert "climatebase" in md.split("nothing came back")[1]
    assert "63/800" in md


def test_footer_reads_as_bullets_in_html():
    """The HTML part only renders headings, bullets and paragraphs."""
    from mail.markdown_html import markdown_to_html

    md = build_markdown_digest(
        [], [], digest_date=date(2026, 8, 1),
        source_stats=[{"source": "indeed", "fetched": 263, "passed": 42}],
        llm_usage={"used": 2, "budget": 300},
    )
    html = markdown_to_html(md[md.index("### Run health"):])
    assert "<h3>Run health</h3>" in html
    assert "<li><strong>indeed</strong> — 42 of 263</li>" in html
    assert "|" not in html


def test_footer_omitted_without_data():
    md = build_markdown_digest([], [], digest_date=date(2026, 8, 1))
    assert "Run health" not in md


def test_proxy_refresh_skips_fresh_file(tmp_path, monkeypatch):
    """A recently downloaded list is reused; only stale/missing files re-download."""
    from pipelines.job_boards.sources import proxy_pool

    path = tmp_path / "proxies.txt"
    path.write_text("1.2.3.4:8080:user:pass\n", encoding="utf-8")

    calls = []

    def _boom(*a, **k):
        calls.append(1)
        raise AssertionError("should not download a fresh file")

    monkeypatch.setattr(proxy_pool, "load_proxy_file", proxy_pool.load_proxy_file)
    import httpx

    monkeypatch.setattr(httpx, "get", _boom)
    assert proxy_pool.refresh_proxy_file(path, "https://example/list", max_age_hours=24) is False
    assert calls == []


def test_proxy_refresh_survives_download_failure(tmp_path, monkeypatch):
    """Webshare being down must never break ingest — boards just go direct."""
    import httpx

    from pipelines.job_boards.sources import proxy_pool

    path = tmp_path / "missing.txt"

    def _fail(*a, **k):
        raise httpx.ConnectError("network down")

    monkeypatch.setattr(httpx, "get", _fail)
    assert proxy_pool.refresh_proxy_file(path, "https://example/list") is False


def test_proxy_pool_without_url_is_quiet(tmp_path):
    from pipelines.job_boards.sources.proxy_pool import ProxyPool

    pool = ProxyPool(tmp_path / "nope.txt")
    assert pool.count == 0
    assert not pool


def test_bq_normalized_batch_dedupes_merge_key():
    """Two rows with the same (source, ats_slug, source_job_id) would make BigQuery
    reject the whole MERGE, silently dropping every job in the batch."""
    from unittest.mock import MagicMock

    from storage.bq_repository import JobBigQuery

    class _S:
        BQ_ENABLED = True
        BQ_WRITE_JOBS = True
        BQ_PROJECT_ID = "p"
        BQ_DATASET_ID = "d"
        BQ_LOCATION = "US"
        BQ_BATCH_CHUNK_SIZE = 50
        BQ_RAW_BATCH_SIZE = 50
        BQ_JOB_TIMEOUT_SECONDS = 5

    bq = JobBigQuery(_S())
    merged: list[list[dict]] = []
    bq._merge_normalized_batch = lambda rows: merged.append(rows)

    def _job(job_id: int, seen: str) -> dict:
        return {
            "id": job_id, "source": "indeed", "ats_slug": "indeed", "source_job_id": "dup-1",
            "company_name": "Acme", "title": "Data Engineer", "url": "u", "is_remote": True,
            "description_text": "d", "content_hash": "h", "prefilter_pass": 1,
            "first_seen_at": "2026-08-01T00:00:00+00:00", "last_seen_at": seen,
            "last_changed_at": seen,
        }

    bq.queue_normalized_job(_job(1, "2026-08-01T00:00:00+00:00"), ingested_at="2026-08-01T00:00:00+00:00")
    bq.queue_normalized_job(_job(2, "2026-08-02T00:00:00+00:00"), ingested_at="2026-08-02T00:00:00+00:00")
    bq.flush_normalized_jobs()

    assert len(merged) == 1
    assert len(merged[0]) == 1, "duplicate merge keys must be collapsed"
    assert merged[0][0]["last_seen_at"] == "2026-08-02T00:00:00+00:00", "keeps the freshest row"
