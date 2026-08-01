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
    # Fetched-but-no-match is healthy; fetched-nothing is called out.
    assert "| remotive | 34 | 0 |" in md
    assert "Sources returning nothing" in md
    assert "climatebase" in md.split("Sources returning nothing")[1]
    assert "63/800" in md


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
