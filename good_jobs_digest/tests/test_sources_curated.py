"""Tests for curated registry revalidation queue."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from config import Settings
from discovery.collector import collect_unified
from discovery.resolve import EmployerCandidate
from discovery.sources_curated import (
    _website_from_row_urls,
    collect_curated_revalidate,
    enrich_candidate_website,
)


def test_website_from_row_urls_skips_ats_hosts():
    row = {
        "careers_url": "https://boards.greenhouse.io/acme",
        "job_board_url": "https://www.acme.org",
    }
    assert _website_from_row_urls(row) == "https://www.acme.org"


def test_collect_curated_revalidate_includes_rows_without_board_url():
    settings = Settings()
    fake_bq = MagicMock()
    fake_bq.fetch_curated_companies.return_value = [
        {
            "company_name": "No Board Yet",
            "job_board_url": "",
            "careers_url": "",
            "mission_category": "climate",
            "ats_type": "",
            "ats_slug": "",
        },
        {
            "company_name": "Greenhouse Co",
            "job_board_url": "https://boards.greenhouse.io/greenco",
            "careers_url": "https://boards.greenhouse.io/greenco",
            "mission_category": "mission",
            "ats_type": "greenhouse",
            "ats_slug": "greenco",
        },
    ]
    with patch("discovery.sources_curated.load_curated_records") as load:
        load.return_value = fake_bq.fetch_curated_companies.return_value
        candidates = collect_curated_revalidate(
            settings,
            fake_bq,
            resolve_greenhouse_websites=False,
        )
    names = {c.company_name for c in candidates}
    assert "No Board Yet" in names
    assert "Greenhouse Co" in names
    gh = next(c for c in candidates if c.company_name == "Greenhouse Co")
    assert gh.ats_hint == ("greenhouse", "greenco")


def test_enrich_candidate_website_from_greenhouse_api():
    candidate = EmployerCandidate(
        company_name="Acme",
        mission_category="mission",
        ats_hint=("greenhouse", "acme"),
    )
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"website": "https://acme.org"})))
    try:
        enrich_candidate_website(client, candidate)
    finally:
        client.close()
    assert candidate.website == "https://acme.org"


def test_collect_unified_mined_only():
    with patch("discovery.collector.load_unprobed_candidates") as load:
        load.return_value = [
            EmployerCandidate(company_name="Mined Co", mission_category="mission"),
        ]
        rows = collect_unified(["mined"], include_mined=True)
    load.assert_called_once()
    assert len(rows) == 1
    assert rows[0].company_name == "Mined Co"


def test_collect_unified_routes_curated_revalidate():
    with patch("discovery.collector.collect_curated_revalidate") as collect:
        collect.return_value = [
            EmployerCandidate(company_name="Queued Co", mission_category="mission"),
        ]
        rows = collect_unified(["curated_revalidate"], settings=Settings(), bq=MagicMock())
    collect.assert_called_once()
    assert len(rows) == 1
    assert rows[0].company_name == "Queued Co"
