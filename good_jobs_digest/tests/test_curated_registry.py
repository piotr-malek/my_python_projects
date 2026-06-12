"""Tests for curated registry loading (CSV fallback + BigQuery priority)."""

from __future__ import annotations

from pathlib import Path

import pytest

from config import Settings
from core.curated_registry import load_curated_board_keys, load_curated_records, load_curated_csv
from pipelines.curated_ats.loader import load_curated_companies


@pytest.fixture
def curated_csv(tmp_path: Path) -> Path:
    path = tmp_path / "curated_companies.csv"
    path.write_text(
        "company_name,job_board_url,mission_category,discovery_source\n"
        "Acme Impact,https://boards.greenhouse.io/acmeimpact,climate,seeds\n"
        "Bad URL Corp,https://example.com/jobs,mission,seeds\n",
        encoding="utf-8",
    )
    return path


def test_load_curated_csv(curated_csv: Path):
    rows = load_curated_csv(curated_csv)
    assert len(rows) == 2
    assert rows[0]["company_name"] == "Acme Impact"


def test_csv_fallback_when_bq_empty(curated_csv: Path, monkeypatch):
    class EmptyBq:
        def fetch_curated_companies(self, *, limit=None):
            return []

    settings = Settings()
    monkeypatch.setattr(settings, "CURATED_COMPANIES_PATH", curated_csv)
    rows = load_curated_records(settings, EmptyBq())
    assert len(rows) == 2
    assert rows[0]["job_board_url"].startswith("https://boards.greenhouse.io/")


def test_bq_takes_priority_over_csv(curated_csv: Path, monkeypatch):
    class PopulatedBq:
        def fetch_curated_companies(self, *, limit=None):
            return [
                {
                    "company_name": "From BQ",
                    "job_board_url": "https://jobs.lever.co/frombq",
                }
            ]

    settings = Settings()
    monkeypatch.setattr(settings, "CURATED_COMPANIES_PATH", curated_csv)
    rows = load_curated_records(settings, PopulatedBq())
    assert len(rows) == 1
    assert rows[0]["company_name"] == "From BQ"


def test_board_keys_from_csv(curated_csv: Path, monkeypatch):
    settings = Settings()
    monkeypatch.setattr(settings, "CURATED_COMPANIES_PATH", curated_csv)
    keys = load_curated_board_keys(settings, bq=None)
    assert ("greenhouse", "acmeimpact") in keys


def test_company_rows_skip_unparseable_urls(curated_csv: Path, monkeypatch):
    settings = Settings()
    monkeypatch.setattr(settings, "CURATED_COMPANIES_PATH", curated_csv)
    rows = load_curated_companies(settings, bq=None)
    assert len(rows) == 1
    assert rows[0].ats_type == "greenhouse"
