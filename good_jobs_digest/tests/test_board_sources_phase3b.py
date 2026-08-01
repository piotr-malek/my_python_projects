"""Normalizers and parsing for the RemoteOK / WWR / Working Nomads / HN / Indeed sources.

Fixtures mirror real payload shapes verified against the live APIs (field names,
date formats and separators all differ per source).
"""

from __future__ import annotations

from normalize.boards import (
    normalize_hn_whoishiring,
    normalize_indeed,
    normalize_remoteok,
    normalize_weworkremotely,
    normalize_workingnomads,
)
from pipelines.job_boards.sources.hn_whoishiring import (
    _plain_text,
    looks_like_posting,
    split_fields,
)
from pipelines.job_boards.sources.remoteok import _fix_mojibake


def test_remoteok_normalizer():
    norm = normalize_remoteok(
        {
            "id": "1135735",
            "slug": "acme-data-engineer",
            "position": "Data Engineer",
            "company": "Acme",
            "location": "Barcelona, ",
            "tags": ["data", "python"],
            "description": "<p>pipelines</p>",
            "url": "https://remoteOK.com/remote-jobs/acme",
            "date": "2026-07-31T08:46:48+00:00",
            "salary_min": 0,
            "salary_max": 0,
        }
    )
    assert norm["source"] == "remoteok"
    assert norm["title"] == "Data Engineer"
    assert norm["location_text"] == "Barcelona"  # trailing ", " stripped
    assert norm["salary_text"] is None  # 0/0 means unknown, not a real floor
    assert "tags: data, python" in norm["description_text"]


def test_remoteok_mojibake_repair():
    assert _fix_mojibake("CataluÃ±a") == "Cataluña"
    assert _fix_mojibake("Berlin") == "Berlin"


def test_weworkremotely_splits_company_from_title():
    norm = normalize_weworkremotely(
        {
            "title": "Hygraph: Senior Data Engineer",
            "region": "Anywhere in the World",
            "country": "🇩🇪 Germany, and 🇪🇸 Spain",
            "state": "Berlin",
            "skills": "SQL, dbt",
            "type": "Full-Time",
            "pubDate": "Fri, 31 Jul 2026 11:59:36 +0000",
            "link": "https://weworkremotely.com/remote-jobs/hygraph-x",
            "guid": "https://weworkremotely.com/remote-jobs/hygraph-x",
            "description": "&lt;p&gt;build&lt;/p&gt;",
        }
    )
    assert norm["company_name"] == "Hygraph"
    assert norm["title"] == "Senior Data Engineer"
    assert "Germany" in norm["location_text"]
    assert "hireable countries" in norm["description_text"]
    # RFC-822 date passed through for the repository parser
    assert norm["posted_at_hint"].startswith("Fri,")


def test_weworkremotely_title_without_colon():
    norm = normalize_weworkremotely({"title": "Data Engineer", "link": "http://x"})
    assert norm["title"] == "Data Engineer"
    assert norm["company_name"] == "Unknown"


def test_workingnomads_keeps_timezone_line():
    norm = normalize_workingnomads(
        {
            "url": "https://www.workingnomads.com/job/go/1764739/",
            "title": "Data Engineer",
            "company_name": "Acme",
            "description": "<p>etl</p>",
            "location": "Time zone: CET (+/- 3 hours)",
            "tags": "python,sql",
            "category_name": "Development",
            "pub_date": "2026-07-31T15:21:46-04:00",
        }
    )
    # The timezone requirement must reach the LLM — it is the EU signal here.
    assert "CET" in norm["description_text"]
    assert norm["location_text"] == "Time zone: CET (+/- 3 hours)"


def test_hn_field_splitting_handles_dashes():
    assert split_fields("Acme | Data Engineer | REMOTE") == ["Acme", "Data Engineer", "REMOTE"]
    assert split_fields("Acme — Data Engineer — REMOTE") == ["Acme", "Data Engineer", "REMOTE"]


def test_hn_rejects_commentary():
    # Real reply seen in the July 2026 thread — must not become a "job".
    assert not looks_like_posting(
        "89k for sr data engineer in nyc onsite? thats really depressing",
        "89k for sr data engineer in nyc onsite? thats really depressing",
    )
    assert not looks_like_posting("CaseLight is looking for a developer.", "prose only")
    assert looks_like_posting(
        "Acme | Data Engineer | REMOTE (EU)", "Acme | Data Engineer | REMOTE (EU)"
    )
    assert looks_like_posting("Acme | Data Engineer", "see https://acme.com/jobs")


def test_hn_plain_text_unescapes_entities():
    raw = r'<a href="x">https:&#x2F;&#x2F;acme.com<\/a> Full-Time &amp; remote<p>Body'
    out = _plain_text(raw)
    assert "https://acme.com" in out
    assert "&#x2F;" not in out and "&amp;" not in out


def test_hn_normalizer_maps_fields():
    norm = normalize_hn_whoishiring(
        {
            "id": 123,
            "text": "Acme | Data Engineer | Berlin | REMOTE (EU)\n\nWe build things.",
            "first_line": "Acme | Data Engineer | Berlin | REMOTE (EU)",
            "parts": ["Acme", "Data Engineer", "Berlin", "REMOTE (EU)"],
            "created_at": "2026-07-01T15:01:52.000Z",
        }
    )
    assert norm["company_name"] == "Acme"
    assert norm["title"] == "Data Engineer"
    assert norm["is_remote"] is True
    assert norm["url"].endswith("id=123")


def test_indeed_normalizer():
    norm = normalize_indeed(
        {
            "id": "in-1",
            "title": "Data Engineer",
            "company": "Acme GmbH",
            "location": "Berlin, Germany",
            "job_url": "https://indeed.com/viewjob?jk=1",
            "job_url_direct": "https://acme.com/jobs/1",
            "description": "build pipelines",
            "is_remote": True,
            "date_posted": "2026-07-30",
            "min_amount": 70000,
            "max_amount": 90000,
            "currency": "EUR",
            "interval": "yearly",
            "search_country": "Germany",
        }
    )
    assert norm["source"] == "indeed"
    assert norm["url"] == "https://acme.com/jobs/1"  # direct URL preferred
    assert norm["is_remote"] is True
    assert "EUR" in norm["salary_text"]
    assert norm["posted_at_hint"] == "2026-07-30"
