import json
from pathlib import Path

from normalize.handlers import normalize_greenhouse, normalize_lever, normalize_smartrecruiters


def _fx(name: str) -> dict:
    p = Path(__file__).parent / "fixtures" / name
    return json.loads(p.read_text(encoding="utf-8"))


def test_normalize_greenhouse_remote_title():
    job = _fx("greenhouse_job.json")
    n = normalize_greenhouse(
        job,
        company_name="TestCo",
        mission_category="climate",
        ats_slug="testco",
    )
    assert n["source"] == "greenhouse"
    assert "analytics" in n["title"].lower()
    assert n["is_remote"] is True
    assert "analytics engineer" in n["description_text"].lower()


def test_normalize_lever_remote():
    job = _fx("lever_job.json")
    n = normalize_lever(
        job,
        company_name="LeverCo",
        mission_category=None,
        ats_slug="leverco",
    )
    assert n["source"] == "lever"
    assert n["is_remote"] is True


def test_normalize_smartrecruiters_bundle():
    bundle = {
        "list": {
            "id": "744000128485745",
            "name": "Data Engineer",
            "releasedDate": "2026-04-01",
            "location": {"city": "Berlin", "country": "de", "remote": True, "fullLocation": "Berlin, Germany"},
            "ref": "https://api.smartrecruiters.com/v1/companies/x/postings/744000128485745",
        },
        "detail": {
            "postingUrl": "https://careers.example.com/job/1",
            "jobAd": {
                "sections": {
                    "jobDescription": {"title": "Job", "text": "<p>ETL and <b>dbt</b>.</p>"}
                }
            },
        },
    }
    n = normalize_smartrecruiters(
        bundle,
        company_name="SRCo",
        mission_category="climate",
        ats_slug="x",
    )
    assert n["source"] == "smartrecruiters"
    assert "dbt" in n["description_text"].lower()
