"""Climatebase: the detail page's raw pageProps must go through _normalize_detail before the board normalizer."""

from normalize.boards import normalize_climatebase_listing
from pipelines.job_boards.sources.climatebase import _normalize_detail

LONG = "<p>" + "You will own our dbt models and the BigQuery warehouse. " * 12 + "</p>"
LISTING = {
    "id": 123,
    "title": "Analytics Engineer",
    "employer_name": "",
    "locations": ["Remote"],
    "employer_short_description": "We decarbonise shipping.",
}
# shape of props.pageProps on climatebase.org/job/<id>
PAGE_PROPS = {
    "sanitizedJobDescription": LONG,
    "data": {"id": 123, "title": "Analytics Engineer", "description": LONG, "employer": {"name": "Ocean Co"}},
}


def test_detail_description_and_employer_come_through():
    n = normalize_climatebase_listing(LISTING, _normalize_detail(PAGE_PROPS))
    assert len(n["description_text"]) > 500
    assert "dbt models" in n["description_text"] and "<p>" not in n["description_text"]
    assert n["company_name"] == "Ocean Co"


def test_raw_page_props_would_lose_the_description():
    # the bug fixed in ingest._ingest_climatebase: raw pageProps only yields the employer one-liner
    n = normalize_climatebase_listing(LISTING, PAGE_PROPS)
    assert n["description_text"] == "We decarbonise shipping."
