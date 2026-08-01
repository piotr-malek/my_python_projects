"""Work on Climate job board."""

from __future__ import annotations

from pipelines.job_boards.sources.html_board import scrape_html_board

LIST_URL = "https://www.workonclimate.org/jobs"


def fetch_jobs(**_kwargs: object) -> list[dict]:
    return scrape_html_board(
        source_id="workonclimate",
        list_url=LIST_URL,
        mission_category="climate",
    )
