"""Animal Advocacy Careers board."""

from __future__ import annotations

from pipelines.job_boards.sources.html_board import scrape_html_board

LIST_URL = "https://animaladvocacycareers.org/job-board/"


def fetch_jobs(**_kwargs: object) -> list[dict]:
    return scrape_html_board(
        source_id="animaladvocacycareers",
        list_url=LIST_URL,
        mission_category="animal_welfare",
    )
