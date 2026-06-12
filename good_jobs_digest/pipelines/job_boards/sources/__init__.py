"""Job board source fetchers (Climatebase, 80k Hours, etc.)."""

from pipelines.job_boards.sources.climatebase import fetch_job_detail as fetch_climatebase_detail
from pipelines.job_boards.sources.climatebase import fetch_job_listings as fetch_climatebase
from pipelines.job_boards.sources.eighty_k_hours import fetch_jobs as fetch_80000hours
from pipelines.job_boards.sources.escapethecity import fetch_jobs as fetch_escapethecity
from pipelines.job_boards.sources.reliefweb import fetch_jobs as fetch_reliefweb
from pipelines.job_boards.sources.techjobsforgood import fetch_jobs as fetch_techjobsforgood
from pipelines.job_boards.sources.types import JobBoardFetchResult

__all__ = [
    "JobBoardFetchResult",
    "fetch_80000hours",
    "fetch_climatebase",
    "fetch_climatebase_detail",
    "fetch_escapethecity",
    "fetch_reliefweb",
    "fetch_techjobsforgood",
]
