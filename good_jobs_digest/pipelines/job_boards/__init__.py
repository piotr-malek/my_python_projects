"""Flow A: scrape mission job boards (Climatebase, 80k Hours, etc.)."""

from pipelines.job_boards.ingest import ingest_job_boards

__all__ = ["ingest_job_boards"]
