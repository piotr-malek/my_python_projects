"""Curated employer board keys (BigQuery or shipped CSV)."""

from __future__ import annotations

from config import Settings
from core.curated_registry import load_curated_board_keys as _load_curated_board_keys
from storage.bq_repository import JobBigQuery

CURATED_ATS_TYPES = frozenset({"greenhouse", "lever", "smartrecruiters"})


def load_curated_board_keys(
    settings: Settings,
    bq: JobBigQuery | None = None,
    *,
    limit: int | None = None,
) -> set[tuple[str, str]]:
    """Return (ats_type, ats_slug) pairs from the active curated registry."""
    return _load_curated_board_keys(settings, bq, limit=limit, allowed=CURATED_ATS_TYPES)
