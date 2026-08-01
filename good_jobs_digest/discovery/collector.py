"""Unified employer candidate collection from v1, v2, and funder sources."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from discovery.employer_candidates import load_unprobed_candidates
from discovery.resolve import EmployerCandidate
from discovery.sources import DEFAULT_SEEDS_PATH, collect_all
from discovery.sources_curated import collect_curated_revalidate
from discovery.sources_funders import collect_funder_sources
from discovery.sources_mission_v2 import collect_mission_v2_incremental

if TYPE_CHECKING:
    from config import Settings
    from storage.bq_repository import JobBigQuery

logger = logging.getLogger(__name__)

V1_SOURCES = frozenset(
    {"80000hours", "escapethecity", "climatebase", "seeds", "eu_mission_tech", "eu_seeds", "bcorp"}
)
V2_SOURCES = frozenset({"coefficient", "sff", "gwwc", "ace", "givewell", "gates", "ea_funds"})
FUNDER_SOURCES = frozenset({"openphil", "echoing_green", "ashoka_fellows", "fast_forward"})
CURATED_REVALIDATE_SOURCES = frozenset({"curated_revalidate", "registry_revalidate"})


def collect_unified(
    sources: Iterable[str],
    *,
    seeds_path: Path = DEFAULT_SEEDS_PATH,
    climatebase_max_listings: int = 100,
    climatebase_fetch_details: bool = True,
    eighty_k_max_pages: int = 50,
    escapethecity_max_pages: int = 12,
    bcorp_max_pages: int = 0,
    bcorp_per_page: int = 250,
    bcorp_requests_per_second: float = 2.0,
    bcorp_reset_checkpoint: bool = False,
    include_mined: bool = True,
    mined_limit: int | None = None,
    settings: Settings | None = None,
    bq: JobBigQuery | None = None,
) -> list[EmployerCandidate]:
    wanted = {s.strip().lower() for s in sources if s.strip()}
    merged: dict[str, EmployerCandidate] = {}

    def _add(cand: EmployerCandidate) -> None:
        key = cand.company_name.strip().lower()
        if not key:
            return
        existing = merged.get(key)
        if existing is None:
            merged[key] = cand
            return
        if cand.ats_hint and not existing.ats_hint:
            existing.ats_hint = cand.ats_hint
        if cand.website and not existing.website:
            existing.website = cand.website
        if cand.discovery_source and cand.discovery_source not in (existing.discovery_source or ""):
            existing.discovery_source = f"{existing.discovery_source}+{cand.discovery_source}"

    v1_wanted = wanted & V1_SOURCES
    if v1_wanted:
        for cand in collect_all(
            sources=v1_wanted,
            climatebase_max_listings=climatebase_max_listings,
            climatebase_fetch_details=climatebase_fetch_details,
            eighty_k_max_pages=eighty_k_max_pages,
            escapethecity_max_pages=escapethecity_max_pages,
            seeds_path=seeds_path,
            bcorp_max_pages=bcorp_max_pages,
            bcorp_per_page=bcorp_per_page,
            bcorp_requests_per_second=bcorp_requests_per_second,
            bcorp_reset_checkpoint=bcorp_reset_checkpoint,
        ):
            _add(cand)

    v2_wanted = wanted & V2_SOURCES
    if v2_wanted:
        for cand in collect_mission_v2_incremental(sources=v2_wanted):
            _add(cand)

    funder_wanted = wanted & FUNDER_SOURCES
    if funder_wanted:
        for cand in collect_funder_sources(funder_wanted):
            _add(cand)

    curated_wanted = wanted & CURATED_REVALIDATE_SOURCES
    if curated_wanted:
        from config import settings as default_settings

        st = settings or default_settings
        for cand in collect_curated_revalidate(st, bq):
            _add(cand)

    if include_mined and not (wanted and wanted <= CURATED_REVALIDATE_SOURCES):
        for cand in load_unprobed_candidates(limit=mined_limit):
            _add(cand)

    return sorted(merged.values(), key=lambda c: c.company_name.lower())
