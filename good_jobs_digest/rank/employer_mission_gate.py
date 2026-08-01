"""Cached employer mission gate for board-sourced jobs."""

from __future__ import annotations

import logging
from typing import Any

from config import Settings
from discovery.employer_candidates import append_employer_candidates
from discovery.mission_filter import EmployerMissionFilter
from discovery.resolve import EmployerCandidate
from storage.repository import JobRepository

logger = logging.getLogger(__name__)

# Curated ATS employers are already mission-vetted via the registry.
_CURATED_ATS = frozenset(
    {
        "ashby",
        "greenhouse",
        "lever",
        "smartrecruiters",
        "workable",
        "recruitee",
        "personio",
        "workday",
        "bamboohr",
        "breezy",
        "jazzhr",
        "teamtailor",
    }
)


def _needs_mission_gate(job: dict[str, Any]) -> bool:
    ats = str(job.get("ats_type") or "").lower()
    if ats in _CURATED_ATS:
        return False
    # job_board and anything else from aggregators
    return True


def filter_jobs_by_employer_mission(
    jobs: list[dict[str, Any]],
    *,
    repo: JobRepository,
    settings: Settings,
    mission_filter: EmployerMissionFilter | None = None,
) -> list[dict[str, Any]]:
    """Drop board jobs whose employer fails the cached mission gate.

    Curated ATS employers always pass. Unknown employers are scored via
    EmployerMissionFilter (Ollama) once and cached in employer_mission.
    """
    if not getattr(settings, "EMPLOYER_MISSION_GATE_ENABLED", True):
        return jobs

    kept: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    pending_names: dict[str, dict[str, Any]] = {}

    for job in jobs:
        if not _needs_mission_gate(job):
            kept.append(job)
            continue
        name = str(job.get("company_name") or "").strip()
        if not name or name.lower() == "unknown":
            kept.append(job)
            continue
        cached = repo.get_employer_mission(name)
        if cached is not None:
            if int(cached["mission_pass"] or 0) == 1:
                kept.append(job)
            else:
                logger.debug(
                    "Mission gate reject (cached) %s — %s",
                    name,
                    cached["reason"],
                )
            continue
        pending.append(job)
        key = JobRepository.employer_key(name)
        if key not in pending_names:
            pending_names[key] = {
                "company_name": name,
                "job_board_url": str(job.get("url") or ""),
                "mission_category": str(job.get("mission_category") or "mission"),
                "discovery_source": str(job.get("source") or job.get("ats_slug") or "job_board"),
            }

    if not pending_names:
        return kept

    scorer = mission_filter or EmployerMissionFilter(settings)
    employers = list(pending_names.values())
    scored = scorer.score_employers(employers)
    by_name = {JobRepository.employer_key(r.get("company_name") or ""): r for r in scored}
    threshold = settings.MISSION_APPROVE_MIN_SCORE
    flywheel: list[EmployerCandidate] = []

    for key, meta in pending_names.items():
        row = by_name.get(key)
        if row is None:
            # Scoring failed — leave unscored for next run (do not cache a reject).
            logger.warning("Mission gate: no score for %s — skipping jobs this run", meta["company_name"])
            continue
        try:
            score = int(row.get("mission_score") or 0)
        except ValueError:
            score = 0
        passed = score >= threshold
        reason = str(row.get("mission_llm_reason") or "")
        mission_type = str(row.get("mission_type") or "")
        repo.upsert_employer_mission(
            company_name=meta["company_name"],
            mission_pass=passed,
            mission_score=score,
            reason=reason,
            mission_type=mission_type,
        )
        if passed:
            flywheel.append(
                EmployerCandidate(
                    company_name=meta["company_name"],
                    mission_category=meta.get("mission_category") or "mission",
                    website="",
                    discovery_source=f"mission_gate:{meta.get('discovery_source') or 'board'}",
                )
            )

    if flywheel:
        append_employer_candidates(flywheel)

    for job in pending:
        name = str(job.get("company_name") or "").strip()
        cached = repo.get_employer_mission(name)
        if cached is not None and int(cached["mission_pass"] or 0) == 1:
            kept.append(job)
        elif cached is None:
            # Failed to score — keep out of this run (retry next time).
            pass
        else:
            logger.info(
                "Mission gate reject %s score=%s — %s",
                name,
                cached["mission_score"],
                cached["reason"],
            )

    logger.info(
        "Employer mission gate: %s in → %s kept (%s unique employers checked)",
        len(jobs),
        len(kept),
        len(pending_names),
    )
    return kept
