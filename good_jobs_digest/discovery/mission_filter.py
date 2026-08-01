"""Gemini mission scoring: auto-approve employers at or above a liberal score threshold."""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from config import Settings
from rank.llm import BudgetExhausted, GeminiClient
from normalize.schema import EmployerMissionScoreResult
from rank.scorer import _extract_json_object

logger = logging.getLogger(__name__)

MISSION_SCORE_BATCH_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["results"],
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["company_name", "mission_score", "purpose_driven", "reason", "mission_type"],
                "properties": {
                    "company_name": {"type": "string"},
                    "mission_score": {"type": "integer", "minimum": 0, "maximum": 100},
                    "purpose_driven": {"type": "boolean"},
                    "reason": {"type": "string"},
                    "mission_type": {"type": "string"},
                },
            },
            "minItems": 1,
        }
    },
}


class _BatchMissionScorePayload(BaseModel):
    results: list[EmployerMissionScoreResult] = Field(min_length=1)


class EmployerMissionFilter:
    def __init__(self, settings: Settings, llm: GeminiClient | None = None):
        self._settings = settings
        prompt_path = (
            Path(__file__).resolve().parents[1] / "rank" / "prompts" / "score_mission_employers_batch.txt"
        )
        self._score_template = prompt_path.read_text(encoding="utf-8")
        self._llm = llm or GeminiClient(settings)
        self.budget_exhausted = False

    def _call_llm(self, prompt: str) -> dict[str, Any] | None:
        if self.budget_exhausted:
            return None
        for temp in (0.1, 0.0):
            try:
                raw = self._llm.generate_json(
                    prompt,
                    schema=MISSION_SCORE_BATCH_JSON_SCHEMA,
                    temperature=temp,
                )
            except BudgetExhausted as exc:
                logger.warning("Gemini daily budget spent (%s) — employers left unscored", exc)
                self.budget_exhausted = True
                return None
            if raw is not None:
                return raw
            logger.info("mission score produced no JSON (temp=%s)", temp)
        return None

    def _build_score_batch_prompt(self, employers: list[dict[str, str]]) -> str:
        blocks: list[str] = []
        for i, row in enumerate(employers, start=1):
            blocks.append(
                f"### Employer {i}\n"
                f"- Name: {row.get('company_name') or ''}\n"
                f"- Careers URL: {row.get('job_board_url') or ''}\n"
                f"- Discovery hint: {row.get('mission_category') or 'unknown'} "
                f"(source: {row.get('discovery_source') or 'unknown'})\n"
            )
        return self._score_template.format(employers_block="\n".join(blocks))

    def _score_chunk(self, employers: list[dict[str, str]]) -> list[dict[str, str]]:
        if not employers:
            return []

        raw = self._call_llm(self._build_score_batch_prompt(employers))
        if raw is None:
            logger.warning("mission score batch failed for %s employers", len(employers))
            return []

        try:
            payload = _BatchMissionScorePayload.model_validate(raw)
        except ValidationError:
            return self._score_chunk_fallback(employers)

        if len(payload.results) != len(employers):
            logger.warning(
                "mission score count mismatch: got %s results for %s employers",
                len(payload.results),
                len(employers),
            )
            return self._score_chunk_fallback(employers)

        scored: list[dict[str, str]] = []
        for row, verdict in zip(employers, payload.results, strict=True):
            out = dict(row)
            out["mission_score"] = str(verdict.mission_score)
            out["purpose_driven"] = "true" if verdict.purpose_driven else "false"
            out["mission_llm_reason"] = verdict.reason
            out["mission_type"] = verdict.mission_type
            scored.append(out)
        return scored

    def _score_chunk_fallback(self, employers: list[dict[str, str]]) -> list[dict[str, str]]:
        """Retry a failed batch one employer at a time.

        Never recurse on a single row: _score_chunk calls back here on a validation
        failure, so a row the model consistently mangles would otherwise ping-pong
        between the two until the stack blows.
        """
        if len(employers) <= 1:
            logger.warning(
                "mission score unusable for %s — skipping",
                employers[0].get("company_name") if employers else "(empty)",
            )
            return []
        scored: list[dict[str, str]] = []
        for row in employers:
            scored.extend(self._score_chunk([row]))
        return scored

    def score_employers(self, employers: list[dict[str, str]]) -> list[dict[str, str]]:
        """Score every employer (0-100 mission_score); does not drop any rows."""
        if not employers:
            return []

        batch_size = max(1, self._settings.LLM_MISSION_BATCH_SIZE)
        workers = max(1, self._settings.LLM_MISSION_WORKERS)
        chunks: list[list[dict[str, str]]] = [
            employers[i : i + batch_size] for i in range(0, len(employers), batch_size)
        ]

        logger.info(
            "Mission LLM score: %s employers (%s batches, size=%s, workers=%s)",
            len(employers),
            len(chunks),
            batch_size,
            workers,
        )
        t0 = time.monotonic()
        results: list[dict[str, str]] = []

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._score_chunk, chunk): chunk for chunk in chunks}
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as exc:
                    logger.warning("mission score chunk failed: %s", exc)

        elapsed = time.monotonic() - t0
        logger.info("Mission LLM score done: %s/%s scored in %.0fs", len(results), len(employers), elapsed)
        return results

    def filter_employers(
        self,
        employers: list[dict[str, str]],
        *,
        min_score: int | None = None,
    ) -> list[dict[str, str]]:
        """Score employers and auto-approve those at or above min_score (default: MISSION_APPROVE_MIN_SCORE)."""
        threshold = self._settings.MISSION_APPROVE_MIN_SCORE if min_score is None else min_score
        scored = self.score_employers(employers)
        approved: list[dict[str, str]] = []
        for row in scored:
            try:
                score = int(row.get("mission_score") or 0)
            except ValueError:
                score = 0
            if score >= threshold:
                approved.append(row)
            else:
                logger.info(
                    "MISSION_REJECT %s score=%s (min=%s) — %s",
                    row.get("company_name"),
                    score,
                    threshold,
                    row.get("mission_llm_reason") or "below threshold",
                )

        logger.info(
            "Mission auto-approve: %s/%s at score ≥%s",
            len(approved),
            len(employers),
            threshold,
        )
        return approved
