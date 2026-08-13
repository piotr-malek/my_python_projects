"""Structured LLM scoring for job rows (parallel + optional multi-job batches)."""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator

from pydantic import ValidationError

from config import Settings
from normalize.schema import JobScorePayload
from rank.llm import BudgetExhausted, MistralClient, make_llm_client

logger = logging.getLogger(__name__)

# Strict structured-output form: every object closed with additionalProperties
# false, every property required, and optional values expressed as a type union
# rather than a separate optional flag. Range limits (0-100) are deliberately
# absent — strict mode rejects `minimum`/`maximum`, and JobScorePayload enforces
# them anyway, so an out-of-range answer leaves the job unscored.
SCORE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "role_relevance",
        "mission_alignment",
        "candidate_fit",
        "remote_ok",
        "eu_hire_ok",
        "timezone_ok",
        "seniority_ok",
        "role_ok",
        "fit_reasons",
        "extracted_salary",
        "top_requirements",
        "risks_or_gaps",
        "one_line_summary",
    ],
    "properties": {
        "role_relevance": {"type": "integer"},
        "mission_alignment": {"type": "integer"},
        "candidate_fit": {"type": "integer"},
        "remote_ok": {"type": "boolean"},
        "eu_hire_ok": {"type": "boolean"},
        "timezone_ok": {"type": "boolean"},
        "seniority_ok": {"type": "boolean"},
        "role_ok": {"type": "boolean"},
        "fit_reasons": {"type": "array", "items": {"type": "string"}},
        "extracted_salary": {"type": ["string", "null"]},
        "top_requirements": {"type": "array", "items": {"type": "string"}},
        "risks_or_gaps": {"type": "array", "items": {"type": "string"}},
        "one_line_summary": {"type": "string"},
    },
    "additionalProperties": False,
}

BATCH_SCORE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["scores"],
    "properties": {
        "scores": {
            "type": "array",
            "items": SCORE_JSON_SCHEMA,
        }
    },
    "additionalProperties": False,
}

def _extract_json_object(text: str) -> str:
    if not text:
        return "{}"
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return text
    return text[start : end + 1]


def _repair_json_blob(blob: str) -> str:
    """Best-effort fixes for common LLM JSON mistakes."""
    s = blob.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```\s*$", "", s)
    # Trailing commas before } or ]
    s = re.sub(r",(\s*[}\]])", r"\1", s)
    return s


def _loads_json_response(text: str) -> dict[str, Any]:
    blob = _repair_json_blob(_extract_json_object(text))
    parsed = json.loads(blob)
    if not isinstance(parsed, dict):
        raise ValueError("expected JSON object")
    return parsed


def _truncate_desc(text: str, limit: int) -> str:
    desc = str(text or "")
    if len(desc) <= limit:
        return desc
    return desc[:limit] + "\n\n[truncated]"


class JobScorer:
    def __init__(self, settings: Settings, llm: MistralClient | None = None):
        self._settings = settings
        self._desc_limit = int(getattr(settings, "LLM_DESC_TRUNCATE", 2000) or 2000)
        prompts = Path(__file__).parent / "prompts"
        self._template = (prompts / "score_job.txt").read_text(encoding="utf-8")
        self._batch_template = (prompts / "score_jobs_batch.txt").read_text(encoding="utf-8")
        self._llm = llm or make_llm_client(settings)
        self.budget_exhausted = False

    @property
    def model(self) -> str:
        """Model id actually answering the prompts, for the BigQuery score rows."""
        return getattr(self._llm, "model", "unknown")

    @property
    def usage(self) -> Any:
        """The client's daily request ledger (None for test stubs)."""
        return getattr(self._llm, "usage", None)

    def _max_tokens_for_batch(self, n_jobs: int) -> int:
        """Batch JSON needs more tokens than a single score object."""
        base = int(getattr(self._settings, "LLM_MAX_OUTPUT_TOKENS", 4096))
        if n_jobs <= 1:
            return base
        return max(base, min(16384, 700 * n_jobs + 512))

    def _call_llm(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        temperature: float = 0.15,
        max_output_tokens: int | None = None,
    ) -> dict[str, Any] | None:
        """One call. The client owns the single retry, at temperature 0."""
        if self.budget_exhausted:
            return None
        try:
            return self._llm.generate_json(
                prompt,
                schema=schema,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
            )
        except BudgetExhausted as exc:
            # Stop the whole run cleanly; unscored jobs are retried tomorrow.
            logger.warning("Daily LLM budget spent (%s) — leaving rest unscored", exc)
            self.budget_exhausted = True
            return None

    def _build_single_prompt(self, row: dict[str, Any], scoring_input: str) -> str:
        return self._template.format(
            scoring_input=scoring_input,
            company_name=row.get("company_name") or "",
            mission_category=row.get("mission_category") or "",
            title=row.get("title") or "",
            location_text=row.get("location_text") or "",
            is_remote=bool(row.get("is_remote")),
            salary_text=row.get("salary_text") or "",
            target_keywords=", ".join(self._settings.TARGET_ROLE_KEYWORDS),
            description=_truncate_desc(row.get("description_text"), self._desc_limit),
        )

    def _build_batch_prompt(self, rows: list[dict[str, Any]], scoring_input: str) -> str:
        per_job_limit = max(800, self._desc_limit // max(1, len(rows)))
        blocks: list[str] = []
        for i, row in enumerate(rows, start=1):
            blocks.append(
                f"### Job {i} (id={row.get('id')})\n"
                f"- Company: {row.get('company_name') or ''}\n"
                f"- Mission category: {row.get('mission_category') or ''}\n"
                f"- Title: {row.get('title') or ''}\n"
                f"- Location: {row.get('location_text') or ''}\n"
                f"- Heuristic remote flag (may be wrong): {bool(row.get('is_remote'))}\n"
                f"- Salary hint: {row.get('salary_text') or ''}\n"
                f"Description:\n{_truncate_desc(row.get('description_text'), per_job_limit)}\n"
            )
        return self._batch_template.format(
            scoring_input=scoring_input,
            target_keywords=", ".join(self._settings.TARGET_ROLE_KEYWORDS),
            jobs_block="\n".join(blocks),
        )

    def score_job(self, row: dict[str, Any], scoring_input: str) -> JobScorePayload | None:
        raw = self._call_llm(
            prompt=self._build_single_prompt(row, scoring_input),
            schema=SCORE_JSON_SCHEMA,
        )
        if raw is None:
            return None
        try:
            return JobScorePayload.model_validate(raw)
        except ValidationError:
            return None

    def _score_chunk(
        self, rows: list[dict[str, Any]], scoring_input: str
    ) -> list[tuple[int, JobScorePayload | None]]:
        if not rows:
            return []
        if len(rows) == 1:
            jid = int(rows[0]["id"])
            return [(jid, self.score_job(rows[0], scoring_input))]

        raw = self._call_llm(
            prompt=self._build_batch_prompt(rows, scoring_input),
            schema=BATCH_SCORE_JSON_SCHEMA,
            max_output_tokens=self._max_tokens_for_batch(len(rows)),
        )
        # A failed batch used to split in half and recurse, and a row the batch
        # mangled used to be re-asked on its own. Both multiplied request cost
        # without much to show for it; these jobs now go out unscored instead.
        if raw is None:
            logger.warning("Batch score failed for %s job(s) — leaving them unscored", len(rows))
            return [(int(r["id"]), None) for r in rows]

        scores_raw = raw.get("scores")
        if not isinstance(scores_raw, list):
            return [(int(r["id"]), None) for r in rows]

        out: list[tuple[int, JobScorePayload | None]] = []
        for i, row in enumerate(rows):
            jid = int(row["id"])
            if i >= len(scores_raw):
                out.append((jid, None))
                continue
            try:
                out.append((jid, JobScorePayload.model_validate(scores_raw[i])))
            except ValidationError as exc:
                logger.info("Batch score unusable for job id=%s: %s", jid, exc)
                out.append((jid, None))
        return out

    def score_jobs_parallel(
        self,
        rows: list[dict[str, Any]],
        scoring_input: str,
    ) -> Iterator[tuple[int, JobScorePayload | None]]:
        """Score many jobs using a thread pool; optional multi-job Ollama batches."""
        if not rows:
            return

        batch_size = max(1, self._settings.LLM_SCORE_BATCH_SIZE)
        workers = max(1, self._settings.LLM_SCORE_WORKERS)
        chunks: list[list[dict[str, Any]]] = []
        for i in range(0, len(rows), batch_size):
            chunks.append(rows[i : i + batch_size])

        logger.info(
            "Scoring %s jobs (%s chunks, batch_size=%s, workers=%s)",
            len(rows),
            len(chunks),
            batch_size,
            workers,
        )
        t0 = time.monotonic()
        done = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._score_chunk, chunk, scoring_input): chunk for chunk in chunks
            }
            for future in as_completed(futures):
                chunk = futures[future]
                try:
                    results = future.result()
                except Exception as exc:
                    logger.warning("score chunk failed (%s jobs): %s", len(chunk), exc)
                    results = [(int(r["id"]), None) for r in chunk]
                for jid, payload in results:
                    done += 1
                    if done % 20 == 0 or done == len(rows):
                        elapsed = time.monotonic() - t0
                        rate = done / elapsed if elapsed > 0 else 0
                        logger.info("Scored %s/%s (%.1f jobs/min)", done, len(rows), rate * 60)
                    yield jid, payload
