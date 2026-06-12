"""Ollama structured scoring for job rows (parallel + optional multi-job batches)."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator

import ollama
from pydantic import ValidationError

from config import Settings
from normalize.schema import JobScorePayload
from profile.preferences import load_preferences
from rank.location_constraints import (
    apply_location_guard,
    format_location_constraints_for_prompt,
    location_constraints_from_job,
    location_policy_from_prefs,
)

logger = logging.getLogger(__name__)

SCORE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": [
        "role_relevance",
        "mission_alignment",
        "candidate_fit",
        "remote_ok",
        "extracted_salary",
        "top_requirements",
        "risks_or_gaps",
        "one_line_summary",
    ],
    "properties": {
        "role_relevance": {"type": "integer", "minimum": 0, "maximum": 100},
        "mission_alignment": {"type": "integer", "minimum": 0, "maximum": 100},
        "candidate_fit": {"type": "integer", "minimum": 0, "maximum": 100},
        "remote_ok": {"type": "boolean"},
        "extracted_salary": {"type": ["string", "null"]},
        "top_requirements": {"type": "array", "items": {"type": "string"}},
        "risks_or_gaps": {"type": "array", "items": {"type": "string"}},
        "one_line_summary": {"type": "string"},
    },
}

BATCH_SCORE_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["scores"],
    "properties": {
        "scores": {
            "type": "array",
            "items": SCORE_JSON_SCHEMA,
            "minItems": 1,
        }
    },
}

_thread_local = threading.local()


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
    def __init__(self, settings: Settings):
        self._settings = settings
        self._desc_limit = int(getattr(settings, "OLLAMA_DESC_TRUNCATE", 2000) or 2000)
        prompts = Path(__file__).parent / "prompts"
        self._template = (prompts / "score_job.txt").read_text(encoding="utf-8")
        self._batch_template = (prompts / "score_jobs_batch.txt").read_text(encoding="utf-8")
        prefs_path = getattr(settings, "PREFERENCES_PATH", None)
        prefs = load_preferences(prefs_path) if prefs_path else {}
        self._location_policy = location_policy_from_prefs(prefs)

    def _location_block(self, row: dict[str, Any]) -> str:
        constraints = location_constraints_from_job(row, policy=self._location_policy)
        return format_location_constraints_for_prompt(
            constraints,
            acceptable_hire_regions=self._location_policy.acceptable_hire_regions or None,
            allow_unspecified_location=self._location_policy.allow_unspecified_location,
        )

    def _guard_payload(self, row: dict[str, Any], payload: JobScorePayload) -> JobScorePayload:
        constraints = location_constraints_from_job(row, policy=self._location_policy)
        return apply_location_guard(payload, constraints, policy=self._location_policy)

    def _client(self) -> ollama.Client:
        client = getattr(_thread_local, "client", None)
        if client is None:
            client = ollama.Client(host=self._settings.OLLAMA_HOST)
            _thread_local.client = client
        return client

    def _generate_options(self, *, temperature: float, num_predict: int | None = None) -> dict[str, Any]:
        return {
            "temperature": temperature,
            "num_predict": num_predict if num_predict is not None else self._settings.OLLAMA_NUM_PREDICT,
            "num_ctx": 8192,
            "top_p": 0.9,
        }

    def _num_predict_for_batch(self, n_jobs: int) -> int:
        """Batch JSON needs more tokens than a single score object."""
        base = self._settings.OLLAMA_NUM_PREDICT
        if n_jobs <= 1:
            return base
        return max(base, min(8192, 320 * n_jobs + 512))

    def _call_ollama(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        temperatures: tuple[float, ...] = (0.15, 0.0),
        num_predict: int | None = None,
    ) -> dict[str, Any] | None:
        client = self._client()
        for attempt, temp in enumerate(temperatures):
            try:
                response = client.generate(
                    model=self._settings.OLLAMA_MODEL,
                    prompt=prompt,
                    format=schema,
                    keep_alive=self._settings.OLLAMA_KEEP_ALIVE,
                    options=self._generate_options(temperature=temp, num_predict=num_predict),
                )
                raw_text = response.get("response") or ""
                return _loads_json_response(raw_text)
            except (json.JSONDecodeError, ValidationError, ValueError) as exc:
                logger.info("score attempt %s failed: %s", attempt + 1, exc)
                continue
            except Exception as exc:
                logger.warning("ollama error: %s", exc)
                break
        return None

    def _build_single_prompt(self, row: dict[str, Any], scoring_input: str) -> str:
        return self._template.format(
            scoring_input=scoring_input,
            company_name=row.get("company_name") or "",
            mission_category=row.get("mission_category") or "",
            title=row.get("title") or "",
            location_text=row.get("location_text") or "",
            location_analysis=self._location_block(row),
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
                f"- Detected location constraints:\n{self._location_block(row)}\n"
                f"- Salary hint: {row.get('salary_text') or ''}\n"
                f"Description:\n{_truncate_desc(row.get('description_text'), per_job_limit)}\n"
            )
        return self._batch_template.format(
            scoring_input=scoring_input,
            target_keywords=", ".join(self._settings.TARGET_ROLE_KEYWORDS),
            jobs_block="\n".join(blocks),
        )

    def score_job(self, row: dict[str, Any], scoring_input: str) -> JobScorePayload | None:
        raw = self._call_ollama(
            prompt=self._build_single_prompt(row, scoring_input),
            schema=SCORE_JSON_SCHEMA,
        )
        if raw is None:
            return None
        try:
            return self._guard_payload(row, JobScorePayload.model_validate(raw))
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

        raw = self._call_ollama(
            prompt=self._build_batch_prompt(rows, scoring_input),
            schema=BATCH_SCORE_JSON_SCHEMA,
            num_predict=self._num_predict_for_batch(len(rows)),
        )
        if raw is None:
            if len(rows) > 1:
                mid = len(rows) // 2
                logger.info(
                    "Batch score failed for %s jobs — retrying as %s + %s",
                    len(rows),
                    mid,
                    len(rows) - mid,
                )
                return self._score_chunk(rows[:mid], scoring_input) + self._score_chunk(
                    rows[mid:], scoring_input
                )
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
                out.append((jid, self._guard_payload(row, JobScorePayload.model_validate(scores_raw[i]))))
            except ValidationError:
                out.append((jid, self.score_job(row, scoring_input)))
        return out

    def score_jobs_parallel(
        self,
        rows: list[dict[str, Any]],
        scoring_input: str,
    ) -> Iterator[tuple[int, JobScorePayload | None]]:
        """Score many jobs using a thread pool; optional multi-job Ollama batches."""
        if not rows:
            return

        batch_size = max(1, self._settings.OLLAMA_SCORE_BATCH_SIZE)
        workers = max(1, self._settings.OLLAMA_SCORE_WORKERS)
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
