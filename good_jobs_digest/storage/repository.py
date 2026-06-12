"""SQLite persistence for normalized jobs."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class JobRow:
    id: int
    company_name: str
    mission_category: str | None
    ats_type: str
    ats_slug: str
    source: str
    source_job_id: str
    title: str
    url: str
    location_text: str | None
    is_remote: int
    salary_text: str | None
    description_text: str
    content_hash: str
    first_seen_at: str
    last_seen_at: str
    last_changed_at: str
    prefilter_pass: int
    relevance_score: int | None
    mission_score: int | None
    fit_score: int | None
    remote_ok: int | None
    combined_score: float | None
    llm_json: str | None
    last_scored_at: str | None
    digest_included_at: str | None


class JobRepository:
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        schema = (Path(__file__).parent / "schema.sql").read_text()
        with self._conn() as conn:
            conn.executescript(schema)
            cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
            if "posted_at" not in cols:
                conn.execute("ALTER TABLE jobs ADD COLUMN posted_at TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_unscored "
                "ON jobs (prefilter_pass, last_scored_at)"
            )

    @staticmethod
    def _parse_posted_at_hint(hint: str | None) -> str | None:
        if not hint:
            return None
        s = str(hint).strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.replace(microsecond=0).isoformat()
        except ValueError:
            return s

    def upsert_job(
        self,
        *,
        company_name: str,
        mission_category: str | None,
        ats_type: str,
        ats_slug: str,
        source: str,
        source_job_id: str,
        title: str,
        url: str,
        location_text: str | None,
        is_remote: bool,
        salary_text: str | None,
        description_text: str,
        chash: str,
        now_iso: str | None = None,
        posted_at: str | None = None,
    ) -> tuple[int, bool]:
        """Insert or update job. Returns (job_id, content_changed_or_new)."""
        now_iso = now_iso or _utc_now_iso()
        is_remote_i = 1 if is_remote else 0
        posted_iso = self._parse_posted_at_hint(posted_at)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, content_hash FROM jobs
                WHERE source = ? AND ats_slug = ? AND source_job_id = ?
                """,
                (source, ats_slug, source_job_id),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO jobs (
                      company_name, mission_category, ats_type, ats_slug, source, source_job_id,
                      title, url, location_text, is_remote, salary_text, posted_at,
                      description_text, content_hash, first_seen_at, last_seen_at,
                      last_changed_at, prefilter_pass
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        company_name,
                        mission_category,
                        ats_type,
                        ats_slug,
                        source,
                        source_job_id,
                        title,
                        url,
                        location_text,
                        is_remote_i,
                        salary_text,
                        posted_iso,
                        description_text,
                        chash,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
                jid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                return jid, True

            jid = int(row["id"])
            old_hash = row["content_hash"]
            changed = old_hash != chash
            new_last_changed = now_iso if changed else None
            if changed:
                conn.execute(
                    """
                    UPDATE jobs SET
                      company_name = ?, mission_category = ?, title = ?, url = ?,
                      location_text = ?, is_remote = ?, salary_text = ?,
                      posted_at = COALESCE(?, posted_at), description_text = ?,
                      content_hash = ?, last_seen_at = ?, last_changed_at = ?,
                      relevance_score = NULL, mission_score = NULL, fit_score = NULL,
                      remote_ok = NULL, combined_score = NULL, llm_json = NULL, last_scored_at = NULL
                    WHERE id = ?
                    """,
                    (
                        company_name,
                        mission_category,
                        title,
                        url,
                        location_text,
                        is_remote_i,
                        salary_text,
                        posted_iso,
                        description_text,
                        chash,
                        now_iso,
                        now_iso,
                        jid,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE jobs SET
                      company_name = ?, mission_category = ?, title = ?, url = ?,
                      location_text = ?, is_remote = ?, salary_text = ?,
                      posted_at = COALESCE(?, posted_at), description_text = ?,
                      last_seen_at = ?
                    WHERE id = ?
                    """,
                    (
                        company_name,
                        mission_category,
                        title,
                        url,
                        location_text,
                        is_remote_i,
                        salary_text,
                        posted_iso,
                        description_text,
                        now_iso,
                        jid,
                    ),
                )
            return jid, changed

    def get_job(self, job_id: int) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    def get_job_by_key(self, source: str, ats_slug: str, source_job_id: str) -> sqlite3.Row | None:
        with self._conn() as conn:
            return conn.execute(
                """
                SELECT * FROM jobs
                WHERE source = ? AND ats_slug = ? AND source_job_id = ?
                """,
                (source, ats_slug, source_job_id),
            ).fetchone()

    def touch_job(self, source: str, ats_slug: str, source_job_id: str, now_iso: str | None = None) -> None:
        now_iso = now_iso or _utc_now_iso()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE jobs SET last_seen_at = ?
                WHERE source = ? AND ats_slug = ? AND source_job_id = ?
                """,
                (now_iso, source, ats_slug, source_job_id),
            )

    def set_prefilter(self, job_id: int, passes: bool) -> None:
        v = 1 if passes else 0
        with self._conn() as conn:
            conn.execute("UPDATE jobs SET prefilter_pass = ? WHERE id = ?", (v, job_id))

    def jobs_needing_score(
        self,
        *,
        limit: int | None = None,
        max_age_days: int | None = None,
    ) -> list[sqlite3.Row]:
        """Jobs that passed prefilter and are unscored or updated since last score."""
        age_clause = ""
        if max_age_days is not None and max_age_days > 0:
            age_clause = (
                f"AND datetime(COALESCE(posted_at, first_seen_at)) "
                f">= datetime('now', '-{int(max_age_days)} days')"
            )
        sql = f"""
            SELECT * FROM jobs
            WHERE prefilter_pass = 1
              AND (
                last_scored_at IS NULL
                OR datetime(last_changed_at) > datetime(last_scored_at)
              )
              {age_clause}
            ORDER BY first_seen_at ASC
            """
        if limit is not None and limit > 0:
            sql += f"\nLIMIT {int(limit)}"
        with self._conn() as conn:
            return list(conn.execute(sql).fetchall())

    def save_score(
        self,
        job_id: int,
        *,
        relevance: int,
        mission: int,
        fit: int,
        remote_ok: bool | None,
        combined: float,
        llm_payload: dict[str, Any],
    ) -> None:
        now = _utc_now_iso()
        remote_i = None if remote_ok is None else (1 if remote_ok else 0)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE jobs SET
                  relevance_score = ?, mission_score = ?, fit_score = ?,
                  remote_ok = ?, combined_score = ?, llm_json = ?, last_scored_at = ?
                WHERE id = ?
                """,
                (
                    relevance,
                    mission,
                    fit,
                    remote_i,
                    combined,
                    json.dumps(llm_payload),
                    now,
                    job_id,
                ),
            )

    def jobs_for_digest(
        self,
        *,
        min_combined: float,
        remote_only: bool,
        ats_types: list[str] | None = None,
        curated_board_keys: set[tuple[str, str]] | None = None,
        unsent_only: bool = True,
    ) -> list[sqlite3.Row]:
        """Jobs matching digest filters. unsent_only skips rows already emailed (SQLite)."""
        remote_clause = "AND remote_ok = 1" if remote_only else ""
        unsent_clause = "AND digest_included_at IS NULL" if unsent_only else ""
        score_clause = "AND combined_score IS NOT NULL"
        if min_combined > 0:
            score_clause += " AND combined_score >= ?"
        ats_clause = ""
        board_clause = ""
        params: list[Any] = []
        if min_combined > 0:
            params.append(min_combined)
        if ats_types:
            qmarks = ",".join("?" * len(ats_types))
            ats_clause = f"AND ats_type IN ({qmarks})"
            params.extend(ats_types)
        if curated_board_keys is not None:
            if not curated_board_keys:
                return []
            placeholders = ",".join("(?, ?)" for _ in curated_board_keys)
            board_clause = f"AND (LOWER(ats_type), LOWER(ats_slug)) IN ({placeholders})"
            for ats_type, ats_slug in sorted(curated_board_keys):
                params.extend([ats_type.lower(), ats_slug.lower()])
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE prefilter_pass = 1
                  {score_clause}
                  {unsent_clause}
                  {remote_clause}
                  {ats_clause}
                  {board_clause}
                ORDER BY combined_score DESC, company_name, title
                """,
                params,
            ).fetchall()
        return list(rows)

    def top_jobs_for_digest(self, **kwargs: Any) -> list[sqlite3.Row]:
        """Backward-compatible alias (ignores top_n if passed)."""
        kwargs.pop("top_n", None)
        return self.jobs_for_digest(**kwargs)

    def delete_stale_curated_jobs(self, allowed_board_keys: set[tuple[str, str]]) -> int:
        """Remove ATS jobs whose board is not in curated_companies."""
        allowed = {(a.lower(), s.lower()) for a, s in allowed_board_keys}
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, ats_type, ats_slug FROM jobs
                WHERE ats_type IN ('greenhouse', 'lever', 'smartrecruiters')
                """
            ).fetchall()
            to_delete = [
                int(r["id"])
                for r in rows
                if (str(r["ats_type"]).lower(), str(r["ats_slug"]).lower()) not in allowed
            ]
            if not to_delete:
                return 0
            qmarks = ",".join("?" * len(to_delete))
            cur = conn.execute(f"DELETE FROM jobs WHERE id IN ({qmarks})", to_delete)
            return cur.rowcount

    def mark_digest_included(self, job_ids: list[int], at_iso: str | None = None) -> None:
        at_iso = at_iso or _utc_now_iso()
        if not job_ids:
            return
        qmarks = ",".join("?" * len(job_ids))
        with self._conn() as conn:
            conn.execute(
                f"UPDATE jobs SET digest_included_at = ? WHERE id IN ({qmarks})",
                [at_iso, *job_ids],
            )

    @staticmethod
    def wipe_local_db(db_path: Path) -> list[str]:
        """Delete SQLite DB + WAL/SHM; return removed file names."""
        removed: list[str] = []
        for path in (db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm")):
            if path.exists():
                path.unlink()
                removed.append(path.name)
        return removed
