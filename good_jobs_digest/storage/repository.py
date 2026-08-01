"""SQLite persistence for normalized jobs."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# Columns added after the v1 schema. init_db() adds any that are missing so
# existing databases migrate in place (schema.sql only covers fresh databases).
_JOBS_EXTRA_COLUMNS: dict[str, str] = {
    "posted_at": "TEXT",
    "canonical_job_id": "TEXT",
    "registry_ats_type": "TEXT",
    "registry_ats_slug": "TEXT",
    "prefilter_reason": "TEXT",
    "eu_hire_ok": "INTEGER",
    "timezone_ok": "INTEGER",
    "seniority_ok": "INTEGER",
}


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
    prefilter_reason: str | None
    relevance_score: int | None
    mission_score: int | None
    fit_score: int | None
    remote_ok: int | None
    eu_hire_ok: int | None
    timezone_ok: int | None
    seniority_ok: int | None
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

    @staticmethod
    def _ensure_jobs_columns(conn: sqlite3.Connection) -> None:
        """Additive migration: add any missing jobs columns (no-op on fresh DBs)."""
        cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if not cols:
            return  # table not created yet; schema.sql handles it
        for name, ddl_type in _JOBS_EXTRA_COLUMNS.items():
            if name not in cols:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl_type}")

    def init_db(self) -> None:
        schema = (Path(__file__).parent / "schema.sql").read_text()
        with self._conn() as conn:
            # Migrate before executescript so index DDL on newer columns succeeds,
            # and after so fresh tables also end up complete.
            self._ensure_jobs_columns(conn)
            conn.executescript(schema)
            self._ensure_jobs_columns(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_unscored "
                "ON jobs (prefilter_pass, last_scored_at)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS curated_poll_state (
                  ats_type TEXT NOT NULL,
                  ats_slug TEXT NOT NULL,
                  last_polled_at TEXT NOT NULL,
                  PRIMARY KEY (ats_type, ats_slug)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS employer_mission (
                  employer_key TEXT PRIMARY KEY,
                  company_name TEXT NOT NULL,
                  mission_pass INTEGER NOT NULL,
                  mission_score INTEGER,
                  reason TEXT,
                  mission_type TEXT,
                  checked_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_employer_mission_pass "
                "ON employer_mission (mission_pass)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_source_stats (
                  run_at TEXT NOT NULL,
                  source TEXT NOT NULL,
                  fetched INTEGER NOT NULL DEFAULT 0,
                  passed INTEGER NOT NULL DEFAULT 0,
                  error TEXT,
                  PRIMARY KEY (run_at, source)
                )
                """
            )

    @staticmethod
    def _parse_posted_at_hint(hint: str | None) -> str | None:
        """Normalize a posted-at hint to ISO-8601.

        Accepts ISO strings, Unix epoch seconds/milliseconds (Arbeitnow, Himalayas
        and RemoteOK all send epochs) and RFC-822 dates (RSS feeds). An unparseable
        value returns None rather than the raw string: SQLite's datetime() yields
        NULL for junk, which would silently exclude the row from the age-filtered
        scoring query instead of falling back to first_seen_at.
        """
        if hint is None:
            return None
        s = str(hint).strip()
        if not s:
            return None

        if s.isdigit():
            value = int(s)
            # Milliseconds since epoch (13 digits) vs seconds (10).
            if value > 10_000_000_000:
                value //= 1000
            try:
                dt = datetime.fromtimestamp(value, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
            return dt.replace(microsecond=0).isoformat()

        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = parsedate_to_datetime(s)  # RFC-822 (RSS pubDate)
            except (TypeError, ValueError):
                logger.debug("Unparseable posted_at hint: %r", s)
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.replace(microsecond=0).isoformat()

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
        canonical_job_id: str | None = None,
        registry_ats_type: str | None = None,
        registry_ats_slug: str | None = None,
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
                      last_changed_at, prefilter_pass, canonical_job_id, registry_ats_type, registry_ats_slug
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
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
                        canonical_job_id,
                        registry_ats_type,
                        registry_ats_slug,
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
                      remote_ok = NULL, eu_hire_ok = NULL, timezone_ok = NULL, seniority_ok = NULL,
                      combined_score = NULL, llm_json = NULL, last_scored_at = NULL
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

    def find_by_canonical_id(self, canonical_job_id: str) -> sqlite3.Row | None:
        if not canonical_job_id:
            return None
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM jobs WHERE canonical_job_id = ? ORDER BY last_seen_at DESC LIMIT 1",
                (canonical_job_id,),
            ).fetchone()

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

    def set_prefilter(self, job_id: int, passes: bool, reason: str | None = None) -> None:
        v = 1 if passes else 0
        with self._conn() as conn:
            conn.execute(
                "UPDATE jobs SET prefilter_pass = ?, prefilter_reason = ? WHERE id = ?",
                (v, None if passes else reason, job_id),
            )

    def jobs_needing_score(
        self,
        *,
        limit: int | None = None,
        max_age_days: int | None = None,
    ) -> list[sqlite3.Row]:
        """Jobs that passed prefilter and are unscored, updated since last score,
        or scored before the fit booleans existed (seniority_ok IS NULL)."""
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
                OR seniority_ok IS NULL
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
        eu_hire_ok: bool | None = None,
        timezone_ok: bool | None = None,
        seniority_ok: bool | None = None,
    ) -> None:
        now = _utc_now_iso()

        def _b(v: bool | None) -> int | None:
            return None if v is None else (1 if v else 0)

        with self._conn() as conn:
            conn.execute(
                """
                UPDATE jobs SET
                  relevance_score = ?, mission_score = ?, fit_score = ?,
                  remote_ok = ?, eu_hire_ok = ?, timezone_ok = ?, seniority_ok = ?,
                  combined_score = ?, llm_json = ?, last_scored_at = ?
                WHERE id = ?
                """,
                (
                    relevance,
                    mission,
                    fit,
                    _b(remote_ok),
                    _b(eu_hire_ok),
                    _b(timezone_ok),
                    _b(seniority_ok),
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
        min_fit: float = 0.0,
        top_n: int | None = None,
    ) -> list[sqlite3.Row]:
        """Jobs matching digest filters. unsent_only skips rows already emailed (SQLite).

        Requires the LLM fit booleans (eu_hire_ok, timezone_ok, seniority_ok) to be
        true; rows scored before those existed are NULL and stay out until rescored.
        """
        remote_clause = "AND remote_ok = 1" if remote_only else ""
        fit_gate_clause = "AND eu_hire_ok = 1 AND timezone_ok = 1 AND seniority_ok = 1"
        unsent_clause = "AND digest_included_at IS NULL" if unsent_only else ""
        score_clause = "AND combined_score IS NOT NULL"
        if min_combined > 0:
            score_clause += " AND combined_score >= ?"
        min_fit_clause = ""
        ats_clause = ""
        board_clause = ""
        params: list[Any] = []
        if min_combined > 0:
            params.append(min_combined)
        if min_fit > 0:
            min_fit_clause = "AND fit_score >= ?"
            params.append(min_fit)
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
        limit_clause = ""
        if top_n is not None and top_n > 0:
            limit_clause = f"LIMIT {int(top_n)}"
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE prefilter_pass = 1
                  {score_clause}
                  {min_fit_clause}
                  {unsent_clause}
                  {remote_clause}
                  {fit_gate_clause}
                  {ats_clause}
                  {board_clause}
                ORDER BY combined_score DESC, company_name, title
                {limit_clause}
                """,
                params,
            ).fetchall()
        return list(rows)

    def top_jobs_for_digest(self, **kwargs: Any) -> list[sqlite3.Row]:
        """Backward-compatible alias."""
        return self.jobs_for_digest(**kwargs)

    def delete_stale_curated_jobs(self, allowed_board_keys: set[tuple[str, str]]) -> int:
        """Remove ATS jobs whose board is not in curated_companies."""
        allowed = {(a.lower(), s.lower()) for a, s in allowed_board_keys}
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, ats_type, ats_slug FROM jobs
                WHERE ats_type != 'job_board'
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

    def fetch_curated_poll_times(self) -> dict[tuple[str, str], str]:
        """Last poll timestamps for CSV-mode curated ingest (ats_type, ats_slug) -> ISO."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT ats_type, ats_slug, last_polled_at FROM curated_poll_state"
            ).fetchall()
        return {
            (str(r["ats_type"]).lower(), str(r["ats_slug"]).lower()): str(r["last_polled_at"])
            for r in rows
        }

    def touch_curated_poll(self, ats_type: str, ats_slug: str, *, polled_at: str | None = None) -> None:
        polled_at = polled_at or _utc_now_iso()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO curated_poll_state (ats_type, ats_slug, last_polled_at)
                VALUES (?, ?, ?)
                ON CONFLICT (ats_type, ats_slug) DO UPDATE SET last_polled_at = excluded.last_polled_at
                """,
                (ats_type.lower(), ats_slug.lower(), polled_at),
            )

    @staticmethod
    def employer_key(company_name: str) -> str:
        return (company_name or "").strip().lower()

    def get_employer_mission(self, company_name: str) -> sqlite3.Row | None:
        key = self.employer_key(company_name)
        if not key:
            return None
        with self._conn() as conn:
            return conn.execute(
                "SELECT * FROM employer_mission WHERE employer_key = ?",
                (key,),
            ).fetchone()

    def upsert_employer_mission(
        self,
        *,
        company_name: str,
        mission_pass: bool,
        mission_score: int | None = None,
        reason: str | None = None,
        mission_type: str | None = None,
        checked_at: str | None = None,
    ) -> None:
        key = self.employer_key(company_name)
        if not key:
            return
        checked_at = checked_at or _utc_now_iso()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO employer_mission (
                  employer_key, company_name, mission_pass, mission_score,
                  reason, mission_type, checked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (employer_key) DO UPDATE SET
                  company_name = excluded.company_name,
                  mission_pass = excluded.mission_pass,
                  mission_score = excluded.mission_score,
                  reason = excluded.reason,
                  mission_type = excluded.mission_type,
                  checked_at = excluded.checked_at
                """,
                (
                    key,
                    company_name.strip(),
                    1 if mission_pass else 0,
                    mission_score,
                    reason,
                    mission_type,
                    checked_at,
                ),
            )

    def record_source_stats(
        self,
        stats: list[dict[str, Any]],
        *,
        run_at: str | None = None,
    ) -> None:
        """Persist one ingest run's per-source outcome (fetched / passed / error)."""
        if not stats:
            return
        run_at = run_at or _utc_now_iso()
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT INTO run_source_stats (run_at, source, fetched, passed, error)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (run_at, source) DO UPDATE SET
                  fetched = excluded.fetched,
                  passed = excluded.passed,
                  error = excluded.error
                """,
                [
                    (
                        run_at,
                        str(s.get("source") or ""),
                        int(s.get("fetched") or 0),
                        int(s.get("passed") or 0),
                        s.get("error"),
                    )
                    for s in stats
                ],
            )

    def latest_source_stats(self) -> list[sqlite3.Row]:
        """Per-source stats from the most recent ingest run."""
        with self._conn() as conn:
            latest = conn.execute("SELECT MAX(run_at) FROM run_source_stats").fetchone()
            if not latest or not latest[0]:
                return []
            return list(
                conn.execute(
                    "SELECT * FROM run_source_stats WHERE run_at = ? ORDER BY source",
                    (latest[0],),
                ).fetchall()
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
