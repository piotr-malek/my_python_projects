"""BigQuery: curated employer registry (read) + normalized job mirror (batch load)."""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

logger = logging.getLogger(__name__)


from core.env import strip_env_path as _strip_env_path


def _bq_ts_iso(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _bq_client(project: str):
    """Prefer service account file; fall back to Application Default Credentials."""
    from google.oauth2 import service_account
    import os
    from google.auth.exceptions import DefaultCredentialsError

    sa = _strip_env_path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    if sa:
        p = Path(sa).expanduser()
        if not p.is_file():
            raise RuntimeError(
                f"GOOGLE_APPLICATION_CREDENTIALS points to a missing or unreadable file: {p}. "
                "Fix the path in .env or remove the variable to use Application Default Credentials."
            )
        creds = service_account.Credentials.from_service_account_file(str(p))
        return bigquery.Client(credentials=creds, project=project)
    try:
        from google.auth import default

        creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return bigquery.Client(credentials=creds, project=project)
    except DefaultCredentialsError as exc:
        raise RuntimeError(
            "No Google credentials found. Either set GOOGLE_APPLICATION_CREDENTIALS to a service "
            "account JSON path, or run: gcloud auth application-default login"
        ) from exc


class JobBigQuery:
    """Read curated_companies; batch-load jobs_normalized (no streaming inserts by default)."""

    def __init__(self, settings):
        self._settings = settings
        self._client: bigquery.Client | None = None
        self._raw_buffer: list[dict[str, Any]] = []
        self._raw_buffer_max = int(getattr(settings, "BQ_RAW_BATCH_SIZE", 50) or 50)
        self._normalized_buffer: list[tuple[dict[str, Any], str]] = []
        self._llm_score_buffer: list[dict[str, Any]] = []
        self._batch_chunk = int(getattr(settings, "BQ_BATCH_CHUNK_SIZE", 50) or 50)
        self._job_timeout = float(getattr(settings, "BQ_JOB_TIMEOUT_SECONDS", 120) or 0)

    def _await(self, job, *, what: str = "BigQuery job"):
        """Wait for a job, but never forever.

        google-cloud-bigquery blocks indefinitely by default, so a dropped socket
        hangs the whole run — which is fatal for an unattended scheduled job.
        On timeout we cancel and raise; callers already treat BQ failures as
        non-fatal and fall back to local state.
        """
        if not self._job_timeout:
            return job.result()
        try:
            return job.result(timeout=self._job_timeout)
        except Exception:
            try:
                job.cancel()
            except Exception:  # noqa: BLE001
                pass
            logger.warning("%s exceeded %ss — cancelled", what, self._job_timeout)
            raise

    def _write_jobs(self) -> bool:
        return bool(getattr(self._settings, "BQ_WRITE_JOBS", True))

    def _write_raw_payloads(self) -> bool:
        return bool(getattr(self._settings, "BQ_WRITE_RAW_PAYLOADS", False))

    def _write_llm_scores(self) -> bool:
        return bool(getattr(self._settings, "BQ_WRITE_LLM_SCORES", False))

    def _write_digest_history(self) -> bool:
        return bool(getattr(self._settings, "BQ_WRITE_DIGEST_HISTORY", False))

    def _append_rows_load(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        """Append rows via a load job (not billed as Streaming Insert)."""
        if not rows:
            return
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = self.client.load_table_from_json(
            rows,
            self.table_id(table_name),
            job_config=job_config,
            location=self._settings.BQ_LOCATION,
        )
        self._await(load_job, what='load job')
        if load_job.error_result or load_job.errors:
            logger.warning(
                "BQ load append to %s failed (%s rows): %s",
                table_name,
                len(rows),
                load_job.error_result or load_job.errors,
            )

    @staticmethod
    def _normalized_staging_row(job: dict[str, Any], *, ingested_at: str) -> dict[str, Any]:
        return {
            "source": job["source"],
            "ats_slug": job["ats_slug"],
            "source_job_id": job["source_job_id"],
            "sqlite_job_id": int(job["id"]),
            "company_name": job["company_name"],
            "mission_category": job.get("mission_category"),
            "title": job["title"],
            "url": job["url"],
            "location_text": job.get("location_text"),
            "is_remote": bool(job.get("is_remote")),
            "salary_text": job.get("salary_text"),
            "description_text": job["description_text"],
            "content_hash": job["content_hash"],
            "prefilter_pass": int(job.get("prefilter_pass") or 0),
            "first_seen_at": job["first_seen_at"],
            "last_seen_at": job["last_seen_at"],
            "last_changed_at": job["last_changed_at"],
            "ingested_at": ingested_at,
        }

    @property
    def enabled(self) -> bool:
        return bool(getattr(self._settings, "BQ_ENABLED", True))

    @property
    def client(self) -> bigquery.Client:
        if self._client is None:
            self._client = _bq_client(self._settings.BQ_PROJECT_ID)
        return self._client

    def table_id(self, name: str) -> str:
        return f"{self._settings.BQ_PROJECT_ID}.{self._settings.BQ_DATASET_ID}.{name}"

    def fqtn(self, name: str) -> str:
        return f"`{self.table_id(name)}`"

    def ensure_dataset(self) -> None:
        ds_id = f"{self._settings.BQ_PROJECT_ID}.{self._settings.BQ_DATASET_ID}"
        try:
            self.client.get_dataset(ds_id)
        except Exception:
            ds = bigquery.Dataset(ds_id)
            ds.location = self._settings.BQ_LOCATION
            self.client.create_dataset(ds, exists_ok=True)
            logger.info("Created BigQuery dataset %s", ds_id)

    def ensure_tables(self) -> None:
        self.ensure_dataset()
        ddl_statements = [
            f"""
CREATE TABLE IF NOT EXISTS {self.fqtn("raw_api_payloads")}
(
  fetched_at TIMESTAMP NOT NULL,
  ingest_batch_id STRING NOT NULL,
  ats_type STRING NOT NULL,
  ats_slug STRING NOT NULL,
  company_name STRING NOT NULL,
  source_job_id STRING,
  request_url STRING,
  http_status INT64,
  payload_kind STRING NOT NULL,
  payload_json STRING NOT NULL
)
PARTITION BY DATE(fetched_at)
CLUSTER BY ats_type, ats_slug, source_job_id
""",
            f"""
CREATE TABLE IF NOT EXISTS {self.fqtn("jobs_normalized")}
(
  source STRING NOT NULL,
  ats_slug STRING NOT NULL,
  source_job_id STRING NOT NULL,
  sqlite_job_id INT64,
  company_name STRING NOT NULL,
  mission_category STRING,
  title STRING NOT NULL,
  url STRING NOT NULL,
  location_text STRING,
  is_remote BOOL NOT NULL,
  salary_text STRING,
  description_text STRING NOT NULL,
  content_hash STRING NOT NULL,
  prefilter_pass INT64 NOT NULL,
  first_seen_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL,
  last_changed_at TIMESTAMP NOT NULL,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source, ats_slug, source_job_id
""",
            f"""
CREATE TABLE IF NOT EXISTS {self.fqtn("llm_score_events")}
(
  scored_at TIMESTAMP NOT NULL,
  sqlite_job_id INT64 NOT NULL,
  source STRING NOT NULL,
  ats_slug STRING NOT NULL,
  source_job_id STRING NOT NULL,
  ollama_model STRING NOT NULL,
  role_relevance INT64,
  mission_alignment INT64,
  candidate_fit INT64,
  remote_ok BOOL,
  combined_score FLOAT64,
  llm_json STRING NOT NULL
)
PARTITION BY DATE(scored_at)
CLUSTER BY source, ats_slug, source_job_id
""",
            f"""
CREATE TABLE IF NOT EXISTS {self.fqtn("selected_digest_jobs")}
(
  selected_at TIMESTAMP NOT NULL,
  digest_date DATE NOT NULL,
  sqlite_job_id INT64 NOT NULL,
  source STRING NOT NULL,
  ats_slug STRING NOT NULL,
  source_job_id STRING NOT NULL,
  company_name STRING NOT NULL,
  title STRING NOT NULL,
  url STRING NOT NULL,
  combined_score FLOAT64,
  remote_ok BOOL,
  llm_json STRING
)
PARTITION BY digest_date
CLUSTER BY source, ats_slug, source_job_id
""",
            f"""
CREATE TABLE IF NOT EXISTS {self.fqtn("curated_companies")}
(
  company_name STRING NOT NULL,
  job_board_url STRING NOT NULL,
  added_at TIMESTAMP NOT NULL,
  discovery_source STRING,
  mission_category STRING,
  ats_type STRING,
  ats_slug STRING,
  ats_region STRING,
  careers_url STRING,
  last_validated_at TIMESTAMP
)
PARTITION BY DATE(added_at)
CLUSTER BY company_name, ats_type, ats_slug
""",
        ]
        for stmt in ddl_statements:
            job = self.client.query(stmt.strip(), location=self._settings.BQ_LOCATION)
            self._await(job, what='query job')
        self._migrate_curated_companies_columns()
        logger.info("BigQuery tables ensured in %s", self.table_id("jobs_normalized").rsplit(".", 1)[0])

    def _migrate_curated_companies_columns(self) -> None:
        """Add metadata columns to existing curated_companies tables."""
        table = self.table_id("curated_companies")
        try:
            existing = {f.name for f in self.client.get_table(table).schema}
        except Exception:
            return
        alters = []
        for col, typ in (
            ("discovery_source", "STRING"),
            ("mission_category", "STRING"),
            ("ats_type", "STRING"),
            ("ats_slug", "STRING"),
            ("ats_region", "STRING"),
            ("careers_url", "STRING"),
            ("last_validated_at", "TIMESTAMP"),
        ):
            if col not in existing:
                alters.append(f"ADD COLUMN {col} {typ}")
        if not alters:
            return
        sql = f"ALTER TABLE {self.fqtn('curated_companies')} {', '.join(alters)}"
        job = self.client.query(sql, location=self._settings.BQ_LOCATION)
        self._await(job, what='query job')
        logger.info("Migrated curated_companies schema (%s)", ", ".join(alters))

    def verify_tables(self) -> list[str]:
        """Return table IDs after ensure_tables (raises if any missing)."""
        missing = []
        for name in (
            "raw_api_payloads",
            "jobs_normalized",
            "llm_score_events",
            "selected_digest_jobs",
            "curated_companies",
        ):
            tid = self.table_id(name)
            try:
                self.client.get_table(tid)
            except Exception:
                missing.append(tid)
        if missing:
            raise RuntimeError(f"BigQuery tables missing after DDL: {missing}")
        return [
            self.table_id(n)
            for n in (
                "raw_api_payloads",
                "jobs_normalized",
                "llm_score_events",
                "selected_digest_jobs",
                "curated_companies",
            )
        ]

    def flush_raw_payloads(self) -> None:
        """Flush buffered raw API rows via load job (call at end of ingest)."""
        if not self._write_raw_payloads() or not self._raw_buffer:
            self._raw_buffer = []
            return
        batch = self._raw_buffer
        self._raw_buffer = []
        self._append_rows_load("raw_api_payloads", batch)
        logger.info("BQ loaded %s raw_api_payloads rows", len(batch))

    def insert_raw_payload(
        self,
        *,
        fetched_at: str,
        ingest_batch_id: str,
        ats_type: str,
        ats_slug: str,
        company_name: str,
        source_job_id: str | None,
        request_url: str | None,
        http_status: int | None,
        payload_kind: str,
        payload: Any,
    ) -> None:
        if not self._write_raw_payloads():
            return
        row = {
            "fetched_at": fetched_at,
            "ingest_batch_id": ingest_batch_id,
            "ats_type": ats_type,
            "ats_slug": ats_slug,
            "company_name": company_name,
            "source_job_id": source_job_id,
            "request_url": request_url,
            "http_status": http_status,
            "payload_kind": payload_kind,
            "payload_json": json.dumps(payload, default=str),
        }
        self._raw_buffer.append(row)
        if len(self._raw_buffer) >= self._raw_buffer_max:
            self.flush_raw_payloads()

    def queue_normalized_job(self, job: dict[str, Any], *, ingested_at: str) -> None:
        """Buffer normalized row for batch flush (call flush_normalized_jobs at end of ingest)."""
        self._normalized_buffer.append((job, ingested_at))
        if len(self._normalized_buffer) >= self._batch_chunk:
            self.flush_normalized_jobs()

    def flush_normalized_jobs(self) -> None:
        if not self._write_jobs() or not self._normalized_buffer:
            self._normalized_buffer = []
            return
        batch = self._normalized_buffer
        self._normalized_buffer = []
        rows = [
            self._normalized_staging_row(job, ingested_at=ingested_at)
            for job, ingested_at in batch
        ]
        # BigQuery rejects an entire MERGE ("must match at most one source row for
        # each target row") if two staged rows share the join key, so the same
        # posting queued twice in one run would silently drop the whole batch.
        # Keep the last occurrence — it carries the freshest last_seen_at.
        deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            deduped[(row["source"], row["ats_slug"], row["source_job_id"])] = row
        if len(deduped) != len(rows):
            logger.info(
                "BQ normalized batch: %s rows collapsed to %s unique job keys",
                len(rows),
                len(deduped),
            )
        rows = list(deduped.values())
        ok = 0
        for i in range(0, len(rows), self._batch_chunk):
            chunk = rows[i : i + self._batch_chunk]
            try:
                self._merge_normalized_batch(chunk)
                ok += len(chunk)
            except Exception as exc:  # noqa: BLE001
                logger.warning("BQ normalized batch merge failed (%s rows): %s", len(chunk), exc)
        logger.info("BQ flushed %s/%s normalized jobs", ok, len(batch))

    def _merge_normalized_batch(self, rows: list[dict[str, Any]]) -> None:
        tmp_name = f"_jobs_staging_{uuid.uuid4().hex[:12]}"
        tmp_table = self.table_id(tmp_name)
        load_job = self.client.load_table_from_json(
            rows,
            tmp_table,
            location=self._settings.BQ_LOCATION,
        )
        self._await(load_job, what='load job')
        if load_job.error_result or load_job.errors:
            raise RuntimeError(load_job.error_result or load_job.errors)
        sql = f"""
MERGE {self.fqtn("jobs_normalized")} T
USING {self.fqtn(tmp_name)} S
ON T.source = S.source AND T.ats_slug = S.ats_slug AND T.source_job_id = S.source_job_id
WHEN MATCHED THEN
  UPDATE SET
    sqlite_job_id = S.sqlite_job_id,
    company_name = S.company_name,
    mission_category = S.mission_category,
    title = S.title,
    url = S.url,
    location_text = S.location_text,
    is_remote = S.is_remote,
    salary_text = S.salary_text,
    description_text = S.description_text,
    content_hash = S.content_hash,
    prefilter_pass = S.prefilter_pass,
    first_seen_at = TIMESTAMP(S.first_seen_at),
    last_seen_at = TIMESTAMP(S.last_seen_at),
    last_changed_at = TIMESTAMP(S.last_changed_at),
    ingested_at = TIMESTAMP(S.ingested_at)
WHEN NOT MATCHED THEN
  INSERT (
    source, ats_slug, source_job_id, sqlite_job_id, company_name, mission_category,
    title, url, location_text, is_remote, salary_text, description_text, content_hash,
    prefilter_pass, first_seen_at, last_seen_at, last_changed_at, ingested_at
  )
  VALUES (
    S.source, S.ats_slug, S.source_job_id, S.sqlite_job_id, S.company_name, S.mission_category,
    S.title, S.url, S.location_text, S.is_remote, S.salary_text, S.description_text, S.content_hash,
    S.prefilter_pass, TIMESTAMP(S.first_seen_at), TIMESTAMP(S.last_seen_at),
    TIMESTAMP(S.last_changed_at), TIMESTAMP(S.ingested_at)
  )
"""
        job = self.client.query(sql, location=self._settings.BQ_LOCATION)
        self._await(job, what='query job')
        self.client.delete_table(tmp_table, not_found_ok=True)

    def queue_llm_score(self, row: dict[str, Any]) -> None:
        self._llm_score_buffer.append(row)
        if len(self._llm_score_buffer) >= self._batch_chunk:
            self.flush_llm_scores()

    def flush_llm_scores(self) -> None:
        if not self._write_llm_scores() or not self._llm_score_buffer:
            self._llm_score_buffer = []
            return
        batch = self._llm_score_buffer
        self._llm_score_buffer = []
        self._append_rows_load("llm_score_events", batch)
        logger.info("BQ loaded %s llm_score_events", len(batch))

    def merge_normalized_job(self, job: dict[str, Any], *, ingested_at: str) -> None:
        """Upsert one row in jobs_normalized (expects SQLite jobs row as dict)."""
        if not self._write_jobs():
            return
        self._merge_normalized_batch([self._normalized_staging_row(job, ingested_at=ingested_at)])

    def fetch_for_scoring(self, source: str, ats_slug: str, source_job_id: str) -> dict[str, Any] | None:
        sql = f"""
        SELECT company_name, mission_category, title, url, location_text, is_remote,
               salary_text, description_text
        FROM {self.fqtn("jobs_normalized")}
        WHERE source = @source AND ats_slug = @ats_slug AND source_job_id = @source_job_id
        ORDER BY ingested_at DESC
        LIMIT 1
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("source", "STRING", source),
                    ScalarQueryParameter("ats_slug", "STRING", ats_slug),
                    ScalarQueryParameter("source_job_id", "STRING", source_job_id),
                ]
            ),
            location=self._settings.BQ_LOCATION,
        )
        rows = list(self._await(job, what='query job'))
        if not rows:
            return None
        r = rows[0]
        return {
            "company_name": r["company_name"] or "",
            "mission_category": (r["mission_category"] or "") if r["mission_category"] is not None else "",
            "title": r["title"] or "",
            "url": r["url"] or "",
            "location_text": r["location_text"] or "",
            "is_remote": bool(r["is_remote"]),
            "salary_text": r["salary_text"] or "",
            "description_text": r["description_text"] or "",
        }

    def append_llm_score(
        self,
        *,
        sqlite_job_id: int,
        source: str,
        ats_slug: str,
        source_job_id: str,
        ollama_model: str,
        role_relevance: int,
        mission_alignment: int,
        candidate_fit: int,
        remote_ok: bool,
        combined_score: float,
        llm_json: str,
        scored_at: str,
    ) -> None:
        if not self._write_llm_scores():
            return
        row = {
            "scored_at": scored_at,
            "sqlite_job_id": sqlite_job_id,
            "source": source,
            "ats_slug": ats_slug,
            "source_job_id": source_job_id,
            "ollama_model": ollama_model,
            "role_relevance": role_relevance,
            "mission_alignment": mission_alignment,
            "candidate_fit": candidate_fit,
            "remote_ok": remote_ok,
            "combined_score": combined_score,
            "llm_json": llm_json,
        }
        if getattr(self._settings, "BQ_BATCH_LLM_SCORES", True):
            self.queue_llm_score(row)
            return
        self._append_rows_load("llm_score_events", [row])

    def fetch_sent_job_keys(self) -> set[tuple[str, str, str]]:
        """Job identities already included in a prior digest email."""
        if not self._write_digest_history():
            return set()
        query = f"""
            SELECT DISTINCT source, ats_slug, source_job_id
            FROM `{self.table_id("selected_digest_jobs")}`
            """
        try:
            job = self.client.query(query, location=self._settings.BQ_LOCATION)
            rows = self._await(job, what='query job')
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load sent digest keys from BQ: %s", exc)
            return set()
        return {
            (str(r["source"]).lower(), str(r["ats_slug"]).lower(), str(r["source_job_id"]))
            for r in rows
        }

    def append_selected_jobs(self, *, digest_date: str, selected_at: str, rows: list[dict[str, Any]]) -> None:
        if not self._write_digest_history() or not rows:
            return
        payload = []
        for r in rows:
            payload.append(
                {
                    "selected_at": selected_at,
                    "digest_date": digest_date,
                    "sqlite_job_id": int(r["id"]),
                    "source": r["source"],
                    "ats_slug": r["ats_slug"],
                    "source_job_id": r["source_job_id"],
                    "company_name": r["company_name"],
                    "title": r["title"],
                    "url": r["url"],
                    "combined_score": float(r["combined_score"]) if r.get("combined_score") is not None else None,
                    "remote_ok": bool(r["remote_ok"]) if r.get("remote_ok") is not None else None,
                    "llm_json": r.get("llm_json"),
                }
            )
        self._append_rows_load("selected_digest_jobs", payload)

    def insert_curated_companies(self, rows: list[dict[str, str]], *, added_at: str) -> int:
        """Insert new purpose-driven employers; skip URLs already present (keeps first added_at)."""
        if not rows:
            return 0

        seen_urls: set[str] = set()
        payload: list[dict[str, str]] = []
        for row in rows:
            url = (row.get("job_board_url") or row.get("careers_url") or "").strip()
            name = (row.get("company_name") or "").strip()
            if not name or not url:
                continue
            key = url.lower()
            if key in seen_urls:
                continue
            seen_urls.add(key)
            payload.append(
                {
                    "company_name": name,
                    "job_board_url": url,
                    "added_at": added_at,
                    "discovery_source": (row.get("discovery_source") or "").strip() or None,
                    "mission_category": (row.get("mission_category") or "mission").strip(),
                    "ats_type": (row.get("ats_type") or "").strip() or None,
                    "ats_slug": (row.get("ats_slug") or "").strip() or None,
                    "ats_region": (row.get("ats_region") or "global").strip() or "global",
                    "careers_url": url,
                    "last_validated_at": added_at,
                }
            )

        if not payload:
            return 0

        existing_sql = f"""
        SELECT job_board_url
        FROM {self.fqtn("curated_companies")}
        WHERE job_board_url IN UNNEST(@urls)
        """
        urls = [r["job_board_url"] for r in payload]
        existing: set[str] = set()
        chunk_size = 500
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i : i + chunk_size]
            job = self.client.query(
                existing_sql,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("urls", "STRING", chunk)]
                ),
                location=self._settings.BQ_LOCATION,
            )
            existing.update((r["job_board_url"] or "").lower() for r in self._await(job, what='query job'))

        new_rows = [r for r in payload if r["job_board_url"].lower() not in existing]
        if not new_rows:
            logger.info("curated_companies: all %s rows already in BQ", len(payload))
            return 0

        self._append_rows_load("curated_companies", new_rows)
        logger.info("Inserted %s rows into curated_companies (%s skipped as duplicates)", len(new_rows), len(payload) - len(new_rows))
        return len(new_rows)

    def fetch_curated_company_names(self) -> set[str]:
        """Return normalized company names already stored in curated_companies."""
        sql = f"""
        SELECT DISTINCT LOWER(TRIM(company_name)) AS company_name
        FROM {self.fqtn("curated_companies")}
        WHERE company_name IS NOT NULL AND TRIM(company_name) != ""
        """
        job = self.client.query(sql, location=self._settings.BQ_LOCATION)
        return {str(r["company_name"] or "").strip() for r in self._await(job, what='query job') if str(r["company_name"] or "").strip()}

    def delete_curated_companies(self, job_board_urls: list[str]) -> int:
        """Delete curated_companies rows by job_board_url (exact match)."""
        urls = [u.strip() for u in job_board_urls if u and u.strip()]
        if not urls:
            return 0

        deleted = 0
        chunk_size = 200
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i : i + chunk_size]
            sql = f"""
            DELETE FROM {self.fqtn("curated_companies")}
            WHERE job_board_url IN UNNEST(@urls)
            """
            job = self.client.query(
                sql,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[bigquery.ArrayQueryParameter("urls", "STRING", chunk)]
                ),
                location=self._settings.BQ_LOCATION,
            )
            self._await(job, what='query job')
            deleted += int(job.num_dml_affected_rows or 0)
        logger.info("Deleted %s row(s) from curated_companies", deleted)
        return deleted

    def fetch_curated_companies(self, *, limit: int | None = None) -> list[dict[str, str]]:
        """Return curated registry rows for ATS ingest."""
        sql = f"""
        SELECT
          company_name,
          job_board_url,
          discovery_source,
          mission_category,
          ats_type,
          ats_slug,
          ats_region,
          COALESCE(careers_url, job_board_url) AS careers_url,
          last_validated_at
        FROM {self.fqtn("curated_companies")}
        WHERE job_board_url IS NOT NULL AND TRIM(job_board_url) != ""
        ORDER BY added_at DESC
        """
        if limit is not None and limit > 0:
            sql += f"\nLIMIT {int(limit)}"
        job = self.client.query(sql, location=self._settings.BQ_LOCATION)
        return [
            {
                "company_name": str(r["company_name"] or ""),
                "job_board_url": str(r["job_board_url"] or ""),
                "discovery_source": str(r.get("discovery_source") or ""),
                "mission_category": str(r.get("mission_category") or "mission"),
                "ats_type": str(r.get("ats_type") or ""),
                "ats_slug": str(r.get("ats_slug") or ""),
                "ats_region": str(r.get("ats_region") or "global"),
                "careers_url": str(r.get("careers_url") or r["job_board_url"] or ""),
                "last_validated_at": _bq_ts_iso(r.get("last_validated_at")),
            }
            for r in self._await(job, what='query job')
        ]

    def touch_curated_last_validated(
        self,
        ats_type: str,
        ats_slug: str,
        *,
        validated_at: str,
    ) -> None:
        """Record that a curated employer board was polled."""
        sql = f"""
        UPDATE {self.fqtn("curated_companies")}
        SET last_validated_at = @validated_at
        WHERE LOWER(ats_type) = LOWER(@ats_type) AND LOWER(ats_slug) = LOWER(@ats_slug)
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("validated_at", "TIMESTAMP", validated_at),
                    bigquery.ScalarQueryParameter("ats_type", "STRING", ats_type),
                    bigquery.ScalarQueryParameter("ats_slug", "STRING", ats_slug),
                ]
            ),
            location=self._settings.BQ_LOCATION,
        )
        self._await(job, what='query job')

    def touch_curated_last_validated_batch(
        self,
        pairs: list[tuple[str, str]],
        *,
        validated_at: str,
    ) -> int:
        """Stamp many curated boards as polled in a single UPDATE.

        BigQuery permits only a couple of concurrent mutating statements per table,
        so one UPDATE per employer from a thread pool mostly ends in
        "could not serialize access" retries. One statement per run avoids that
        entirely (and is far cheaper).
        """
        if not pairs:
            return 0
        keys = [f"{a.lower()}:{s.lower()}" for a, s in pairs if a and s]
        if not keys:
            return 0
        sql = f"""
        UPDATE {self.fqtn("curated_companies")}
        SET last_validated_at = @validated_at
        WHERE CONCAT(LOWER(ats_type), ':', LOWER(ats_slug)) IN UNNEST(@keys)
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("validated_at", "TIMESTAMP", validated_at),
                    bigquery.ArrayQueryParameter("keys", "STRING", keys),
                ]
            ),
            location=self._settings.BQ_LOCATION,
        )
        self._await(job, what='query job')
        return int(job.num_dml_affected_rows or 0)

    def update_curated_company_board(
        self,
        company_name: str,
        *,
        ats_type: str,
        ats_slug: str,
        ats_region: str,
        careers_url: str,
        job_board_url: str,
        validated_at: str,
    ) -> int:
        """Update ATS metadata for an existing curated employer (revalidation)."""
        sql = f"""
        UPDATE {self.fqtn("curated_companies")}
        SET
          ats_type = @ats_type,
          ats_slug = @ats_slug,
          ats_region = @ats_region,
          careers_url = @careers_url,
          job_board_url = @job_board_url,
          last_validated_at = @validated_at
        WHERE LOWER(company_name) = LOWER(@company_name)
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("ats_type", "STRING", ats_type),
                    bigquery.ScalarQueryParameter("ats_slug", "STRING", ats_slug),
                    bigquery.ScalarQueryParameter("ats_region", "STRING", ats_region or "global"),
                    bigquery.ScalarQueryParameter("careers_url", "STRING", careers_url),
                    bigquery.ScalarQueryParameter("job_board_url", "STRING", job_board_url),
                    bigquery.ScalarQueryParameter("validated_at", "TIMESTAMP", validated_at),
                    bigquery.ScalarQueryParameter("company_name", "STRING", company_name),
                ]
            ),
            location=self._settings.BQ_LOCATION,
        )
        self._await(job, what='query job')
        return int(job.num_dml_affected_rows or 0)

    def update_curated_companies_from_matches(
        self,
        rows: list[dict[str, str]],
        *,
        validated_at: str,
    ) -> int:
        """Batch-update curated_companies ATS fields after revalidation."""
        payloads: list[dict[str, str]] = []
        for row in rows:
            name = (row.get("company_name") or "").strip()
            board_url = (row.get("job_board_url") or row.get("careers_url") or "").strip()
            ats_type = (row.get("ats_type") or "").strip()
            ats_slug = (row.get("ats_slug") or "").strip()
            if not name or not board_url or not ats_type or not ats_slug:
                continue
            payloads.append(
                {
                    "company_name": name,
                    "ats_type": ats_type,
                    "ats_slug": ats_slug,
                    "ats_region": (row.get("ats_region") or "global").strip() or "global",
                    "careers_url": board_url,
                    "job_board_url": board_url,
                }
            )
        if not payloads:
            return 0

        updated = 0
        chunk_size = 200
        for i in range(0, len(payloads), chunk_size):
            chunk = payloads[i : i + chunk_size]
            tmp_name = f"_curated_updates_{uuid.uuid4().hex[:12]}"
            tmp_table = self.table_id(tmp_name)
            load_job = self.client.load_table_from_json(
                [
                    {
                        **row,
                        "validated_at": validated_at,
                    }
                    for row in chunk
                ],
                tmp_table,
            )
            self._await(load_job, what='load job')
            sql = f"""
            MERGE {self.fqtn("curated_companies")} T
            USING {self.fqtn(tmp_name)} S
            ON LOWER(T.company_name) = LOWER(S.company_name)
            WHEN MATCHED THEN UPDATE SET
              ats_type = S.ats_type,
              ats_slug = S.ats_slug,
              ats_region = S.ats_region,
              careers_url = S.careers_url,
              job_board_url = S.job_board_url,
              last_validated_at = TIMESTAMP(S.validated_at)
            """
            job = self.client.query(sql, location=self._settings.BQ_LOCATION)
            self._await(job, what='query job')
            updated += int(job.num_dml_affected_rows or 0)
            self.client.delete_table(tmp_table, not_found_ok=True)
        logger.info("Batch-updated %s curated_companies row(s)", updated)
        return updated
