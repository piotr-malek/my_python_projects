"""BigQuery: raw API payloads, normalized job mirror, LLM score audit trail."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

logger = logging.getLogger(__name__)


from core.env import strip_env_path as _strip_env_path


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
    """Write fetches + normalized rows to BQ; read normalized text for LLM scoring."""

    def __init__(self, settings):
        self._settings = settings
        self._client: bigquery.Client | None = None
        self._raw_buffer: list[dict[str, Any]] = []
        self._raw_buffer_max = int(getattr(settings, "BQ_RAW_BATCH_SIZE", 50) or 50)
        self._normalized_buffer: list[tuple[dict[str, Any], str]] = []
        self._llm_score_buffer: list[dict[str, Any]] = []
        self._batch_chunk = int(getattr(settings, "BQ_BATCH_CHUNK_SIZE", 50) or 50)

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
  added_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(added_at)
CLUSTER BY company_name
""",
        ]
        for stmt in ddl_statements:
            job = self.client.query(stmt.strip(), location=self._settings.BQ_LOCATION)
            job.result()
        logger.info("BigQuery tables ensured in %s", self.table_id("jobs_normalized").rsplit(".", 1)[0])

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
        """Flush buffered raw API rows (call at end of ingest)."""
        if not self._raw_buffer:
            return
        batch = self._raw_buffer
        self._raw_buffer = []
        errors = self.client.insert_rows_json(self.table_id("raw_api_payloads"), batch)
        if errors:
            logger.warning("BQ raw insert errors (%s rows): %s", len(batch), errors)

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
        if not self._normalized_buffer:
            return
        batch = self._normalized_buffer
        self._normalized_buffer = []
        ok = 0
        for job, ingested_at in batch:
            try:
                self.merge_normalized_job(job, ingested_at=ingested_at)
                ok += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("BQ normalized merge failed for job %s: %s", job.get("id"), exc)
        logger.info("BQ flushed %s/%s normalized jobs", ok, len(batch))

    def queue_llm_score(self, row: dict[str, Any]) -> None:
        self._llm_score_buffer.append(row)
        if len(self._llm_score_buffer) >= self._batch_chunk:
            self.flush_llm_scores()

    def flush_llm_scores(self) -> None:
        if not self._llm_score_buffer:
            return
        batch = self._llm_score_buffer
        self._llm_score_buffer = []
        errors = self.client.insert_rows_json(self.table_id("llm_score_events"), batch)
        if errors:
            logger.warning("BQ llm_score batch insert errors (%s rows): %s", len(batch), errors)
        else:
            logger.info("BQ flushed %s llm_score_events", len(batch))

    def merge_normalized_job(self, job: dict[str, Any], *, ingested_at: str) -> None:
        """Upsert one row in jobs_normalized (expects SQLite jobs row as dict)."""
        is_remote = bool(job.get("is_remote"))
        prefilter = int(job.get("prefilter_pass") or 0)
        mission = job.get("mission_category")
        sql = f"""
MERGE {self.fqtn("jobs_normalized")} T
USING (
  SELECT
    @source AS source,
    @ats_slug AS ats_slug,
    @source_job_id AS source_job_id,
    @sqlite_job_id AS sqlite_job_id,
    @company_name AS company_name,
    @mission_category AS mission_category,
    @title AS title,
    @url AS url,
    @location_text AS location_text,
    @is_remote AS is_remote,
    @salary_text AS salary_text,
    @description_text AS description_text,
    @content_hash AS content_hash,
    @prefilter_pass AS prefilter_pass,
    TIMESTAMP(@first_seen_at) AS first_seen_at,
    TIMESTAMP(@last_seen_at) AS last_seen_at,
    TIMESTAMP(@last_changed_at) AS last_changed_at,
    TIMESTAMP(@ingested_at) AS ingested_at
) S
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
    first_seen_at = S.first_seen_at,
    last_seen_at = S.last_seen_at,
    last_changed_at = S.last_changed_at,
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (
    source, ats_slug, source_job_id, sqlite_job_id, company_name, mission_category,
    title, url, location_text, is_remote, salary_text, description_text, content_hash,
    prefilter_pass, first_seen_at, last_seen_at, last_changed_at, ingested_at
  )
  VALUES (
    S.source, S.ats_slug, S.source_job_id, S.sqlite_job_id, S.company_name, S.mission_category,
    S.title, S.url, S.location_text, S.is_remote, S.salary_text, S.description_text, S.content_hash,
    S.prefilter_pass, S.first_seen_at, S.last_seen_at, S.last_changed_at, S.ingested_at
  )
"""
        params = [
            ScalarQueryParameter("source", "STRING", job["source"]),
            ScalarQueryParameter("ats_slug", "STRING", job["ats_slug"]),
            ScalarQueryParameter("source_job_id", "STRING", job["source_job_id"]),
            ScalarQueryParameter("sqlite_job_id", "INT64", int(job["id"])),
            ScalarQueryParameter("company_name", "STRING", job["company_name"]),
            ScalarQueryParameter("mission_category", "STRING", mission),
            ScalarQueryParameter("title", "STRING", job["title"]),
            ScalarQueryParameter("url", "STRING", job["url"]),
            ScalarQueryParameter("location_text", "STRING", job.get("location_text")),
            ScalarQueryParameter("is_remote", "BOOL", is_remote),
            ScalarQueryParameter("salary_text", "STRING", job.get("salary_text")),
            ScalarQueryParameter("description_text", "STRING", job["description_text"]),
            ScalarQueryParameter("content_hash", "STRING", job["content_hash"]),
            ScalarQueryParameter("prefilter_pass", "INT64", prefilter),
            ScalarQueryParameter("first_seen_at", "STRING", job["first_seen_at"]),
            ScalarQueryParameter("last_seen_at", "STRING", job["last_seen_at"]),
            ScalarQueryParameter("last_changed_at", "STRING", job["last_changed_at"]),
            ScalarQueryParameter("ingested_at", "STRING", ingested_at),
        ]
        job_q = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=params),
            location=self._settings.BQ_LOCATION,
        )
        job_q.result()

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
        rows = list(job.result())
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
        errors = self.client.insert_rows_json(self.table_id("llm_score_events"), [row])
        if errors:
            logger.warning("BQ llm_score insert errors: %s", errors)

    def fetch_sent_job_keys(self) -> set[tuple[str, str, str]]:
        """Job identities already included in a prior digest email."""
        query = f"""
            SELECT DISTINCT source, ats_slug, source_job_id
            FROM `{self.table_id("selected_digest_jobs")}`
            """
        try:
            job = self.client.query(query, location=self._settings.BQ_LOCATION)
            rows = job.result()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load sent digest keys from BQ: %s", exc)
            return set()
        return {
            (str(r["source"]).lower(), str(r["ats_slug"]).lower(), str(r["source_job_id"]))
            for r in rows
        }

    def append_selected_jobs(self, *, digest_date: str, selected_at: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
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
        errors = self.client.insert_rows_json(self.table_id("selected_digest_jobs"), payload)
        if errors:
            logger.warning("BQ selected_digest_jobs insert errors: %s", errors)

    def insert_curated_companies(self, rows: list[dict[str, str]], *, added_at: str) -> int:
        """Insert new purpose-driven employers; skip URLs already present (keeps first added_at)."""
        if not rows:
            return 0

        seen_urls: set[str] = set()
        payload: list[dict[str, str]] = []
        for row in rows:
            url = (row.get("job_board_url") or "").strip()
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
            existing.update((r["job_board_url"] or "").lower() for r in job.result())

        new_rows = [r for r in payload if r["job_board_url"].lower() not in existing]
        if not new_rows:
            logger.info("curated_companies: all %s rows already in BQ", len(payload))
            return 0

        errors = self.client.insert_rows_json(self.table_id("curated_companies"), new_rows)
        if errors:
            logger.warning("BQ curated_companies insert errors: %s", errors)
            return 0
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
        return {str(r["company_name"] or "").strip() for r in job.result() if str(r["company_name"] or "").strip()}

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
            job.result()
            deleted += int(job.num_dml_affected_rows or 0)
        logger.info("Deleted %s row(s) from curated_companies", deleted)
        return deleted

    def fetch_curated_companies(self, *, limit: int | None = None) -> list[dict[str, str]]:
        """Return company_name + job_board_url rows for ATS ingest."""
        sql = f"""
        SELECT company_name, job_board_url
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
            }
            for r in job.result()
        ]
