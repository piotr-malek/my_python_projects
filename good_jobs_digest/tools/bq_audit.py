#!/usr/bin/env python3
"""Print BigQuery row counts and samples for good_jobs_digest pipeline debugging."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings
from storage.bq_repository import JobBigQuery


def main() -> None:
    if not settings.BQ_ENABLED:
        raise SystemExit("BQ_ENABLED is false")
    bq = JobBigQuery(settings)
    bq.ensure_tables()
    ds = f"{settings.BQ_PROJECT_ID}.{settings.BQ_DATASET_ID}"
    client = bq.client

    def q(sql: str) -> list:
        job = client.query(sql, location=settings.BQ_LOCATION)
        return list(job.result())

    print(f"Dataset: {ds} (location={settings.BQ_LOCATION})\n")

    for table in (
        "raw_api_payloads",
        "jobs_normalized",
        "llm_score_events",
        "selected_digest_jobs",
        "curated_companies",
    ):
        rows = q(f"SELECT COUNT(*) AS n FROM `{ds}.{table}`")
        print(f"{table}: {rows[0]['n']}")

    print("\n--- raw_api_payloads by ats_type / company (top 25) ---")
    for r in q(
        f"""
        SELECT ats_type, company_name, COUNT(*) AS n
        FROM `{ds}.raw_api_payloads`
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 25
        """
    ):
        print(f"  {r['ats_type']:16} {r['company_name'][:40]:40} {r['n']}")

    print("\n--- jobs_normalized: prefilter_pass ---")
    for r in q(
        f"""
        SELECT prefilter_pass, COUNT(*) AS n
        FROM `{ds}.jobs_normalized`
        GROUP BY 1
        ORDER BY 1
        """
    ):
        print(f"  prefilter_pass={r['prefilter_pass']}: {r['n']}")

    print("\n--- latest llm_score_events (5) ---")
    for r in q(
        f"""
        SELECT scored_at, company_name, title, combined_score, ollama_model
        FROM `{ds}.llm_score_events` e
        LEFT JOIN `{ds}.jobs_normalized` j
          ON e.source = j.source AND e.ats_slug = j.ats_slug AND e.source_job_id = j.source_job_id
        ORDER BY scored_at DESC
        LIMIT 5
        """
    ):
        print(
            f"  {r['scored_at']} score={r['combined_score']} model={r['ollama_model']} "
            f"{r.get('company_name') or '?'} — {r.get('title') or '?'}"
        )

    print("\n--- sample normalized row (prefilter_pass=1) ---")
    sample = q(
        f"""
        SELECT company_name, title, LENGTH(description_text) AS desc_len, prefilter_pass
        FROM `{ds}.jobs_normalized`
        WHERE prefilter_pass = 1
        ORDER BY ingested_at DESC
        LIMIT 3
        """
    )
    for r in sample:
        print(f"  {r['company_name']}: {r['title']} (desc_len={r['desc_len']})")


if __name__ == "__main__":
    main()
