# Architecture notes

## Data flow

1. **Ingest** — `garmin/wellness.py` fetches a rolling window of Connect data into raw BigQuery tables. `strava/sync.py` incrementally syncs activities and streams.

2. **Materialize** — `jobs/materialize_history.py` builds `wellness_daily_complete` with full-day stress/steps (morning partials are nulled for "today" but past days must be complete).

3. **Analyze** — `pipeline/analyzer.py` computes training load (ATL/CTL/TSS), composite scores, insight detectors, and assembles the digest payload.

4. **Generate** — `llm/digest.py` sends the payload + prompt to Ollama, validates JSON output, renders markdown.

5. **Deliver** — `mail/mailer.py` sends HTML + plain-text email; failures write to `LOCAL_STATE_DIR/fallback_digests/`.

## Why BigQuery

BigQuery is the system of record: MERGE upserts, 90-day analytics windows, pipeline run history, and LLM insight storage. It requires a GCP project and incurs usage-based cost.

A local SQLite or Parquet backend would lower the barrier for new users but is not implemented yet. Contributions welcome — see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Device capabilities

Garmin hardware varies widely. `garmin/capabilities.py` probes which endpoints return data for your account and caches results. The analytics layer adapts (e.g. nocturnal HR proxy when nightly HRV is unavailable).

## LLM provider

Currently **Ollama only** (`llm/digest.py`). The prompt template lives in `llm/prompts/daily_digest.txt`. A pluggable provider interface would allow OpenAI-compatible APIs without changing analytics.
