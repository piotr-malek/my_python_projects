# Good Jobs Digest

[License: MIT](LICENSE)

Every morning, a ranked list of job postings lands in your inbox — filtered for roles that fit you, at organizations that seem to care about something beyond the quarterly report.

Purpose-driven work is scattered across niche boards and opaque ATS pages. This pipeline pulls from mission-oriented job boards, polls curated employer feeds, scores postings against *your* profile with a local LLM, and emails the shortlist.

> **Heads up:** Scrapes public boards and calls third-party ATS APIs. Respect terms and rate limits. Don't commit credentials to git.

## What you'll need

| Thing | Why |
|-------|-----|
| Python 3.12+ | Runs the pipeline |
| Gemini API key | Scores jobs (`gemini-3.5-flash-lite`). Use an [AI Studio key](https://aistudio.google.com/apikey) on a project with **billing disabled** so it stays free |
| SMTP | Email — Gmail app password works |
| Google Cloud + BigQuery | Optional — curated registry (read) + job mirror (batch load; free tier friendly) |

Copy `.env.example` → `.env`, the `profile/*.example.*` files, and `config/service_account.json.example`. Example files are the full config reference.

Curated employers work without BigQuery: the repo ships `registry/curated_companies.csv` (EU-first, mission-filtered orgs). BigQuery `curated_companies` **takes priority when populated** — if you prune or extend the CSV, sync BigQuery too or the change won't take effect.

## How it fits together

```
Mission job boards + remote/EU aggregators      Curated employers (CSV/BigQuery)
        │                                                    │
        │  employer mission gate (cached per employer)        │
        └──────────────────────┬─────────────────────────────┘
                               ▼
        ingest → title gate → SQLite → Gemini scoring → email
```

Aggregator boards (Remotive, Arbeitnow, Jobicy, Himalayas, Remote OK, We Work
Remotely, Working Nomads, HN "Who is hiring", Indeed) carry plenty of non-mission
employers, so every board-sourced job passes an employer mission gate before it is
scored. Verdicts are cached per employer in `employer_mission`, and employers that
pass feed back into the curated registry.

Two sections in the digest: curated employers first, then board listings.

## Getting started

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
cp profile/preferences.example.yaml profile/preferences.yaml
cp profile/profile.example.md profile/profile.md
cp config/service_account.json.example config/service_account.json

python main.py init-bq          # skip if BQ_ENABLED=false
python main.py run-all
```

Edit `profile/preferences.yaml` before your first real email — role, seniority, stack, location, mission. Preview what the scorer sees: `python -m profile.preferences`.

**Tip:** `python main.py digest --dry-run-email` writes to `data/digests/` instead of sending. Tighten `TARGET_ROLE_KEYWORDS` in `.env` so you're not scoring obvious non-matches.

## Where the jobs come from

### Mission job boards

Climatebase, [80,000 Hours](https://80000hours.org), [Escape the City](https://www.escapethecity.org), [Tech Jobs for Good](https://techjobsforgood.com), [ReliefWeb](https://reliefweb.int) (needs `RELIEFWEB_APPNAME`).

**Blocked by the site?** Climatebase and Tech Jobs for Good often return 403/Cloudflare from datacenter or overused IPs. The client retries via proxies in `config/webshare_proxies.txt` — I use [Webshare](https://www.webshare.io/)'s free 10-datacenter pool (`WEBSHARE_PROXY_LIST_URL` + `python tools/sync_webshare_proxies.py`). Residential proxies would likely work better; 80k Hours, Escape the City, and ReliefWeb are usually fine without.

**Missing a board?** [Open an issue](https://github.com/piotr-malek/my_python_projects/issues) with the URL — or send a PR. Details in [CONTRIBUTING.md](CONTRIBUTING.md).

### Curated employers

**`registry/curated_companies.csv`** — 213 orgs with Greenhouse / Lever / SmartRecruiters URLs, already mission-filtered. Works out of the box; no discovery run needed.

```csv
company_name,job_board_url,mission_category,discovery_source
Watershed,https://boards.greenhouse.io/watershed,climate,seeds
```

**Add an org:** PR a row with name + careers URL. Name only? Open an issue.

BigQuery overrides the CSV when you've populated your own registry. Occasional slug mismatches happen for generic org names ("Health …", "Foundation …") — spot-check odd links.

**Grow the list yourself** (optional):

```bash
# Batch: v1 job-board sources + seeds (+ v2 funders, bcorp, mined pool)
python discovery/build_registry.py --sources 80000hours,escapethecity,climatebase,seeds

# Continuous: mine orgs during ingest, then probe pool → curated_companies
python main.py ingest
python main.py discover-candidates --limit 100
```

Refresh the shipped CSV from BQ: `python tools/export_curated_registry.py`. See [CONTRIBUTING.md](CONTRIBUTING.md) for discovery flags.

## Scoring (Gemini free tier)

Set `GEMINI_API_KEY` from [AI Studio](https://aistudio.google.com/apikey). Create the
key on a Google Cloud project with **billing disabled** — Google cannot charge such a
key, so exceeding the quota returns 429 rather than a bill.

Two further guardrails live in code: `GEMINI_RPM` throttles requests per minute and
`GEMINI_DAILY_REQUEST_BUDGET` caps requests per calendar day (persisted in
`data/gemini_usage.json`, so restarts don't reset it). When the budget runs out the
remaining jobs simply stay unscored and are picked up on the next run.

Jobs are scored in batches (`LLM_SCORE_BATCH_SIZE`) to keep request counts low;
steady-state usage is roughly 50–150 requests/day, well inside the free tier.

Google retires pinned model ids without notice (`gemini-2.5-flash-lite` now 404s for
new keys), so the client falls back through `GEMINI_MODEL_FALLBACKS` and logs a
warning rather than leaving you without a digest.

`MIN_COMBINED_SCORE` = digest cutoff (`0` = all scored). `SCORE_MAX_AGE_DAYS` skips stale postings.

## Email setup

Gmail: 2FA on → [App Password](https://myaccount.google.com/apppasswords) → set `SMTP_USER`, `EMAIL_TO`, `SMTP_PASSWORD`. Any SMTP works.

## Day-to-day

```bash
python main.py ingest | score | digest | run-all
```

| Flag | Use |
|------|-----|
| `--dry-run-email` | Preview only |
| `--curated-only` / `--boards-only` | One ingest path |
| `--limit N` / `--max N` | Cap companies / jobs scored |

### Scheduled runs (GitHub Actions)

`.github/workflows/good-jobs-digest.yml` **at the repository root** (this project is a
subdirectory of a monorepo, and GitHub only reads workflows from the root) runs the
pipeline daily at 06:30 UTC, and can be triggered manually with a `dry_run` toggle.

Repository **secrets**:

| Secret | Purpose |
|---|---|
| `GEMINI_API_KEY` | Scoring |
| `SMTP_USER`, `SMTP_PASSWORD`, `EMAIL_TO` | Sending the digest |
| `GCP_SERVICE_ACCOUNT_JSON` | **Contents** of `config/service_account.json`, not a path — the file is gitignored, so the workflow recreates it and points `GOOGLE_APPLICATION_CREDENTIALS` at it |
| `WEBSHARE_API_KEY` | Optional — proxies for Cloudflare-guarded boards (preferred over the list URL) |
| `WEBSHARE_PROXY_LIST_URL` | Optional fallback if you don't set the API key |
| `PREFERENCES_YAML` | Full contents of `profile/preferences.yaml` (gitignored, so the runner has no other copy) |

Repository **variables**: `BQ_PROJECT_ID`, `BQ_DATASET_ID`, `BQ_LOCATION`.

`data/jobs.db` is carried between runs with `actions/cache` (rolling key plus
`restore-keys`), which is what keeps "already emailed" state. Because caches can be
evicted, the workflow also sets `BQ_WRITE_DIGEST_HISTORY=true` so de-duplication
survives in BigQuery regardless.

Webshare rotates free proxies without notice, so the cached list is re-downloaded
automatically whenever it is older than `WEBSHARE_PROXY_MAX_AGE_HOURS` (default 12) —
no manual `tools/sync_webshare_proxies.py` run needed.

Runner IPs are datacenter IPs, so Cloudflare-guarded boards (Climatebase, Tech Jobs
for Good) may return 403 there. They degrade to zero rather than failing the run, and
the digest's **Run health** footer reports any source that came back empty.

To run locally on a schedule instead: `30 7 * * * cd /path/to/good_jobs_digest && .venv/bin/python main.py run-all >> data/cron.log 2>&1`.

**BigQuery billing:** By default BQ reads `curated_companies` and batch-loads `jobs_normalized` only — no streaming inserts (the usual source of sub-dollar monthly charges). Optional audit tables (`raw_api_payloads`, `llm_score_events`, `selected_digest_jobs`) are off unless you set `BQ_WRITE_RAW_PAYLOADS`, `BQ_WRITE_LLM_SCORES`, or `BQ_WRITE_DIGEST_HISTORY` to `true` in `.env`.

## When something breaks

- **No curated jobs** — check `registry/curated_companies.csv`, or run discovery into BigQuery.
- **Board failures** — usually IP blocking; see proxies above.
- **Nothing gets scored** — check `GEMINI_API_KEY`; if the daily budget is spent the log says so and the run resumes tomorrow.
- **Indeed board missing** — `pip install python-jobspy`, or set `BOARD_INDEED_ENABLED=false`.

Tests: `pip install -r requirements-dev.txt && pytest`

## Contributing

PRs welcome — new rows in `registry/curated_companies.csv`, job boards, fixes. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
