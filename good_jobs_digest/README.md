# Good Jobs Digest

[License: MIT](LICENSE)

Every morning, a ranked list of job postings lands in your inbox — filtered for roles that fit you, at organizations that seem to care about something beyond the quarterly report.

Purpose-driven work is scattered across niche boards and opaque ATS pages. This pipeline pulls from mission-oriented job boards, polls curated employer feeds, scores postings against *your* profile with a local LLM, and emails the shortlist.

> **Heads up:** Scrapes public boards and calls third-party ATS APIs. Respect terms and rate limits. Don't commit credentials to git.

## What you'll need

| Thing | Why |
|-------|-----|
| Python 3.12+ | Runs the pipeline |
| Ollama + a local model | Scores jobs (`qwen3:14b-q4_K_M` by default) |
| SMTP | Email — Gmail app password works |
| Google Cloud + BigQuery | Optional — de-dupe + private registry (virtually free at personal scale) |

Copy `.env.example` → `.env`, the `profile/*.example.*` files, and `config/service_account.json.example`. Example files are the full config reference.

Curated employers work without BigQuery: the repo ships `registry/curated_companies.csv` (213 mission-filtered orgs). BigQuery `curated_companies` takes priority when populated; otherwise ingest uses the CSV.

## How it fits together

```
Mission job boards          Curated employers (213 orgs, CSV)
        │                              │
        └────────────┬─────────────────┘
                     ▼
        ingest → SQLite (+ BigQuery) → Ollama scoring → email
```

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

**Grow the list yourself** (optional): `python tools/build_registry.py --sources seeds --dry-run` probes ATS slugs; drop `--dry-run` to write to BigQuery. Refresh the shipped CSV from BQ: `python tools/export_curated_registry.py`. See [CONTRIBUTING.md](CONTRIBUTING.md) for discovery flags.

## Picking a local model

Ollama (`OLLAMA_HOST`, `OLLAMA_MODEL`). Pull the model, keep the daemon running.

First run with loose filters (e.g. any "engineer" title) can take **hours** — hundreds of jobs to score. Daily runs are much faster. Smaller models work; quality drops. Tune `OLLAMA_SCORE_WORKERS` / `OLLAMA_SCORE_BATCH_SIZE` if the GPU struggles.

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

Schedule daily with cron or launchd, e.g. `30 7 * * * cd /path/to/good_jobs_digest && .venv/bin/python main.py run-all >> data/cron.log 2>&1`.

## When something breaks

- **No curated jobs** — check `registry/curated_companies.csv`, or run discovery into BigQuery.
- **Board failures** — usually IP blocking; see proxies above.
- **ReliefWeb empty** — set `RELIEFWEB_APPNAME` or disable it.
- **Ollama timeouts** — lower `OLLAMA_SCORE_WORKERS` or use `--max`.

Tests: `pip install -r requirements-dev.txt && pytest`

## Contributing

PRs welcome — new rows in `registry/curated_companies.csv`, job boards, fixes. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).
