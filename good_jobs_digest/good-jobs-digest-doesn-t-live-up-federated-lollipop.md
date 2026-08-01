# good_jobs_digest — Diagnosis & Overhaul Plan

Project: `/Users/pedro/Documents/Code/projects/github/my_python_projects/good_jobs_digest`

## Context

Digests deliver 1–5 jobs per run (down from ~34 in May) and what arrives is mostly too senior,
US-based, US-timezone, or office-bound. Target: **remote-friendly, EU-timezone, EU-citizen-friendly,
mid-level (2–3 yrs) data/AI engineering roles at mission-driven orgs** (per `profile/preferences.yaml`).
**Not looking for ML/machine-learning roles.**

User decisions:
- **Mission orgs only** in the digest; broad intake channels OK but mission-gated at employer level; registry rebuilt EU-first.
- Sources: **public APIs + Indeed via python-jobspy** (no LinkedIn).
- Filtering philosophy: **deterministic filters on position title only; the LLM owns the fit check** (location, remote, timezone, EU-citizen, office requirements). No region/description regexes.
- Scoring: **Gemini Flash only** (no Ollama), must stay in free tier — zero cost.
- Scheduling: **GitHub Actions daily**.

## Diagnosis (verified in code + data/jobs.db, 2026-05-29 → 2026-07-12)

Funnel: 3,108 fetched → 350 pass title prefilter (11%) → 240 scored → 148 ever digested.
Of digested: **60% Senior/Staff/Lead/Principal titles, 37% US-located**. Three independent causes:

**1. Filtering bugs drop wanted jobs and leak unwanted ones**
- `rank/location_constraints.py:11`: US regex `\bu\.?\s*s\.?\b` matches the pronoun **"us"** ("join us", "about us") in descriptions → phantom "United States" region on nearly every job → `apply_location_guard` forces `remote_ok=False` → plain-"Remote" EU-friendly jobs dropped by the remote-only digest gate. It also poisons the LLM prompt ("stated hire region does NOT match → remote_ok should be FALSE"). Resolution per user direction: **remove this whole deterministic location-guard layer**, not fix its regexes.
- `rank/prefilter.py:37-39`: excludes are plain substrings checked before includes — `software engineer` kills "Data Software Engineer", `intern` kills "International …", `product engineer` kills "Data Product Engineer".
- No seniority filter anywhere deterministic; LLM `candidate_fit` weighted only 0.25 in `combined = 0.4*role + 0.35*mission + 0.25*fit` (`config.py:122`), so senior/US roles clear `MIN_COMBINED_SCORE=35`. The local LLM (qwen3:14b) misjudges timezone/region/seniority nuance.

**2. Sources structurally mismatched (US-heavy mission directories that rarely hire data engineers)**
- 9 boards wired: Climatebase, 80000 Hours, Escape the City, Tech Jobs for Good, ReliefWeb (disabled — `RELIEFWEB_APPNAME` unset), Idealist, Impactpool, Animal Advocacy Careers (403), Work on Climate (404). The last four use a generic HTML-fallback scraper yielding ~0 usable rows. Yield: escapethecity 1,959 fetched → 0 digested; 80000hours 749 → 0. Greenhouse curated ATS supplies 78% of all digested jobs.
- 213 curated employers (`registry/curated_companies.csv`): 125 from the **US B Corp directory** (Typesense collection `companies-production-en-us`), 19 Gates Foundation grantees, ~10 Coefficient Giving (ex-OpenPhil), 4 EA Funds, 55 opaque `mission_filter` rows (produced outside this repo). Only greenhouse/lever/smartrecruiters rows exist, though clients for 9 more ATS types (ashby, workable, recruitee, personio, bamboohr, breezy, jazzhr, teamtailor, workday) are implemented and wired.
- Only ⅓ of curated employers polled per run (`CURATED_POLL_ROTATION_DIVISOR=3`).

**3. Operations**
- No scheduler installed — manual runs with multi-day gaps; each job emailed at most once, so digests shrink as backlog drains.
- Config traps: `.env` `DIGEST_REMOTE_ONLY=false` silently overridden by `preferences.yaml digest.remote_only: true` (`profile/preferences.py:240`); `DIGEST_TOP_N` is dead code; hardcoded Algolia keys (80k/ETC) fail silently; deal-breaker "No visa sponsorship needed" phrasing confuses the LLM (an EU job saying "EU citizens only, no sponsorship" is ideal for the user).

---

## Plan

### Phase 1 — Fix the funnel: title-only deterministic filters, LLM owns fit

1. **Remove the deterministic location-guard layer** (`rank/location_constraints.py` usage in `rank/scorer.py`): no more region-regex extraction, no `apply_location_guard` post-LLM override, no location-constraints hint in the prompt. The LLM receives raw title + location line + description + preferences and decides fit. (This removes the phantom-US bug by deletion.)
2. **Prefilter redesign — title only** (`rank/prefilter.py`, config in `config.py`):
   - Word-boundary matching for excludes (`\bintern\b` no longer hits "International"; drop redundant `internship`).
   - Two exclude tiers: *hard* (intern, recruiter, sales, account executive, customer success, marketing… — always drop) and *discipline* (software engineer/developer, frontend, backend, devops, sre… — drop **only if** the title lacks a role-family qualifier {data, analytics, ai, artificial intelligence, etl, elt, pipeline, integration, platform}). So "Data Software Engineer" / "AI Software Engineer" pass; bare "Software Engineer" fails. **No ML/machine-learning qualifiers or keywords — ML roles are not targeted** (titles matching only ML terms keep failing the include gate; keyword lists stay as the user set them).
   - Includes: existing phrase list OR (role-family qualifier + engineer/engineering token). Note: bare `pipeline` can match literal (oil/gas) "Pipeline Engineer" on climate boards — acceptable, LLM catches.
   - **Seniority gate from title** (still position-name filtering): word-boundary drop for senior|staff|principal|lead|head|director|vp|chief|manager + too-junior (intern/graduate/working student); configurable `SENIORITY_EXCLUDE_KEYWORDS` env, mirrors `preferences.yaml too_senior`. Store a `prefilter_reason` on rejected rows for debugging.
3. **LLM fit check made explicit** (`rank/scorer.py` + `rank/prompts/score_job.txt`): extend the structured output schema with explicit booleans the digest can gate on — `remote_ok`, `eu_hire_ok` (EU-citizen-friendly / hireable from EU), `timezone_ok` (European-hours compatible), `seniority_ok`, each with a one-line reason. Prompt spells out the user's hard criteria (remote required, hybrid 0 days, CET overlap, EU citizen, mid-level 2–3 yrs). Digest gate: all four booleans true + `candidate_fit` floor (env, default ≥40) + `MIN_COMBINED_SCORE`.
4. **Config cleanup**: make env `DIGEST_REMOTE_ONLY` win over preferences.yaml (or log a loud warning); remove dead `DIGEST_TOP_N` or implement it; `CURATED_POLL_ROTATION_DIVISOR=1` (poll all employers every run); fix `preferences.yaml` deal-breaker phrasing → "Requires US work authorization / US-only hire region".
5. **Additive SQLite migration helper** (guarded `ALTER TABLE … ADD COLUMN` via `PRAGMA table_info`) in `storage/repository.py` — needed for new columns (`prefilter_reason`, new LLM booleans, employer mission cache, run stats); repo has none today (past "no such column: canonical_job_id" crash).

### Phase 2 — Gemini Flash scoring (only engine; zero cost)

- Replace the Ollama client in `rank/scorer.py` (and `discovery/mission_filter.py::EmployerMissionFilter`) with **google-genai SDK + JSON `response_schema` structured output**. Env: `GEMINI_API_KEY`, `GEMINI_MODEL` (default `gemini-2.5-flash-lite`, the Flash-family model with the highest free-tier daily quota; `gemini-2.5-flash` selectable). Remove `OLLAMA_*` config. Local and CI runs are identical.
- **Zero-cost guarantees**: AI Studio key on a project **without billing enabled** — over-quota requests return 429, charging is impossible. Belt-and-braces in code: RPM throttle + `GEMINI_DAILY_REQUEST_BUDGET` (default ~800, below the free-tier RPD); when the budget is hit, remaining jobs stay unscored and are picked up next run. Retries with backoff on 429.
- **Volume & cost estimate** (to validate free-tier fit; exact quotas re-checked at impl):
  - Scoring calls = jobs passing the title prefilter, scored once each. Current sources produce ~8/day; with new sources and the fixed prefilter, plan for **30–80 scoring calls/day**. Employer mission checks are cached per employer: one-time backfill ~300–500 calls during registry rebuild, then **~5–15/day** for newly discovered employers.
  - Steady state: **~50–100 requests/day ≈ 1,500–3,000/month**. One-time spikes (registry rebuild, first-run backlog of ~350 stored jobs) spread across 1–2 days.
  - Free tier (as of knowledge cutoff): gemini-2.5-flash-lite ≈ **1,000 requests/day** (15 RPM), gemini-2.5-flash ≈ 250 RPD. Steady state uses **~5–10% of flash-lite's daily quota** → comfortable fit, **$0/month**.
  - Paid-price reference (will not apply — billing disabled): ~2k input + ~300 output tokens/job × 3,000 req/month ≈ 6M in / 0.9M out ≈ **$0.60 + $0.36 ≈ under $1/month** at flash-lite rates. Confirms there is no cost cliff even if quotas change.
  - Caveat: free-tier prompts may be used by Google for training — acceptable (job postings are public).

### Phase 3 — Mission-first EU sources

**A. Rebuild the curated registry EU-first** (highest-value work; reuses `discovery/` infra: ATS probing for 12 ATS types, `EmployerMissionFilter`, `build_registry_v2`, `tools/export_curated_registry.py`):
   - B Corp collector (`discovery/sources.py::collect_from_bcorp`): add **country facet filter** to the Typesense query — pull EU-country B Corps instead of the unfiltered US-heavy set.
   - Probe the 383-row seeds file `discovery/seeds/mission_employers.csv` (ATS columns mostly blank — never probed).
   - Add a researched EU mission-tech seed list (~50–100 orgs: e.g. Wikimedia DE, Ecosia, Electricity Maps, Open Climate Fix, Climate Policy Radar, Global Fishing Watch, Development Seed, OCCRP, CorrelAid, mySociety/Code for All members, TransitionZero, Ember, GiveDirectly, IDinsight…) — expand during implementation.
   - **EU-footprint validation step**: fetch each org's board once and classify the *structured location fields* of its postings (no description regexes); demote/flag orgs with zero EU/remote-eligible postings. Run it over the existing 213 to prune US-only dead weight; re-export CSV + BQ.
   - Keep funder collectors (Coefficient, Gates, EA Funds, GWWC) — they pass through the same EU-footprint check.
**B. New job-level intake feeds, mission-gated** (follow existing board-source pattern: `pipelines/job_boards/sources/*.py` + `normalize/boards.py::BOARD_SOURCES` + normalize handler + config flag):
   - API boards: **Remotive** (category filter; `candidate_required_location` field), **Arbeitnow** (EU-centric; remote + visa_sponsorship flags), **Jobicy** (`geo=emea`), **Himalayas** (timezone fields), **RemoteOK**, **WeWorkRemotely** (category RSS), **Working Nomads**, **HN Who's Hiring** (Algolia: latest `author_whoishiring` thread → comments; parse REMOTE/Europe entries). Structured location/remote/timezone fields are passed through to the LLM prompt as context (not regex-gated). Exact params/fields verified by curling each endpoint at implementation time.
   - **Indeed via python-jobspy**: new source module; role queries × EU countries (`country_indeed`), remote filter, `hours_old` for incremental fetches.
   - **Employer mission gate**: cached per-employer verdicts (new table `employer_mission` or columns on `employer_candidates`) via `EmployerMissionFilter` on Gemini — each employer judged once; only mission-passing employers' jobs reach scoring/digest. Passing employers feed `employer_candidates` → curated registry (flywheel; `main.py discover-candidates` flow exists).
**C. Board repairs**: drop the four zero-yield HTML boards (idealist, impactpool, animaladvocacycareers, workonclimate) unless a clean feed is found (workonclimate may have moved to a Getro-hosted board with JSON); enable ReliefWeb (`RELIEFWEB_APPNAME` is self-declared per API docs — instant, free); keep Climatebase/80k/ETC/TJFG.

### Phase 4 — GitHub Actions automation + observability

- `.github/workflows/digest.yml`: `schedule: cron '30 6 * * *'` (06:30 UTC ≈ 08:30 Warsaw summer) + `workflow_dispatch`; checkout → setup-python + pip cache → **restore `data/jobs.db` via actions/cache** (rolling key + `restore-keys` fallback) → `python main.py run-all` → save cache → upload digest/log artifact on failure.
- Durable sent-dedup independent of cache eviction: set `BQ_WRITE_DIGEST_HISTORY=true` (mechanism already implemented — `sent_keys` pull in `cmd_digest`).
- Secrets: `SMTP_USER`/`SMTP_PASS`, `GEMINI_API_KEY`, GCP service-account JSON (BQ), Webshare proxy list URL. CI env flag to skip the Playwright fallback chain.
- Expect Climatebase/TJFG 403s from datacenter IPs — Webshare proxies mitigate; failures must be *visible*, not silent:
- **Health footer in the digest email**: per-source fetched/passed/error counts for the run + sources returning 0 (surfaces Algolia key rotation, 403s, dead boards) + Gemini budget usage. Persist minimal per-run stats (small `run_source_stats` table written from the ingest summaries both pipelines already log).

## Verification

- **Unit tests** (pytest + existing `.github/workflows/test.yml`): prefilter ("International Data Engineer" passes; "Data Software Engineer" passes; bare "Software Engineer" fails; "Machine Learning Engineer" fails — not targeted; "Senior/Staff/Lead Data Engineer" dropped by seniority gate), Gemini client mocked (schema parsing, budget throttle, 429 handling), digest gating on the new LLM booleans.
- **Backtest** (`tools/backtest_gates.py`): replay the new title prefilter over a **copy** of `data/jobs.db` (3,108 real rows) and report deltas (previously-dropped-now-pass, previously-passed-now-dropped with reasons). One-time **Gemini re-score of the ~350 title-passing stored jobs** (fits free tier in a day) comparing old qwen3 verdicts vs new booleans — quantifies how many US/senior leaks are now excluded and which EU-remote jobs resurface.
- **End-to-end**: local `run-all --dry-run-email` → inspect digest; one `workflow_dispatch` CI run with dry-run email → inspect artifact + health footer; then enable live email.
- User validates BigQuery-side changes himself (no `bq` runs from Claude, per standing preference).

## Critical files

`rank/prefilter.py`, `rank/scorer.py` (+ `rank/prompts/score_job.txt`), `rank/location_constraints.py` (removed from scoring path), `config.py`, `.env`/`.env.example`, `profile/preferences.yaml`/`preferences.py`, `storage/repository.py` (+ migration helper, `jobs_for_digest`, stats table), `storage/schema.sql`, `main.py`, `pipelines/job_boards/sources/` (new API sources + jobspy module), `normalize/boards.py`, `pipelines/curated_ats/ingest.py`, `discovery/sources.py` (B Corp country filter), `discovery/mission_filter.py`, new EU-footprint validator + `tools/backtest_gates.py`, `digest/builder.py` (health footer), new `.github/workflows/digest.yml`, `registry/curated_companies.csv` (regenerated).

## Suggested order & sizing

1. Phase 1 (S/M) — prefilter + seniority gate + location-guard removal + migration helper + backtest: biggest immediate quality jump, verifiable offline.
2. Phase 2 (M) — Gemini swap with budget guardrails; one-time re-score validation.
3. Phase 4 (M) — GH Actions + health footer (start collecting reliable daily data early).
4. Phase 3 (L, iterative) — registry rebuild + new sources + mission-gate flywheel; add sources incrementally, watching per-source yield in the health footer.

Note: external API details (Remotive/Jobicy/Himalayas fields, exact current Gemini free-tier quotas, jobspy version) get verified by curling endpoints/docs at implementation start; all are stable public APIs.
