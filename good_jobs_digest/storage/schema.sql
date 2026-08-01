-- job_digest — SQLite v1

PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_name TEXT NOT NULL,
  mission_category TEXT,
  ats_type TEXT NOT NULL,
  ats_slug TEXT NOT NULL,
  source TEXT NOT NULL,
  source_job_id TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT NOT NULL,
  location_text TEXT,
  is_remote INTEGER NOT NULL DEFAULT 0,
  salary_text TEXT,
  posted_at TEXT,
  description_text TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  last_changed_at TEXT NOT NULL,
  prefilter_pass INTEGER NOT NULL DEFAULT 0,
  prefilter_reason TEXT,
  relevance_score INTEGER,
  mission_score INTEGER,
  fit_score INTEGER,
  remote_ok INTEGER,
  eu_hire_ok INTEGER,
  timezone_ok INTEGER,
  seniority_ok INTEGER,
  combined_score REAL,
  llm_json TEXT,
  last_scored_at TEXT,
  digest_included_at TEXT,
  canonical_job_id TEXT,
  registry_ats_type TEXT,
  registry_ats_slug TEXT,
  UNIQUE (source, ats_slug, source_job_id)
);

CREATE TABLE IF NOT EXISTS employer_candidates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_name TEXT NOT NULL,
  discovery_source TEXT NOT NULL,
  mission_category TEXT,
  website TEXT,
  ats_hint_type TEXT,
  ats_hint_slug TEXT,
  seen_at TEXT NOT NULL,
  probed INTEGER NOT NULL DEFAULT 0,
  UNIQUE (company_name, discovery_source)
);

CREATE INDEX IF NOT EXISTS idx_jobs_canonical ON jobs (canonical_job_id);

CREATE INDEX IF NOT EXISTS idx_jobs_combined ON jobs (combined_score DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_digest ON jobs (prefilter_pass, last_changed_at);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs (ats_type, ats_slug);
CREATE INDEX IF NOT EXISTS idx_jobs_unscored ON jobs (prefilter_pass, last_scored_at);

-- Cached EmployerMissionFilter verdicts (board-job mission gate).
CREATE TABLE IF NOT EXISTS employer_mission (
  employer_key TEXT PRIMARY KEY,
  company_name TEXT NOT NULL,
  mission_pass INTEGER NOT NULL,
  mission_score INTEGER,
  reason TEXT,
  mission_type TEXT,
  checked_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_employer_mission_pass ON employer_mission (mission_pass);

-- Per-source outcome of each ingest run; drives the digest health footer so a
-- silently broken board (blocked IP, rotated API key, dead feed) is visible.
CREATE TABLE IF NOT EXISTS run_source_stats (
  run_at TEXT NOT NULL,
  source TEXT NOT NULL,
  fetched INTEGER NOT NULL DEFAULT 0,
  passed INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  PRIMARY KEY (run_at, source)
);

CREATE INDEX IF NOT EXISTS idx_run_source_stats_run ON run_source_stats (run_at DESC);
