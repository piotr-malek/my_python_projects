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
  relevance_score INTEGER,
  mission_score INTEGER,
  fit_score INTEGER,
  remote_ok INTEGER,
  combined_score REAL,
  llm_json TEXT,
  last_scored_at TEXT,
  digest_included_at TEXT,
  UNIQUE (source, ats_slug, source_job_id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_combined ON jobs (combined_score DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_digest ON jobs (prefilter_pass, last_changed_at);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs (ats_type, ats_slug);
CREATE INDEX IF NOT EXISTS idx_jobs_unscored ON jobs (prefilter_pass, last_scored_at);
