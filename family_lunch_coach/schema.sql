CREATE TABLE IF NOT EXISTS recipes (
  id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  source_url TEXT,
  cuisine TEXT,
  nutrition_profile TEXT,
  estimated_minutes INTEGER,
  actual_minutes_avg REAL,
  main_ingredients TEXT,
  batch_friendly INTEGER,
  portions_yield INTEGER,
  times_cooked INTEGER DEFAULT 0,
  last_cooked_date DATE,
  your_rating REAL,
  would_repeat INTEGER,
  your_notes TEXT,
  discovery_reasoning TEXT,
  discovery_source TEXT DEFAULT 'manual',
  added_date DATE,
  base_works_for_kids INTEGER,
  adult_upgrade TEXT,
  adult_upgrade_effort TEXT,
  kid_acceptance_notes TEXT,
  is_shared_base_staple INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS food_log (
  id INTEGER PRIMARY KEY,
  date DATE,
  meal TEXT,
  recipe_id INTEGER,
  free_text TEXT,
  energy_note TEXT,
  logged_at TIMESTAMP,
  FOREIGN KEY(recipe_id) REFERENCES recipes(id)
);

CREATE TABLE IF NOT EXISTS weekly_plans (
  id INTEGER PRIMARY KEY,
  week_start DATE,
  plan_json TEXT,
  generated_at TIMESTAMP,
  followed INTEGER,
  coverage_check TEXT
);

CREATE TABLE IF NOT EXISTS nudge_log (
  id INTEGER PRIMARY KEY,
  nudge_type TEXT,
  occurrence_key TEXT UNIQUE,
  scheduled_at TIMESTAMP,
  sent_at TIMESTAMP,
  was_delayed INTEGER,
  status TEXT,
  context_json TEXT,
  user_responded INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fallback_stack (
  id INTEGER PRIMARY KEY,
  rank INTEGER,
  title TEXT,
  instruction TEXT,
  needs_from_staples TEXT,
  kid_version TEXT
);

CREATE TABLE IF NOT EXISTS profile_cache (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS onboarding_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  status TEXT NOT NULL DEFAULT 'not_started',
  current_step TEXT,
  answers_json TEXT DEFAULT '{}',
  skip_count INTEGER DEFAULT 0,
  last_nudge_at TIMESTAMP,
  completed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS family_members (
  id INTEGER PRIMARY KEY,
  role TEXT,
  age_years REAL,
  age_band TEXT,
  allergies_json TEXT DEFAULT '[]',
  accepts_json TEXT DEFAULT '[]',
  rejects_json TEXT DEFAULT '[]',
  notes TEXT
);

CREATE TABLE IF NOT EXISTS activities_cache (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  fetched_at TIMESTAMP,
  data_json TEXT
);

CREATE TABLE IF NOT EXISTS app_meta (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMP
);
