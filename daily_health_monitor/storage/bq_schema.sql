CREATE SCHEMA IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}`
OPTIONS (location = 'EU');

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_heart_rate` (
  date DATE NOT NULL,
  rhr FLOAT64,
  avg_hr FLOAT64,
  min_hr FLOAT64,
  max_hr FLOAT64,
  samples_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_stress` (
  date DATE NOT NULL,
  avg_stress FLOAT64,
  rest_pct FLOAT64,
  high_pct FLOAT64,
  samples_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_sleep` (
  date DATE NOT NULL,
  sleep_start TIMESTAMP,
  sleep_end TIMESTAMP,
  sleep_minutes FLOAT64,
  deep_minutes FLOAT64,
  light_minutes FLOAT64,
  rem_minutes FLOAT64,
  awake_minutes FLOAT64,
  sleep_score FLOAT64,
  sleep_stress FLOAT64,
  raw_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_body_battery` (
  date DATE NOT NULL,
  bb_high FLOAT64,
  bb_low FLOAT64,
  charged FLOAT64,
  drained FLOAT64,
  timeline_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_activity_daily` (
  date DATE NOT NULL,
  steps INT64,
  calories FLOAT64,
  active_calories FLOAT64,
  intensity_minutes FLOAT64,
  sedentary_minutes FLOAT64,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_respiration` (
  date DATE NOT NULL,
  waking_rr FLOAT64,
  sleep_rr FLOAT64,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_fitness` (
  date DATE NOT NULL,
  vo2max FLOAT64,
  readiness_score FLOAT64,
  morning_readiness FLOAT64,
  hrv_status STRING,
  training_status STRING,
  garmin_only_load BOOL,
  load_balance_json STRING,
  raw_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.activities` (
  strava_activity_id INT64 NOT NULL,
  name STRING,
  sport_type STRING,
  start_date TIMESTAMP,
  start_date_local TIMESTAMP,
  moving_time INT64,
  elapsed_time INT64,
  distance FLOAT64,
  elevation_gain FLOAT64,
  avg_hr FLOAT64,
  max_hr FLOAT64,
  avg_speed FLOAT64,
  max_speed FLOAT64,
  avg_cadence FLOAT64,
  avg_watts FLOAT64,
  weighted_avg_watts FLOAT64,
  kilojoules FLOAT64,
  suffer_score FLOAT64,
  calories FLOAT64,
  trainer BOOL,
  device_name STRING,
  external_id STRING,
  garmin_activity_id STRING,
  gear_id STRING,
  elev_high FLOAT64,
  elev_low FLOAT64,
  details_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.activity_streams` (
  strava_activity_id INT64 NOT NULL,
  streams_json STRING,
  fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.garmin_activity_enrichment` (
  strava_activity_id INT64 NOT NULL,
  aerobic_te FLOAT64,
  anaerobic_te FLOAT64,
  activity_load FLOAT64,
  hr_zones_json STRING,
  enriched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sync_state` (
  state_key STRING NOT NULL,
  state_value STRING,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.derived_metrics` (
  date DATE NOT NULL,
  metric_name STRING NOT NULL,
  metric_window STRING,
  value FLOAT64,
  source STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.activity_derived_metrics` (
  strava_activity_id INT64 NOT NULL,
  hr_drift FLOAT64,
  aerobic_decoupling FLOAT64,
  efficiency_factor FLOAT64,
  np_proxy FLOAT64,
  tss_proxy FLOAT64,
  tss_source STRING,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.activity_derived_metrics`
ADD COLUMN IF NOT EXISTS tss_source STRING;

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.daily_aggregates` (
  date DATE NOT NULL,
  recovery_score FLOAT64,
  burnout_risk_score FLOAT64,
  illness_probability_score FLOAT64,
  training_readiness_score FLOAT64,
  cognitive_readiness_score FLOAT64,
  atl FLOAT64,
  ctl FLOAT64,
  load_ratio FLOAT64,
  garmin_fitness_partial BOOL,
  scores_json STRING,
  flags_json STRING,
  pattern_alerts_json STRING,
  data_quality_json STRING,
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.llm_insights` (
  date DATE NOT NULL,
  prompt_hash STRING,
  response_text STRING,
  model STRING,
  sent_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.digest_themes` (
  date DATE NOT NULL,
  themes ARRAY<STRING>,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.raw_hrv` (
  date DATE NOT NULL,
  last_night_avg_ms FLOAT64,
  weekly_avg_ms FLOAT64,
  status STRING,
  baseline_low_ms FLOAT64,
  baseline_high_ms FLOAT64,
  nocturnal_proxy FLOAT64,
  raw_json STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.insight_cache` (
  computed_on DATE NOT NULL,
  findings_json STRING,
  window_days INT64,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.insight_history` (
  date DATE NOT NULL,
  finding_ids ARRAY<STRING>,
  categories ARRAY<STRING>,
  headline STRING,
  lead_finding_category STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.pipeline_runs` (
  run_id STRING NOT NULL,
  run_date DATE,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status STRING,
  error STRING
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.wellness_daily_complete` (
  date DATE NOT NULL,
  rhr FLOAT64,
  sleep_minutes FLOAT64,
  deep_minutes FLOAT64,
  rem_minutes FLOAT64,
  light_minutes FLOAT64,
  awake_minutes FLOAT64,
  sleep_start_local TIMESTAMP,
  sleep_end_local TIMESTAMP,
  waking_rr FLOAT64,
  sleep_rr FLOAT64,
  avg_stress_fullday FLOAT64,
  high_stress_pct_fullday FLOAT64,
  rest_pct_fullday FLOAT64,
  stress_band_morning FLOAT64,
  stress_band_work FLOAT64,
  stress_band_evening FLOAT64,
  stress_peak_window STRING,
  stress_peak_value FLOAT64,
  stress_settled_after STRING,
  high_stress_minutes FLOAT64,
  steps_fullday INT64,
  intensity_minutes_prenoon INT64,
  bb_high FLOAT64,
  bb_low FLOAT64,
  bb_recharge_efficiency FLOAT64,
  hrv_proxy_nocturnal FLOAT64,
  sleep_quality_index FLOAT64,
  is_hard_day BOOL,
  tss FLOAT64,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.insight_diagnostics` (
  computed_on DATE NOT NULL,
  detector_id STRING NOT NULL,
  n_pairs INT64,
  best_lag STRING,
  best_r FLOAT64,
  passed BOOL,
  reason STRING,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
