-- birding/birding_dbt/models/marts/mart_comparison_chunk.sql
{{ config(materialized='table') }}

WITH arr AS (
  SELECT *
  FROM {{ ref('core_arrival_metrics_chunk') }}
),
hist AS (
  SELECT *
  FROM {{ ref('core_historical_metrics_chunk') }}
),
base AS (
  SELECT
    a.chunk_id,

    a.daily_mean_chunk AS daily_mean_chunk_arrival,
    h.daily_mean_chunk_10yr_avg AS daily_mean_chunk_hist,
    {{ ratio('a.daily_mean_chunk', 'h.daily_mean_chunk_10yr_avg') }} AS daily_mean_chunk_ratio,
    {{ delta('a.daily_mean_chunk', 'h.daily_mean_chunk_10yr_avg') }} AS daily_mean_chunk_delta,
    {{ compute_z_value('a.daily_mean_chunk', 'h.daily_mean_chunk_10yr_avg', 'h.daily_mean_chunk_10yr_sd') }} AS daily_mean_chunk_z,

    a.rainfall_intensity_chunk AS rainfall_intensity_chunk_arrival,
    h.rainfall_intensity_chunk_10yr_avg AS rainfall_intensity_chunk_hist,
    {{ ratio('a.rainfall_intensity_chunk', 'h.rainfall_intensity_chunk_10yr_avg') }} AS rainfall_intensity_chunk_ratio,
    {{ delta('a.rainfall_intensity_chunk', 'h.rainfall_intensity_chunk_10yr_avg') }} AS rainfall_intensity_chunk_delta,
    {{ compute_z_value('a.rainfall_intensity_chunk', 'h.rainfall_intensity_chunk_10yr_avg', 'h.rainfall_intensity_chunk_10yr_sd') }} AS rainfall_intensity_chunk_z,

    a.sunshine_duration_minutes_chunk AS sunshine_duration_minutes_chunk_arrival,
    h.sunshine_duration_minutes_chunk_10yr_avg AS sunshine_duration_minutes_chunk_hist,
    {{ ratio('a.sunshine_duration_minutes_chunk', 'h.sunshine_duration_minutes_chunk_10yr_avg') }} AS sunshine_duration_minutes_chunk_ratio,
    {{ delta('a.sunshine_duration_minutes_chunk', 'h.sunshine_duration_minutes_chunk_10yr_avg') }} AS sunshine_duration_minutes_chunk_delta,
    {{ compute_z_value('a.sunshine_duration_minutes_chunk', 'h.sunshine_duration_minutes_chunk_10yr_avg', 'h.sunshine_duration_minutes_chunk_10yr_sd') }} AS sunshine_duration_minutes_chunk_z,

    a.aligned_wind_chunk AS aligned_wind_chunk_arrival,
    h.aligned_wind_chunk_10yr_avg AS aligned_wind_chunk_hist,
    {{ ratio('a.aligned_wind_chunk', 'h.aligned_wind_chunk_10yr_avg') }} AS aligned_wind_chunk_ratio,
    {{ delta('a.aligned_wind_chunk', 'h.aligned_wind_chunk_10yr_avg') }} AS aligned_wind_chunk_delta,
    {{ compute_z_value('a.aligned_wind_chunk', 'h.aligned_wind_chunk_10yr_avg', 'h.aligned_wind_chunk_10yr_sd') }} AS aligned_wind_chunk_z,

    a.num_days_aligned_wind_5_plus_chunk AS num_days_aligned_wind_5_plus_chunk_arrival,
    h.num_days_aligned_wind_5_plus_chunk_10yr_avg AS num_days_aligned_wind_5_plus_chunk_hist,
    {{ ratio('a.num_days_aligned_wind_5_plus_chunk', 'h.num_days_aligned_wind_5_plus_chunk_10yr_avg') }} AS num_days_aligned_wind_5_plus_chunk_ratio,
    {{ delta('a.num_days_aligned_wind_5_plus_chunk', 'h.num_days_aligned_wind_5_plus_chunk_10yr_avg') }} AS num_days_aligned_wind_5_plus_chunk_delta,
    {{ compute_z_value('a.num_days_aligned_wind_5_plus_chunk', 'h.num_days_aligned_wind_5_plus_chunk_10yr_avg', 'h.num_days_aligned_wind_5_plus_chunk_10yr_sd') }} AS num_days_aligned_wind_5_plus_chunk_z,

    a.num_days_rain_10mm_plus_chunk AS num_days_rain_10mm_plus_chunk_arrival,
    h.num_days_rain_10mm_plus_chunk_10yr_avg AS num_days_rain_10mm_plus_chunk_hist,
    {{ ratio('a.num_days_rain_10mm_plus_chunk', 'h.num_days_rain_10mm_plus_chunk_10yr_avg') }} AS num_days_rain_10mm_plus_chunk_ratio,
    {{ delta('a.num_days_rain_10mm_plus_chunk', 'h.num_days_rain_10mm_plus_chunk_10yr_avg') }} AS num_days_rain_10mm_plus_chunk_delta,
    {{ compute_z_value('a.num_days_rain_10mm_plus_chunk', 'h.num_days_rain_10mm_plus_chunk_10yr_avg', 'h.num_days_rain_10mm_plus_chunk_10yr_sd') }} AS num_days_rain_10mm_plus_chunk_z,

    a.num_days_moderate_rain_chunk AS num_days_moderate_rain_chunk_arrival,
    h.num_days_moderate_rain_chunk_10yr_avg AS num_days_moderate_rain_chunk_hist,
    {{ ratio('a.num_days_moderate_rain_chunk', 'h.num_days_moderate_rain_chunk_10yr_avg') }} AS num_days_moderate_rain_chunk_ratio,
    {{ delta('a.num_days_moderate_rain_chunk', 'h.num_days_moderate_rain_chunk_10yr_avg') }} AS num_days_moderate_rain_chunk_delta,
    {{ compute_z_value('a.num_days_moderate_rain_chunk', 'h.num_days_moderate_rain_chunk_10yr_avg', 'h.num_days_moderate_rain_chunk_10yr_sd') }} AS num_days_moderate_rain_chunk_z
  FROM arr a
  JOIN hist h
    USING (chunk_id)
)

SELECT
  base.*,
  {{ flag_from_z('base.daily_mean_chunk_z', 'h.daily_mean_chunk_10yr_sd') }} AS daily_mean_chunk_flag,
  {{ flag_from_z('base.rainfall_intensity_chunk_z', 'h.rainfall_intensity_chunk_10yr_sd') }} AS rainfall_intensity_chunk_flag,
  {{ flag_from_z('base.sunshine_duration_minutes_chunk_z', 'h.sunshine_duration_minutes_chunk_10yr_sd') }} AS sunshine_duration_minutes_chunk_flag,
  {{ flag_from_z('base.aligned_wind_chunk_z', 'h.aligned_wind_chunk_10yr_sd') }} AS aligned_wind_chunk_flag,
  {{ flag_from_z('base.num_days_aligned_wind_5_plus_chunk_z', 'h.num_days_aligned_wind_5_plus_chunk_10yr_sd') }} AS num_days_aligned_wind_5_plus_chunk_flag,
  {{ flag_from_z('base.num_days_rain_10mm_plus_chunk_z', 'h.num_days_rain_10mm_plus_chunk_10yr_sd') }} AS num_days_rain_10mm_plus_chunk_flag,
  {{ flag_from_z('base.num_days_moderate_rain_chunk_z', 'h.num_days_moderate_rain_chunk_10yr_sd') }} AS num_days_moderate_rain_chunk_flag
FROM base
JOIN {{ ref('core_historical_metrics_chunk') }} h
  USING (chunk_id)