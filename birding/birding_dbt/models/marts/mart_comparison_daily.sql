-- birding/birding_dbt/models/marts/mart_comparison_daily.sql
{{ config(materialized='table') }}

WITH arr AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    weather_date,
    chunk_id,
    day_idx,
    daily_mean_rolling,
    rainfall_intensity_rolling,
    sunshine_duration_minutes_rolling,
    aligned_wind_rolling,
    SAFE_DIVIDE(num_days_aligned_wind_5_plus_rolling, 3) AS p_aligned_wind_5_plus_arrival,
    SAFE_DIVIDE(num_days_rain_10mm_plus_rolling, 3) AS p_rain_10mm_plus_arrival,
    SAFE_DIVIDE(num_days_moderate_rain_rolling, 3) AS p_moderate_rain_arrival
  FROM {{ ref('core_arrival_metrics_base_rolling') }}
),
hist AS (
  SELECT *
  FROM {{ ref('mart_historical_10yr_avgs') }}
),
base AS (
  SELECT
    a.bird,
    a.arrival_year,
    a.location_name,
    a.weather_date,
    a.chunk_id,
    a.day_idx,

    a.daily_mean_rolling AS daily_mean_rolling_arrival,
    h.daily_mean_rolling_10yr_avg AS daily_mean_rolling_hist,
    {{ ratio('a.daily_mean_rolling', 'h.daily_mean_rolling_10yr_avg') }} AS daily_mean_rolling_ratio,
    {{ delta('a.daily_mean_rolling', 'h.daily_mean_rolling_10yr_avg') }} AS daily_mean_rolling_delta,
    {{ compute_z_value('a.daily_mean_rolling', 'h.daily_mean_rolling_10yr_avg', 'h.daily_mean_rolling_10yr_sd') }} AS daily_mean_rolling_z,

    a.rainfall_intensity_rolling AS rainfall_intensity_rolling_arrival,
    h.rainfall_intensity_rolling_10yr_avg AS rainfall_intensity_rolling_hist,
    {{ ratio('a.rainfall_intensity_rolling', 'h.rainfall_intensity_rolling_10yr_avg') }} AS rainfall_intensity_rolling_ratio,
    {{ delta('a.rainfall_intensity_rolling', 'h.rainfall_intensity_rolling_10yr_avg') }} AS rainfall_intensity_rolling_delta,
    {{ compute_z_value('a.rainfall_intensity_rolling', 'h.rainfall_intensity_rolling_10yr_avg', 'h.rainfall_intensity_rolling_10yr_sd') }} AS rainfall_intensity_rolling_z,

    a.sunshine_duration_minutes_rolling AS sunshine_duration_minutes_rolling_arrival,
    h.sunshine_duration_minutes_rolling_10yr_avg AS sunshine_duration_minutes_rolling_hist,
    {{ ratio('a.sunshine_duration_minutes_rolling', 'h.sunshine_duration_minutes_rolling_10yr_avg') }} AS sunshine_duration_minutes_rolling_ratio,
    {{ delta('a.sunshine_duration_minutes_rolling', 'h.sunshine_duration_minutes_rolling_10yr_avg') }} AS sunshine_duration_minutes_rolling_delta,
    {{ compute_z_value('a.sunshine_duration_minutes_rolling', 'h.sunshine_duration_minutes_rolling_10yr_avg', 'h.sunshine_duration_minutes_rolling_10yr_sd') }} AS sunshine_duration_minutes_rolling_z,

    a.aligned_wind_rolling AS aligned_wind_rolling_arrival,
    h.aligned_wind_rolling_10yr_avg AS aligned_wind_rolling_hist,
    {{ ratio('a.aligned_wind_rolling', 'h.aligned_wind_rolling_10yr_avg') }} AS aligned_wind_rolling_ratio,
    {{ delta('a.aligned_wind_rolling', 'h.aligned_wind_rolling_10yr_avg') }} AS aligned_wind_rolling_delta,
    {{ compute_z_value('a.aligned_wind_rolling', 'h.aligned_wind_rolling_10yr_avg', 'h.aligned_wind_rolling_10yr_sd') }} AS aligned_wind_rolling_z,

    a.p_aligned_wind_5_plus_arrival AS p_aligned_wind_5_plus_arrival,
    h.p_aligned_wind_5_plus_10yr_avg AS p_aligned_wind_5_plus_hist,
    {{ ratio('a.p_aligned_wind_5_plus_arrival', 'h.p_aligned_wind_5_plus_10yr_avg') }} AS p_aligned_wind_5_plus_ratio,
    {{ delta('a.p_aligned_wind_5_plus_arrival', 'h.p_aligned_wind_5_plus_10yr_avg') }} AS p_aligned_wind_5_plus_delta,
    {{ compute_z_value('a.p_aligned_wind_5_plus_arrival', 'h.p_aligned_wind_5_plus_10yr_avg', 'h.p_aligned_wind_5_plus_10yr_sd') }} AS p_aligned_wind_5_plus_z,

    a.p_rain_10mm_plus_arrival AS p_rain_10mm_plus_arrival,
    h.p_rain_10mm_plus_10yr_avg AS p_rain_10mm_plus_hist,
    {{ ratio('a.p_rain_10mm_plus_arrival', 'h.p_rain_10mm_plus_10yr_avg') }} AS p_rain_10mm_plus_ratio,
    {{ delta('a.p_rain_10mm_plus_arrival', 'h.p_rain_10mm_plus_10yr_avg') }} AS p_rain_10mm_plus_delta,
    {{ compute_z_value('a.p_rain_10mm_plus_arrival', 'h.p_rain_10mm_plus_10yr_avg', 'h.p_rain_10mm_plus_10yr_sd') }} AS p_rain_10mm_plus_z,

    a.p_moderate_rain_arrival AS p_moderate_rain_arrival,
    h.p_moderate_rain_10yr_avg AS p_moderate_rain_hist,
    {{ ratio('a.p_moderate_rain_arrival', 'h.p_moderate_rain_10yr_avg') }} AS p_moderate_rain_ratio,
    {{ delta('a.p_moderate_rain_arrival', 'h.p_moderate_rain_10yr_avg') }} AS p_moderate_rain_delta,
    {{ compute_z_value('a.p_moderate_rain_arrival', 'h.p_moderate_rain_10yr_avg', 'h.p_moderate_rain_10yr_sd') }} AS p_moderate_rain_z
  FROM arr a
  JOIN hist h
    USING (bird, arrival_year, location_name, chunk_id, day_idx)
)

SELECT
  base.*,
  {{ flag_from_z('base.daily_mean_rolling_z', 'h.daily_mean_rolling_10yr_sd') }} AS daily_mean_rolling_flag,
  {{ flag_from_z('base.rainfall_intensity_rolling_z', 'h.rainfall_intensity_rolling_10yr_sd') }} AS rainfall_intensity_rolling_flag,
  {{ flag_from_z('base.sunshine_duration_minutes_rolling_z', 'h.sunshine_duration_minutes_rolling_10yr_sd') }} AS sunshine_duration_minutes_rolling_flag,
  {{ flag_from_z('base.aligned_wind_rolling_z', 'h.aligned_wind_rolling_10yr_sd') }} AS aligned_wind_rolling_flag,
  {{ flag_from_z('base.p_aligned_wind_5_plus_z', 'h.p_aligned_wind_5_plus_10yr_sd') }} AS p_aligned_wind_5_plus_flag,
  {{ flag_from_z('base.p_rain_10mm_plus_z', 'h.p_rain_10mm_plus_10yr_sd') }} AS p_rain_10mm_plus_flag,
  {{ flag_from_z('base.p_moderate_rain_z', 'h.p_moderate_rain_10yr_sd') }} AS p_moderate_rain_flag
FROM base
JOIN {{ ref('mart_historical_10yr_avgs') }} h
  USING (bird, arrival_year, location_name, chunk_id, day_idx)