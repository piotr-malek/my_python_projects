-- birding/birding_dbt/models/marts/mart_comparison_period.sql
{{ config(materialized='table') }}

WITH arr AS (
  SELECT *
  FROM {{ ref('core_arrival_metrics_period') }}
),
hist AS (
  SELECT *
  FROM {{ ref('core_historical_metrics_period') }}
),
base AS (
  SELECT
    a.bird,
    a.arrival_year,
    a.location_name,
    a.period_start,
    a.period_end,

    -- daily_mean
    a.daily_mean_period AS daily_mean_period_arrival,
    h.daily_mean_period_10yr_avg AS daily_mean_period_hist,
    {{ ratio('a.daily_mean_period', 'h.daily_mean_period_10yr_avg') }} AS daily_mean_period_ratio,
    {{ delta('a.daily_mean_period', 'h.daily_mean_period_10yr_avg') }} AS daily_mean_period_delta,
    {{ compute_z_value('a.daily_mean_period', 'h.daily_mean_period_10yr_avg', 'h.daily_mean_period_10yr_sd') }} AS daily_mean_period_z,
    {{ flag_from_z(compute_z_value('a.daily_mean_period', 'h.daily_mean_period_10yr_avg', 'h.daily_mean_period_10yr_sd'), 'h.daily_mean_period_10yr_sd') }} AS daily_mean_period_flag,

    -- rainfall_intensity
    a.rainfall_intensity_period AS rainfall_intensity_period_arrival,
    h.rainfall_intensity_period_10yr_avg AS rainfall_intensity_period_hist,
    {{ ratio('a.rainfall_intensity_period', 'h.rainfall_intensity_period_10yr_avg') }} AS rainfall_intensity_period_ratio,
    {{ delta('a.rainfall_intensity_period', 'h.rainfall_intensity_period_10yr_avg') }} AS rainfall_intensity_period_delta,
    {{ compute_z_value('a.rainfall_intensity_period', 'h.rainfall_intensity_period_10yr_avg', 'h.rainfall_intensity_period_10yr_sd') }} AS rainfall_intensity_period_z,
    {{ flag_from_z(compute_z_value('a.rainfall_intensity_period', 'h.rainfall_intensity_period_10yr_avg', 'h.rainfall_intensity_period_10yr_sd'), 'h.rainfall_intensity_period_10yr_sd') }} AS rainfall_intensity_period_flag,

    -- sunshine_duration_minutes
    a.sunshine_duration_minutes_period AS sunshine_duration_minutes_period_arrival,
    h.sunshine_duration_minutes_period_10yr_avg AS sunshine_duration_minutes_period_hist,
    {{ ratio('a.sunshine_duration_minutes_period', 'h.sunshine_duration_minutes_period_10yr_avg') }} AS sunshine_duration_minutes_period_ratio,
    {{ delta('a.sunshine_duration_minutes_period', 'h.sunshine_duration_minutes_period_10yr_avg') }} AS sunshine_duration_minutes_period_delta,
    {{ compute_z_value('a.sunshine_duration_minutes_period', 'h.sunshine_duration_minutes_period_10yr_avg', 'h.sunshine_duration_minutes_period_10yr_sd') }} AS sunshine_duration_minutes_period_z,
    {{ flag_from_z(compute_z_value('a.sunshine_duration_minutes_period', 'h.sunshine_duration_minutes_period_10yr_avg', 'h.sunshine_duration_minutes_period_10yr_sd'), 'h.sunshine_duration_minutes_period_10yr_sd') }} AS sunshine_duration_minutes_period_flag,

    -- aligned_wind
    a.aligned_wind_period AS aligned_wind_period_arrival,
    h.aligned_wind_period_10yr_avg AS aligned_wind_period_hist,
    {{ ratio('a.aligned_wind_period', 'h.aligned_wind_period_10yr_avg') }} AS aligned_wind_period_ratio,
    {{ delta('a.aligned_wind_period', 'h.aligned_wind_period_10yr_avg') }} AS aligned_wind_period_delta,
    {{ compute_z_value('a.aligned_wind_period', 'h.aligned_wind_period_10yr_avg', 'h.aligned_wind_period_10yr_sd') }} AS aligned_wind_period_z,
    {{ flag_from_z(compute_z_value('a.aligned_wind_period', 'h.aligned_wind_period_10yr_avg', 'h.aligned_wind_period_10yr_sd'), 'h.aligned_wind_period_10yr_sd') }} AS aligned_wind_period_flag,

    -- num_days_aligned_wind_5_plus
    a.num_days_aligned_wind_5_plus_period AS num_days_aligned_wind_5_plus_period_arrival,
    h.num_days_aligned_wind_5_plus_period_10yr_avg AS num_days_aligned_wind_5_plus_period_hist,
    {{ ratio('a.num_days_aligned_wind_5_plus_period', 'h.num_days_aligned_wind_5_plus_period_10yr_avg') }} AS num_days_aligned_wind_5_plus_period_ratio,
    {{ delta('a.num_days_aligned_wind_5_plus_period', 'h.num_days_aligned_wind_5_plus_period_10yr_avg') }} AS num_days_aligned_wind_5_plus_period_delta,
    {{ compute_z_value('a.num_days_aligned_wind_5_plus_period', 'h.num_days_aligned_wind_5_plus_period_10yr_avg', 'h.num_days_aligned_wind_5_plus_period_10yr_sd') }} AS num_days_aligned_wind_5_plus_period_z,
    {{ flag_from_z(compute_z_value('a.num_days_aligned_wind_5_plus_period', 'h.num_days_aligned_wind_5_plus_period_10yr_avg', 'h.num_days_aligned_wind_5_plus_period_10yr_sd'), 'h.num_days_aligned_wind_5_plus_period_10yr_sd') }} AS num_days_aligned_wind_5_plus_period_flag,

    -- num_days_rain_10mm_plus
    a.num_days_rain_10mm_plus_period AS num_days_rain_10mm_plus_period_arrival,
    h.num_days_rain_10mm_plus_period_10yr_avg AS num_days_rain_10mm_plus_period_hist,
    {{ ratio('a.num_days_rain_10mm_plus_period', 'h.num_days_rain_10mm_plus_period_10yr_avg') }} AS num_days_rain_10mm_plus_period_ratio,
    {{ delta('a.num_days_rain_10mm_plus_period', 'h.num_days_rain_10mm_plus_period_10yr_avg') }} AS num_days_rain_10mm_plus_period_delta,
    {{ compute_z_value('a.num_days_rain_10mm_plus_period', 'h.num_days_rain_10mm_plus_period_10yr_avg', 'h.num_days_rain_10mm_plus_period_10yr_sd') }} AS num_days_rain_10mm_plus_period_z,
    {{ flag_from_z(compute_z_value('a.num_days_rain_10mm_plus_period', 'h.num_days_rain_10mm_plus_period_10yr_avg', 'h.num_days_rain_10mm_plus_period_10yr_sd'), 'h.num_days_rain_10mm_plus_period_10yr_sd') }} AS num_days_rain_10mm_plus_period_flag,

    -- num_days_moderate_rain
    a.num_days_moderate_rain_period AS num_days_moderate_rain_period_arrival,
    h.num_days_moderate_rain_period_10yr_avg AS num_days_moderate_rain_period_hist,
    {{ ratio('a.num_days_moderate_rain_period', 'h.num_days_moderate_rain_period_10yr_avg') }} AS num_days_moderate_rain_period_ratio,
    {{ delta('a.num_days_moderate_rain_period', 'h.num_days_moderate_rain_period_10yr_avg') }} AS num_days_moderate_rain_period_delta,
    {{ compute_z_value('a.num_days_moderate_rain_period', 'h.num_days_moderate_rain_period_10yr_avg', 'h.num_days_moderate_rain_period_10yr_sd') }} AS num_days_moderate_rain_period_z,
    {{ flag_from_z(compute_z_value('a.num_days_moderate_rain_period', 'h.num_days_moderate_rain_period_10yr_avg', 'h.num_days_moderate_rain_period_10yr_sd'), 'h.num_days_moderate_rain_period_10yr_sd') }} AS num_days_moderate_rain_period_flag
  FROM arr a
  JOIN hist h
    USING (bird, arrival_year, location_name)
)

SELECT *
FROM base