{{ config(materialized='table') }}

SELECT
  bird,
  arrival_year,
  location_name,
  period_start,
  period_end,
  daily_mean_period_flag,
  rainfall_intensity_period_flag,
  sunshine_duration_minutes_period_flag,
  aligned_wind_period_flag,
  num_days_aligned_wind_5_plus_period_flag,
  num_days_rain_10mm_plus_period_flag,
  num_days_moderate_rain_period_flag
FROM {{ ref('mart_comparison_period') }}

