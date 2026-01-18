{{ config(materialized='table') }}

SELECT
  bird,
  arrival_year,
  location_name,
  weather_date,
  chunk_id,
  day_idx,
  daily_mean_rolling_flag,
  rainfall_intensity_rolling_flag ,
  sunshine_duration_minutes_rolling_flag,
  aligned_wind_rolling_flag,
  p_aligned_wind_5_plus_flag,
  p_rain_10mm_plus_flag,
  p_moderate_rain_flag
FROM {{ ref('mart_comparison_daily') }}

