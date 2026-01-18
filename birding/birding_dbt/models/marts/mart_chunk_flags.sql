{{ config(materialized='table') }}

SELECT
  chunk_id,
  daily_mean_chunk_flag,
  rainfall_intensity_chunk_flag,
  sunshine_duration_minutes_chunk_flag,
  aligned_wind_chunk_flag,
  num_days_aligned_wind_5_plus_chunk_flag,
  num_days_rain_10mm_plus_chunk_flag,
  num_days_moderate_rain_chunk_flag
FROM {{ ref('mart_comparison_chunk') }}

