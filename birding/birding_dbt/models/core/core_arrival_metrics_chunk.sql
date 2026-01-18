{{ config(materialized='table') }}

SELECT
  chunk_id,
  
  AVG(daily_mean) AS daily_mean_chunk,
  AVG(rainfall_intensity) AS rainfall_intensity_chunk,
  AVG(sunshine_duration_minutes) AS sunshine_duration_minutes_chunk,
  AVG(aligned_wind) AS aligned_wind_chunk,

  COUNTIF(aligned_wind >= 5) AS num_days_aligned_wind_5_plus_chunk,
  COUNTIF(rain_10mm_plus_day) AS num_days_rain_10mm_plus_chunk,
  COUNTIF(rainfall_intensity >= 1) AS num_days_moderate_rain_chunk
FROM {{ ref('core_arrival_metrics_base_rolling') }}
GROUP BY 1