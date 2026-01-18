-- models/marts/arrival_metrics_period_agg.sql
{{ config(materialized='table') }}

SELECT
  bird,
  arrival_year,
  location_name,

  MIN(weather_date) AS period_start,
  MAX(weather_date) AS period_end,

  AVG(daily_mean) AS daily_mean_period,
  AVG(rainfall_intensity) AS rainfall_intensity_period,
  AVG(sunshine_duration_minutes) AS sunshine_duration_minutes_period,
  AVG(aligned_wind) AS aligned_wind_period,

  COUNTIF(aligned_wind >= 5) AS num_days_aligned_wind_5_plus_period,
  COUNTIF(rain_10mm_plus_day) AS num_days_rain_10mm_plus_period,
  COUNTIF(rainfall_intensity >= 1) AS num_days_moderate_rain_period
FROM {{ ref('core_arrival_metrics_base_rolling') }}
GROUP BY 1,2,3