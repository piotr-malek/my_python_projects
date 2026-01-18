-- birding/birding_dbt/models/marts/mart_historical_10yr_avgs.sql
{{ config(materialized='table') }}

WITH hist AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    chunk_id,
    day_idx,
    daily_mean_rolling,
    rainfall_intensity_rolling,
    sunshine_duration_minutes_rolling,
    aligned_wind_rolling,
    num_days_aligned_wind_5_plus_rolling,
    num_days_rain_10mm_plus_rolling,
    num_days_moderate_rain_rolling
  FROM {{ ref('core_historical_metrics_base_rolling') }}
)

SELECT
  bird,
  arrival_year,
  location_name,
  chunk_id,
  day_idx,

  AVG(daily_mean_rolling) AS daily_mean_rolling_10yr_avg,
  STDDEV_SAMP(daily_mean_rolling) AS daily_mean_rolling_10yr_sd,
  AVG(rainfall_intensity_rolling) AS rainfall_intensity_rolling_10yr_avg,
  STDDEV_SAMP(rainfall_intensity_rolling) AS rainfall_intensity_rolling_10yr_sd,
  AVG(sunshine_duration_minutes_rolling) AS sunshine_duration_minutes_rolling_10yr_avg,
  STDDEV_SAMP(sunshine_duration_minutes_rolling) AS sunshine_duration_minutes_rolling_10yr_sd,
  AVG(aligned_wind_rolling) AS aligned_wind_rolling_10yr_avg,
  STDDEV_SAMP(aligned_wind_rolling) AS aligned_wind_rolling_10yr_sd,

  AVG(SAFE_DIVIDE(num_days_aligned_wind_5_plus_rolling, 3)) AS p_aligned_wind_5_plus_10yr_avg,
  STDDEV_SAMP(SAFE_DIVIDE(num_days_aligned_wind_5_plus_rolling, 3)) AS p_aligned_wind_5_plus_10yr_sd,
  AVG(SAFE_DIVIDE(num_days_rain_10mm_plus_rolling, 3)) AS p_rain_10mm_plus_10yr_avg,
  STDDEV_SAMP(SAFE_DIVIDE(num_days_rain_10mm_plus_rolling, 3)) AS p_rain_10mm_plus_10yr_sd,
  AVG(SAFE_DIVIDE(num_days_moderate_rain_rolling, 3)) AS p_moderate_rain_10yr_avg
  ,STDDEV_SAMP(SAFE_DIVIDE(num_days_moderate_rain_rolling, 3)) AS p_moderate_rain_10yr_sd
FROM hist
GROUP BY 1,2,3,4,5


