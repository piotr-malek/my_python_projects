{{ config(materialized='table') }}

-- Compute chunk-level metrics for each historical effective_year
WITH hist_yearly_chunk AS (
  SELECT
    chunk_id,
    effective_year,

    AVG(daily_mean) AS daily_mean_chunk_year,
    AVG(rainfall_intensity) AS rainfall_intensity_chunk_year,
    AVG(sunshine_duration_minutes) AS sunshine_duration_minutes_chunk_year,
    AVG(aligned_wind) AS aligned_wind_chunk_year,

    COUNTIF(aligned_wind >= 5) AS num_days_aligned_wind_5_plus_chunk_year,
    COUNTIF(rain_10mm_plus_day) AS num_days_rain_10mm_plus_chunk_year,
    COUNTIF(rainfall_intensity >= 1) AS num_days_moderate_rain_chunk_year
  FROM {{ ref('core_historical_metrics_base_rolling') }}
  GROUP BY 1,2
)

SELECT
  chunk_id,

  AVG(daily_mean_chunk_year) AS daily_mean_chunk_10yr_avg,
  STDDEV_SAMP(daily_mean_chunk_year) AS daily_mean_chunk_10yr_sd,

  AVG(rainfall_intensity_chunk_year) AS rainfall_intensity_chunk_10yr_avg,
  STDDEV_SAMP(rainfall_intensity_chunk_year) AS rainfall_intensity_chunk_10yr_sd,

  AVG(sunshine_duration_minutes_chunk_year) AS sunshine_duration_minutes_chunk_10yr_avg,
  STDDEV_SAMP(sunshine_duration_minutes_chunk_year) AS sunshine_duration_minutes_chunk_10yr_sd,

  AVG(aligned_wind_chunk_year) AS aligned_wind_chunk_10yr_avg,
  STDDEV_SAMP(aligned_wind_chunk_year) AS aligned_wind_chunk_10yr_sd,

  AVG(num_days_aligned_wind_5_plus_chunk_year) AS num_days_aligned_wind_5_plus_chunk_10yr_avg,
  STDDEV_SAMP(num_days_aligned_wind_5_plus_chunk_year) AS num_days_aligned_wind_5_plus_chunk_10yr_sd,

  AVG(num_days_rain_10mm_plus_chunk_year) AS num_days_rain_10mm_plus_chunk_10yr_avg,
  STDDEV_SAMP(num_days_rain_10mm_plus_chunk_year) AS num_days_rain_10mm_plus_chunk_10yr_sd,

  AVG(num_days_moderate_rain_chunk_year) AS num_days_moderate_rain_chunk_10yr_avg,
  STDDEV_SAMP(num_days_moderate_rain_chunk_year) AS num_days_moderate_rain_chunk_10yr_sd
FROM hist_yearly_chunk
GROUP BY 1


