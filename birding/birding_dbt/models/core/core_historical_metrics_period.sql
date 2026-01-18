{{ config(materialized='table') }}

-- Derive period bounds from arrival metrics so comparisons use the arrival-year window
WITH arr_period AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    MIN(weather_date) AS period_start,
    MAX(weather_date) AS period_end
  FROM {{ ref('core_arrival_metrics_base_rolling') }}
  GROUP BY 1,2,3
),

-- Compute period-level metrics for each historical effective_year
hist_yearly_period AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    effective_year,

    AVG(daily_mean) AS daily_mean_period_year,
    AVG(rainfall_intensity) AS rainfall_intensity_period_year,
    AVG(sunshine_duration_minutes) AS sunshine_duration_minutes_period_year,
    AVG(aligned_wind) AS aligned_wind_period_year,

    COUNTIF(aligned_wind >= 5) AS num_days_aligned_wind_5_plus_period_year,
    COUNTIF(rain_10mm_plus_day) AS num_days_rain_10mm_plus_period_year,
    COUNTIF(rainfall_intensity >= 1) AS num_days_moderate_rain_period_year
  FROM {{ ref('core_historical_metrics_base_rolling') }}
  GROUP BY 1,2,3,4
),

-- Aggregate across the historical years: 10yr mean and 10yr stddev at the period level
hist_period AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    AVG(daily_mean_period_year) AS daily_mean_period_10yr_avg,
    STDDEV_SAMP(daily_mean_period_year) AS daily_mean_period_10yr_sd,

    AVG(rainfall_intensity_period_year) AS rainfall_intensity_period_10yr_avg,
    STDDEV_SAMP(rainfall_intensity_period_year) AS rainfall_intensity_period_10yr_sd,

    AVG(sunshine_duration_minutes_period_year) AS sunshine_duration_minutes_period_10yr_avg,
    STDDEV_SAMP(sunshine_duration_minutes_period_year) AS sunshine_duration_minutes_period_10yr_sd,

    AVG(aligned_wind_period_year) AS aligned_wind_period_10yr_avg,
    STDDEV_SAMP(aligned_wind_period_year) AS aligned_wind_period_10yr_sd,

    AVG(num_days_aligned_wind_5_plus_period_year) AS num_days_aligned_wind_5_plus_period_10yr_avg,
    STDDEV_SAMP(num_days_aligned_wind_5_plus_period_year) AS num_days_aligned_wind_5_plus_period_10yr_sd,

    AVG(num_days_rain_10mm_plus_period_year) AS num_days_rain_10mm_plus_period_10yr_avg,
    STDDEV_SAMP(num_days_rain_10mm_plus_period_year) AS num_days_rain_10mm_plus_period_10yr_sd,

    AVG(num_days_moderate_rain_period_year) AS num_days_moderate_rain_period_10yr_avg,
    STDDEV_SAMP(num_days_moderate_rain_period_year) AS num_days_moderate_rain_period_10yr_sd
  FROM hist_yearly_period
  GROUP BY 1,2,3
)

SELECT
  p.bird,
  p.arrival_year,
  p.location_name,
  p.period_start,
  p.period_end,
  h.* EXCEPT(bird, arrival_year, location_name)
FROM arr_period p
LEFT JOIN hist_period h
  USING (bird, arrival_year, location_name)