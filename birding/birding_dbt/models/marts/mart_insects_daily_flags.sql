{{ config(materialized='table') }}

SELECT
  bird,
  arrival_year,
  location_name,
  weather_date,
  chunk_id,
  day_idx,
  tmean_rolling_flag,
  tmin_rolling_flag,
  tmax_rolling_flag,
  precip_rolling_flag,
  srad_rolling_flag,
  warm10_rolling_flag,
  warm15_rolling_flag,
  warm20_rolling_flag,
  warm_anomaly_rolling_flag,
  optimal_insect_day_rolling_flag
FROM {{ ref('mart_insects_comparison_daily') }}




