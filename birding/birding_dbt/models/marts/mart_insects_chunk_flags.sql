{{ config(materialized='table') }}

SELECT
  chunk_id,
  tmean_avg_flag,
  tmin_avg_flag,
  tmax_avg_flag,
  precip_total_flag,
  srad_avg_flag,
  warm10_days_flag,
  warm15_days_flag,
  warm20_days_flag,
  warm_anomaly_days_flag,
  optimal_insect_days_flag
FROM {{ ref('mart_insects_comparison_chunk') }}




