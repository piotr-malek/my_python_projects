{{ config(materialized='table') }}

-- Daily-level historical 10yr averages aligned by day_idx within chunk for insect metrics

WITH with_thresholds AS (
  SELECT * FROM {{ ref('core_insects_historical_metrics_base_rolling') }}
)

SELECT
  bird,
  arrival_year,
  location_name,
  chunk_id,
  day_idx,

  AVG(tmean) AS tmean_10yr_avg,
  STDDEV_SAMP(tmean) AS tmean_10yr_sd,

  AVG(tmin) AS tmin_10yr_avg,
  STDDEV_SAMP(tmin) AS tmin_10yr_sd,

  AVG(tmax) AS tmax_10yr_avg,
  STDDEV_SAMP(tmax) AS tmax_10yr_sd,

  AVG(precip) AS precip_10yr_avg,
  STDDEV_SAMP(precip) AS precip_10yr_sd,

  AVG(srad) AS srad_10yr_avg,
  STDDEV_SAMP(srad) AS srad_10yr_sd,

  AVG(CAST(warm10 AS INT64)) AS p_warm10_10yr_avg,
  STDDEV_SAMP(CAST(warm10 AS INT64)) AS p_warm10_10yr_sd,

  AVG(CAST(warm15 AS INT64)) AS p_warm15_10yr_avg,
  STDDEV_SAMP(CAST(warm15 AS INT64)) AS p_warm15_10yr_sd,

  AVG(CAST(warm20 AS INT64)) AS p_warm20_10yr_avg,
  STDDEV_SAMP(CAST(warm20 AS INT64)) AS p_warm20_10yr_sd,

  AVG(CAST(warm_anomaly AS INT64)) AS p_warm_anomaly_10yr_avg,
  STDDEV_SAMP(CAST(warm_anomaly AS INT64)) AS p_warm_anomaly_10yr_sd,

  AVG(CAST(optimal_insect_day AS INT64)) AS p_optimal_insect_day_10yr_avg,
  STDDEV_SAMP(CAST(optimal_insect_day AS INT64)) AS p_optimal_insect_day_10yr_sd,

  -- Add rolling averages for historical data
  AVG(tmean_rolling) AS tmean_rolling_10yr_avg,
  STDDEV_SAMP(tmean_rolling) AS tmean_rolling_10yr_sd,

  AVG(tmin_rolling) AS tmin_rolling_10yr_avg,
  STDDEV_SAMP(tmin_rolling) AS tmin_rolling_10yr_sd,

  AVG(tmax_rolling) AS tmax_rolling_10yr_avg,
  STDDEV_SAMP(tmax_rolling) AS tmax_rolling_10yr_sd,

  AVG(precip_rolling) AS precip_rolling_10yr_avg,
  STDDEV_SAMP(precip_rolling) AS precip_rolling_10yr_sd,

  AVG(srad_rolling) AS srad_rolling_10yr_avg,
  STDDEV_SAMP(srad_rolling) AS srad_rolling_10yr_sd,

  AVG(warm10_3day_count) AS warm10_rolling_10yr_avg,
  STDDEV_SAMP(warm10_3day_count) AS warm10_rolling_10yr_sd,

  AVG(warm15_3day_count) AS warm15_rolling_10yr_avg,
  STDDEV_SAMP(warm15_3day_count) AS warm15_rolling_10yr_sd,

  AVG(warm20_3day_count) AS warm20_rolling_10yr_avg,
  STDDEV_SAMP(warm20_3day_count) AS warm20_rolling_10yr_sd,

  AVG(warm_anomaly_3day_count) AS warm_anomaly_rolling_10yr_avg,
  STDDEV_SAMP(warm_anomaly_3day_count) AS warm_anomaly_rolling_10yr_sd,

  AVG(optimal_insect_3day_count) AS optimal_insect_rolling_10yr_avg,
  STDDEV_SAMP(optimal_insect_3day_count) AS optimal_insect_rolling_10yr_sd
FROM with_thresholds
GROUP BY 1,2,3,4,5




