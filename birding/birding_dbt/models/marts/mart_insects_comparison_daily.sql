{{ config(materialized='table') }}

-- Daily-level comparison of arrival-year insect metrics vs 10yr historical per day_idx within chunk

WITH arr AS (
  SELECT * FROM {{ ref('core_insects_arrival_metrics_base_rolling') }}
),
hist AS (
  SELECT * FROM {{ ref('mart_insects_historical_10yr_avgs') }}
),
base AS (
  SELECT
    a.bird,
    a.arrival_year,
    a.location_name,
    a.weather_date,
    a.chunk_id,
    a.day_idx,

    -- Daily metrics comparisons
    a.tmean AS tmean_arrival,
    h.tmean_10yr_avg AS tmean_hist,
    {{ ratio('a.tmean', 'h.tmean_10yr_avg') }} AS tmean_ratio,
    {{ delta('a.tmean', 'h.tmean_10yr_avg') }} AS tmean_delta,
    {{ compute_z_value('a.tmean', 'h.tmean_10yr_avg', 'h.tmean_10yr_sd') }} AS tmean_z,

    a.tmin AS tmin_arrival,
    h.tmin_10yr_avg AS tmin_hist,
    {{ ratio('a.tmin', 'h.tmin_10yr_avg') }} AS tmin_ratio,
    {{ delta('a.tmin', 'h.tmin_10yr_avg') }} AS tmin_delta,
    {{ compute_z_value('a.tmin', 'h.tmin_10yr_avg', 'h.tmin_10yr_sd') }} AS tmin_z,

    a.tmax AS tmax_arrival,
    h.tmax_10yr_avg AS tmax_hist,
    {{ ratio('a.tmax', 'h.tmax_10yr_avg') }} AS tmax_ratio,
    {{ delta('a.tmax', 'h.tmax_10yr_avg') }} AS tmax_delta,
    {{ compute_z_value('a.tmax', 'h.tmax_10yr_avg', 'h.tmax_10yr_sd') }} AS tmax_z,

    a.precip AS precip_arrival,
    h.precip_10yr_avg AS precip_hist,
    {{ ratio('a.precip', 'h.precip_10yr_avg') }} AS precip_ratio,
    {{ delta('a.precip', 'h.precip_10yr_avg') }} AS precip_delta,
    {{ compute_z_value('a.precip', 'h.precip_10yr_avg', 'h.precip_10yr_sd') }} AS precip_z,

    a.srad AS srad_arrival,
    h.srad_10yr_avg AS srad_hist,
    {{ ratio('a.srad', 'h.srad_10yr_avg') }} AS srad_ratio,
    {{ delta('a.srad', 'h.srad_10yr_avg') }} AS srad_delta,
    {{ compute_z_value('a.srad', 'h.srad_10yr_avg', 'h.srad_10yr_sd') }} AS srad_z,

    CAST(a.warm10 AS INT64) AS warm10_arrival,
    h.p_warm10_10yr_avg AS warm10_hist,
    {{ ratio('CAST(a.warm10 AS INT64)', 'h.p_warm10_10yr_avg') }} AS warm10_ratio,
    {{ delta('CAST(a.warm10 AS INT64)', 'h.p_warm10_10yr_avg') }} AS warm10_delta,
    {{ compute_z_value('CAST(a.warm10 AS INT64)', 'h.p_warm10_10yr_avg', 'h.p_warm10_10yr_sd') }} AS warm10_z,

    CAST(a.warm15 AS INT64) AS warm15_arrival,
    h.p_warm15_10yr_avg AS warm15_hist,
    {{ ratio('CAST(a.warm15 AS INT64)', 'h.p_warm15_10yr_avg') }} AS warm15_ratio,
    {{ delta('CAST(a.warm15 AS INT64)', 'h.p_warm15_10yr_avg') }} AS warm15_delta,
    {{ compute_z_value('CAST(a.warm15 AS INT64)', 'h.p_warm15_10yr_avg', 'h.p_warm15_10yr_sd') }} AS warm15_z,

    CAST(a.warm20 AS INT64) AS warm20_arrival,
    h.p_warm20_10yr_avg AS warm20_hist,
    {{ ratio('CAST(a.warm20 AS INT64)', 'h.p_warm20_10yr_avg') }} AS warm20_ratio,
    {{ delta('CAST(a.warm20 AS INT64)', 'h.p_warm20_10yr_avg') }} AS warm20_delta,
    {{ compute_z_value('CAST(a.warm20 AS INT64)', 'h.p_warm20_10yr_avg', 'h.p_warm20_10yr_sd') }} AS warm20_z,

    CAST(a.warm_anomaly AS INT64) AS warm_anomaly_arrival,
    h.p_warm_anomaly_10yr_avg AS warm_anomaly_hist,
    {{ ratio('CAST(a.warm_anomaly AS INT64)', 'h.p_warm_anomaly_10yr_avg') }} AS warm_anomaly_ratio,
    {{ delta('CAST(a.warm_anomaly AS INT64)', 'h.p_warm_anomaly_10yr_avg') }} AS warm_anomaly_delta,
    {{ compute_z_value('CAST(a.warm_anomaly AS INT64)', 'h.p_warm_anomaly_10yr_avg', 'h.p_warm_anomaly_10yr_sd') }} AS warm_anomaly_z,

    CAST(a.optimal_insect_day AS INT64) AS optimal_insect_day_arrival,
    h.p_optimal_insect_day_10yr_avg AS optimal_insect_day_hist,
    {{ ratio('CAST(a.optimal_insect_day AS INT64)', 'h.p_optimal_insect_day_10yr_avg') }} AS optimal_insect_day_ratio,
    {{ delta('CAST(a.optimal_insect_day AS INT64)', 'h.p_optimal_insect_day_10yr_avg') }} AS optimal_insect_day_delta,
    {{ compute_z_value('CAST(a.optimal_insect_day AS INT64)', 'h.p_optimal_insect_day_10yr_avg', 'h.p_optimal_insect_day_10yr_sd') }} AS optimal_insect_day_z,

    -- Rolling metrics comparisons
    a.tmean_rolling AS tmean_rolling_arrival,
    h.tmean_rolling_10yr_avg AS tmean_rolling_hist,
    {{ ratio('a.tmean_rolling', 'h.tmean_rolling_10yr_avg') }} AS tmean_rolling_ratio,
    {{ delta('a.tmean_rolling', 'h.tmean_rolling_10yr_avg') }} AS tmean_rolling_delta,
    {{ compute_z_value('a.tmean_rolling', 'h.tmean_rolling_10yr_avg', 'h.tmean_rolling_10yr_sd') }} AS tmean_rolling_z,

    a.tmin_rolling AS tmin_rolling_arrival,
    h.tmin_rolling_10yr_avg AS tmin_rolling_hist,
    {{ ratio('a.tmin_rolling', 'h.tmin_rolling_10yr_avg') }} AS tmin_rolling_ratio,
    {{ delta('a.tmin_rolling', 'h.tmin_rolling_10yr_avg') }} AS tmin_rolling_delta,
    {{ compute_z_value('a.tmin_rolling', 'h.tmin_rolling_10yr_avg', 'h.tmin_rolling_10yr_sd') }} AS tmin_rolling_z,

    a.tmax_rolling AS tmax_rolling_arrival,
    h.tmax_rolling_10yr_avg AS tmax_rolling_hist,
    {{ ratio('a.tmax_rolling', 'h.tmax_rolling_10yr_avg') }} AS tmax_rolling_ratio,
    {{ delta('a.tmax_rolling', 'h.tmax_rolling_10yr_avg') }} AS tmax_rolling_delta,
    {{ compute_z_value('a.tmax_rolling', 'h.tmax_rolling_10yr_avg', 'h.tmax_rolling_10yr_sd') }} AS tmax_rolling_z,

    a.precip_rolling AS precip_rolling_arrival,
    h.precip_rolling_10yr_avg AS precip_rolling_hist,
    {{ ratio('a.precip_rolling', 'h.precip_rolling_10yr_avg') }} AS precip_rolling_ratio,
    {{ delta('a.precip_rolling', 'h.precip_rolling_10yr_avg') }} AS precip_rolling_delta,
    {{ compute_z_value('a.precip_rolling', 'h.precip_rolling_10yr_avg', 'h.precip_rolling_10yr_sd') }} AS precip_rolling_z,

    a.srad_rolling AS srad_rolling_arrival,
    h.srad_rolling_10yr_avg AS srad_rolling_hist,
    {{ ratio('a.srad_rolling', 'h.srad_rolling_10yr_avg') }} AS srad_rolling_ratio,
    {{ delta('a.srad_rolling', 'h.srad_rolling_10yr_avg') }} AS srad_rolling_delta,
    {{ compute_z_value('a.srad_rolling', 'h.srad_rolling_10yr_avg', 'h.srad_rolling_10yr_sd') }} AS srad_rolling_z,

    a.warm10_3day_count AS warm10_rolling_arrival,
    h.warm10_rolling_10yr_avg AS warm10_rolling_hist,
    {{ ratio('a.warm10_3day_count', 'h.warm10_rolling_10yr_avg') }} AS warm10_rolling_ratio,
    {{ delta('a.warm10_3day_count', 'h.warm10_rolling_10yr_avg') }} AS warm10_rolling_delta,
    {{ compute_z_value('a.warm10_3day_count', 'h.warm10_rolling_10yr_avg', 'h.warm10_rolling_10yr_sd') }} AS warm10_rolling_z,

    a.warm15_3day_count AS warm15_rolling_arrival,
    h.warm15_rolling_10yr_avg AS warm15_rolling_hist,
    {{ ratio('a.warm15_3day_count', 'h.warm15_rolling_10yr_avg') }} AS warm15_rolling_ratio,
    {{ delta('a.warm15_3day_count', 'h.warm15_rolling_10yr_avg') }} AS warm15_rolling_delta,
    {{ compute_z_value('a.warm15_3day_count', 'h.warm15_rolling_10yr_avg', 'h.warm15_rolling_10yr_sd') }} AS warm15_rolling_z,

    a.warm20_3day_count AS warm20_rolling_arrival,
    h.warm20_rolling_10yr_avg AS warm20_rolling_hist,
    {{ ratio('a.warm20_3day_count', 'h.warm20_rolling_10yr_avg') }} AS warm20_rolling_ratio,
    {{ delta('a.warm20_3day_count', 'h.warm20_rolling_10yr_avg') }} AS warm20_rolling_delta,
    {{ compute_z_value('a.warm20_3day_count', 'h.warm20_rolling_10yr_avg', 'h.warm20_rolling_10yr_sd') }} AS warm20_rolling_z,

    a.warm_anomaly_3day_count AS warm_anomaly_rolling_arrival,
    h.warm_anomaly_rolling_10yr_avg AS warm_anomaly_rolling_hist,
    {{ ratio('a.warm_anomaly_3day_count', 'h.warm_anomaly_rolling_10yr_avg') }} AS warm_anomaly_rolling_ratio,
    {{ delta('a.warm_anomaly_3day_count', 'h.warm_anomaly_rolling_10yr_avg') }} AS warm_anomaly_rolling_delta,
    {{ compute_z_value('a.warm_anomaly_3day_count', 'h.warm_anomaly_rolling_10yr_avg', 'h.warm_anomaly_rolling_10yr_sd') }} AS warm_anomaly_rolling_z,

    a.optimal_insect_3day_count AS optimal_insect_day_rolling_arrival,
    h.optimal_insect_rolling_10yr_avg AS optimal_insect_day_rolling_hist,
    {{ ratio('a.optimal_insect_3day_count', 'h.optimal_insect_rolling_10yr_avg') }} AS optimal_insect_day_rolling_ratio,
    {{ delta('a.optimal_insect_3day_count', 'h.optimal_insect_rolling_10yr_avg') }} AS optimal_insect_day_rolling_delta,
    {{ compute_z_value('a.optimal_insect_3day_count', 'h.optimal_insect_rolling_10yr_avg', 'h.optimal_insect_rolling_10yr_sd') }} AS optimal_insect_day_rolling_z
  FROM arr a
  JOIN hist h
    USING (bird, arrival_year, location_name, chunk_id, day_idx)
)

SELECT
  base.*,
  -- Daily flags
  {{ flag_from_z('base.tmean_z', 'h.tmean_10yr_sd') }} AS tmean_flag,
  {{ flag_from_z('base.tmin_z', 'h.tmin_10yr_sd') }} AS tmin_flag,
  {{ flag_from_z('base.tmax_z', 'h.tmax_10yr_sd') }} AS tmax_flag,
  {{ flag_from_z('base.precip_z', 'h.precip_10yr_sd') }} AS precip_flag,
  {{ flag_from_z('base.srad_z', 'h.srad_10yr_sd') }} AS srad_flag,
  {{ flag_from_z('base.warm10_z', 'h.p_warm10_10yr_sd') }} AS warm10_flag,
  {{ flag_from_z('base.warm15_z', 'h.p_warm15_10yr_sd') }} AS warm15_flag,
  {{ flag_from_z('base.warm20_z', 'h.p_warm20_10yr_sd') }} AS warm20_flag,
  {{ flag_from_z('base.warm_anomaly_z', 'h.p_warm_anomaly_10yr_sd') }} AS warm_anomaly_flag,
  {{ flag_from_z('base.optimal_insect_day_z', 'h.p_optimal_insect_day_10yr_sd') }} AS optimal_insect_day_flag,

  -- Rolling flags
  {{ flag_from_z('base.tmean_rolling_z', 'h.tmean_rolling_10yr_sd') }} AS tmean_rolling_flag,
  {{ flag_from_z('base.tmin_rolling_z', 'h.tmin_rolling_10yr_sd') }} AS tmin_rolling_flag,
  {{ flag_from_z('base.tmax_rolling_z', 'h.tmax_rolling_10yr_sd') }} AS tmax_rolling_flag,
  {{ flag_from_z('base.precip_rolling_z', 'h.precip_rolling_10yr_sd') }} AS precip_rolling_flag,
  {{ flag_from_z('base.srad_rolling_z', 'h.srad_rolling_10yr_sd') }} AS srad_rolling_flag,
  {{ flag_from_z('base.warm10_rolling_z', 'h.warm10_rolling_10yr_sd') }} AS warm10_rolling_flag,
  {{ flag_from_z('base.warm15_rolling_z', 'h.warm15_rolling_10yr_sd') }} AS warm15_rolling_flag,
  {{ flag_from_z('base.warm20_rolling_z', 'h.warm20_rolling_10yr_sd') }} AS warm20_rolling_flag,
  {{ flag_from_z('base.warm_anomaly_rolling_z', 'h.warm_anomaly_rolling_10yr_sd') }} AS warm_anomaly_rolling_flag,
  {{ flag_from_z('base.optimal_insect_day_rolling_z', 'h.optimal_insect_rolling_10yr_sd') }} AS optimal_insect_day_rolling_flag
FROM base
JOIN {{ ref('mart_insects_historical_10yr_avgs') }} h
  USING (bird, arrival_year, location_name, chunk_id, day_idx)



