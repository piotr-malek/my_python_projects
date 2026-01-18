{{ config(materialized='table') }}

-- Compare arrival-year insect chunk metrics to 10yr historical distribution at chunk level

WITH arr AS (
  SELECT *
  FROM {{ ref('core_insects_arrival_metrics_chunk') }}
),
hist AS (
  SELECT *
  FROM {{ ref('core_insects_historical_metrics_chunk') }}
),
base AS (
  SELECT
    a.chunk_id,

    a.tmean_avg AS tmean_avg_arrival,
    h.tmean_avg_10yr_avg AS tmean_avg_hist,
    {{ ratio('a.tmean_avg', 'h.tmean_avg_10yr_avg') }} AS tmean_avg_ratio,
    {{ delta('a.tmean_avg', 'h.tmean_avg_10yr_avg') }} AS tmean_avg_delta,
    {{ compute_z_value('a.tmean_avg', 'h.tmean_avg_10yr_avg', 'h.tmean_avg_10yr_sd') }} AS tmean_avg_z,

    a.tmin_avg AS tmin_avg_arrival,
    h.tmin_avg_10yr_avg AS tmin_avg_hist,
    {{ ratio('a.tmin_avg', 'h.tmin_avg_10yr_avg') }} AS tmin_avg_ratio,
    {{ delta('a.tmin_avg', 'h.tmin_avg_10yr_avg') }} AS tmin_avg_delta,
    {{ compute_z_value('a.tmin_avg', 'h.tmin_avg_10yr_avg', 'h.tmin_avg_10yr_sd') }} AS tmin_avg_z,

    a.tmax_avg AS tmax_avg_arrival,
    h.tmax_avg_10yr_avg AS tmax_avg_hist,
    {{ ratio('a.tmax_avg', 'h.tmax_avg_10yr_avg') }} AS tmax_avg_ratio,
    {{ delta('a.tmax_avg', 'h.tmax_avg_10yr_avg') }} AS tmax_avg_delta,
    {{ compute_z_value('a.tmax_avg', 'h.tmax_avg_10yr_avg', 'h.tmax_avg_10yr_sd') }} AS tmax_avg_z,

    a.precip_total AS precip_total_arrival,
    h.precip_total_10yr_avg AS precip_total_hist,
    {{ ratio('a.precip_total', 'h.precip_total_10yr_avg') }} AS precip_total_ratio,
    {{ delta('a.precip_total', 'h.precip_total_10yr_avg') }} AS precip_total_delta,
    {{ compute_z_value('a.precip_total', 'h.precip_total_10yr_avg', 'h.precip_total_10yr_sd') }} AS precip_total_z,

    a.srad_avg AS srad_avg_arrival,
    h.srad_avg_10yr_avg AS srad_avg_hist,
    {{ ratio('a.srad_avg', 'h.srad_avg_10yr_avg') }} AS srad_avg_ratio,
    {{ delta('a.srad_avg', 'h.srad_avg_10yr_avg') }} AS srad_avg_delta,
    {{ compute_z_value('a.srad_avg', 'h.srad_avg_10yr_avg', 'h.srad_avg_10yr_sd') }} AS srad_avg_z,

    a.warm10_days AS warm10_days_arrival,
    h.warm10_days_10yr_avg AS warm10_days_hist,
    {{ ratio('a.warm10_days', 'h.warm10_days_10yr_avg') }} AS warm10_days_ratio,
    {{ delta('a.warm10_days', 'h.warm10_days_10yr_avg') }} AS warm10_days_delta,
    {{ compute_z_value('a.warm10_days', 'h.warm10_days_10yr_avg', 'h.warm10_days_10yr_sd') }} AS warm10_days_z,

    a.warm15_days AS warm15_days_arrival,
    h.warm15_days_10yr_avg AS warm15_days_hist,
    {{ ratio('a.warm15_days', 'h.warm15_days_10yr_avg') }} AS warm15_days_ratio,
    {{ delta('a.warm15_days', 'h.warm15_days_10yr_avg') }} AS warm15_days_delta,
    {{ compute_z_value('a.warm15_days', 'h.warm15_days_10yr_avg', 'h.warm15_days_10yr_sd') }} AS warm15_days_z,

    a.warm20_days AS warm20_days_arrival,
    h.warm20_days_10yr_avg AS warm20_days_hist,
    {{ ratio('a.warm20_days', 'h.warm20_days_10yr_avg') }} AS warm20_days_ratio,
    {{ delta('a.warm20_days', 'h.warm20_days_10yr_avg') }} AS warm20_days_delta,
    {{ compute_z_value('a.warm20_days', 'h.warm20_days_10yr_avg', 'h.warm20_days_10yr_sd') }} AS warm20_days_z,

    a.warm_anomaly_days AS warm_anomaly_days_arrival,
    h.warm_anomaly_days_10yr_avg AS warm_anomaly_days_hist,
    {{ ratio('a.warm_anomaly_days', 'h.warm_anomaly_days_10yr_avg') }} AS warm_anomaly_days_ratio,
    {{ delta('a.warm_anomaly_days', 'h.warm_anomaly_days_10yr_avg') }} AS warm_anomaly_days_delta,
    {{ compute_z_value('a.warm_anomaly_days', 'h.warm_anomaly_days_10yr_avg', 'h.warm_anomaly_days_10yr_sd') }} AS warm_anomaly_days_z,

    a.optimal_insect_days AS optimal_insect_days_arrival,
    h.optimal_insect_days_10yr_avg AS optimal_insect_days_hist,
    {{ ratio('a.optimal_insect_days', 'h.optimal_insect_days_10yr_avg') }} AS optimal_insect_days_ratio,
    {{ delta('a.optimal_insect_days', 'h.optimal_insect_days_10yr_avg') }} AS optimal_insect_days_delta,
    {{ compute_z_value('a.optimal_insect_days', 'h.optimal_insect_days_10yr_avg', 'h.optimal_insect_days_10yr_sd') }} AS optimal_insect_days_z
  FROM arr a
  JOIN hist h USING (chunk_id)
)

SELECT
  base.*,
  {{ flag_from_z('base.tmean_avg_z', 'h.tmean_avg_10yr_sd') }} AS tmean_avg_flag,
  {{ flag_from_z('base.tmin_avg_z', 'h.tmin_avg_10yr_sd') }} AS tmin_avg_flag,
  {{ flag_from_z('base.tmax_avg_z', 'h.tmax_avg_10yr_sd') }} AS tmax_avg_flag,
  {{ flag_from_z('base.precip_total_z', 'h.precip_total_10yr_sd') }} AS precip_total_flag,
  {{ flag_from_z('base.srad_avg_z', 'h.srad_avg_10yr_sd') }} AS srad_avg_flag,
  {{ flag_from_z('base.warm10_days_z', 'h.warm10_days_10yr_sd') }} AS warm10_days_flag,
  {{ flag_from_z('base.warm15_days_z', 'h.warm15_days_10yr_sd') }} AS warm15_days_flag,
  {{ flag_from_z('base.warm20_days_z', 'h.warm20_days_10yr_sd') }} AS warm20_days_flag,
  {{ flag_from_z('base.warm_anomaly_days_z', 'h.warm_anomaly_days_10yr_sd') }} AS warm_anomaly_days_flag,
  {{ flag_from_z('base.optimal_insect_days_z', 'h.optimal_insect_days_10yr_sd') }} AS optimal_insect_days_flag
FROM base
JOIN {{ ref('core_insects_historical_metrics_chunk') }} h USING (chunk_id)




