{{ config(materialized='table') }}

-- 1) Daily rolling metrics
WITH daily AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    -- Temperature
    AVG(tmean_rolling_flag) AS d_tmean_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmean_rolling_flag)) * tmean_rolling_flag),
                SUM(EXP(0.7 * ABS(tmean_rolling_flag)))) AS d_tmean_peak,

    AVG(tmin_rolling_flag) AS d_tmin_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmin_rolling_flag)) * tmin_rolling_flag),
                SUM(EXP(0.7 * ABS(tmin_rolling_flag)))) AS d_tmin_peak,

    AVG(tmax_rolling_flag) AS d_tmax_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmax_rolling_flag)) * tmax_rolling_flag),
                SUM(EXP(0.7 * ABS(tmax_rolling_flag)))) AS d_tmax_peak,

    AVG(precip_rolling_flag) AS d_precip_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(precip_rolling_flag)) * precip_rolling_flag),
                SUM(EXP(0.7 * ABS(precip_rolling_flag)))) AS d_precip_peak,

    AVG(srad_rolling_flag) AS d_srad_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(srad_rolling_flag)) * srad_rolling_flag),
                SUM(EXP(0.7 * ABS(srad_rolling_flag)))) AS d_srad_peak,

    AVG(warm10_rolling_flag) AS d_warm10_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm10_rolling_flag)) * warm10_rolling_flag),
                SUM(EXP(0.7 * ABS(warm10_rolling_flag)))) AS d_warm10_peak,

    AVG(warm15_rolling_flag) AS d_warm15_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm15_rolling_flag)) * warm15_rolling_flag),
                SUM(EXP(0.7 * ABS(warm15_rolling_flag)))) AS d_warm15_peak,

    AVG(warm20_rolling_flag) AS d_warm20_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm20_rolling_flag)) * warm20_rolling_flag),
                SUM(EXP(0.7 * ABS(warm20_rolling_flag)))) AS d_warm20_peak,

    AVG(warm_anomaly_rolling_flag) AS d_warm_anomaly_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm_anomaly_rolling_flag)) * warm_anomaly_rolling_flag),
                SUM(EXP(0.7 * ABS(warm_anomaly_rolling_flag)))) AS d_warm_anomaly_peak,

    AVG(optimal_insect_day_rolling_flag) AS d_optimal_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(optimal_insect_day_rolling_flag)) * optimal_insect_day_rolling_flag),
                SUM(EXP(0.7 * ABS(optimal_insect_day_rolling_flag)))) AS d_optimal_peak

  FROM {{ ref('mart_insects_daily_flags') }}
  GROUP BY bird, arrival_year, location_name
),

-- 2) Chunk-level metrics
chunk_join AS (
  SELECT
    d.bird,
    d.arrival_year,
    d.location_name,
    c.*
  FROM {{ ref('mart_insects_chunk_flags') }} c
  JOIN {{ ref('mart_insects_daily_flags') }} d
    USING (chunk_id)
),

chunk AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    AVG(tmean_avg_flag) AS c_tmean_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmean_avg_flag)) * tmean_avg_flag),
                SUM(EXP(0.7 * ABS(tmean_avg_flag)))) AS c_tmean_peak,

    AVG(tmin_avg_flag) AS c_tmin_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmin_avg_flag)) * tmin_avg_flag),
                SUM(EXP(0.7 * ABS(tmin_avg_flag)))) AS c_tmin_peak,

    AVG(tmax_avg_flag) AS c_tmax_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(tmax_avg_flag)) * tmax_avg_flag),
                SUM(EXP(0.7 * ABS(tmax_avg_flag)))) AS c_tmax_peak,

    AVG(precip_total_flag) AS c_precip_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(precip_total_flag)) * precip_total_flag),
                SUM(EXP(0.7 * ABS(precip_total_flag)))) AS c_precip_peak,

    AVG(srad_avg_flag) AS c_srad_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(srad_avg_flag)) * srad_avg_flag),
                SUM(EXP(0.7 * ABS(srad_avg_flag)))) AS c_srad_peak,

    AVG(warm10_days_flag) AS c_warm10_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm10_days_flag)) * warm10_days_flag),
                SUM(EXP(0.7 * ABS(warm10_days_flag)))) AS c_warm10_peak,

    AVG(warm15_days_flag) AS c_warm15_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm15_days_flag)) * warm15_days_flag),
                SUM(EXP(0.7 * ABS(warm15_days_flag)))) AS c_warm15_peak,

    AVG(warm20_days_flag) AS c_warm20_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm20_days_flag)) * warm20_days_flag),
                SUM(EXP(0.7 * ABS(warm20_days_flag)))) AS c_warm20_peak,

    AVG(warm_anomaly_days_flag) AS c_warm_anomaly_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(warm_anomaly_days_flag)) * warm_anomaly_days_flag),
                SUM(EXP(0.7 * ABS(warm_anomaly_days_flag)))) AS c_warm_anomaly_peak,

    AVG(optimal_insect_days_flag) AS c_optimal_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(optimal_insect_days_flag)) * optimal_insect_days_flag),
                SUM(EXP(0.7 * ABS(optimal_insect_days_flag)))) AS c_optimal_peak

  FROM chunk_join
  GROUP BY bird, arrival_year, location_name
),

-- 3) Combine into **unified insect activity scores**
final AS (
  SELECT
    COALESCE(d.bird, c.bird) AS bird,
    COALESCE(d.arrival_year, c.arrival_year) AS arrival_year,
    COALESCE(d.location_name, c.location_name) AS location_name,

    -- temperature: weighted daily + chunk
    SAFE_DIVIDE(
      0.6 * c_c_tmean + 0.4 * d_c_tmean, 0.6 + 0.4
    ) AS tmean_score,
    SAFE_DIVIDE(
      0.6 * c_c_tmin + 0.4 * d_c_tmin, 0.6 + 0.4
    ) AS tmin_score,
    SAFE_DIVIDE(
      0.6 * c_c_tmax + 0.4 * d_c_tmax, 0.6 + 0.4
    ) AS tmax_score,

    -- INVERT precipitation: multiply by -1 so higher scores = less precipitation = more favorable for insects
    SAFE_DIVIDE(
      0.5 * (-1 * c_c_precip) + 0.5 * (-1 * d_c_precip), 1.0
    ) AS precip_score,

    SAFE_DIVIDE(
      0.5 * c_c_srad + 0.5 * d_c_srad, 1.0
    ) AS srad_score,

    SAFE_DIVIDE(
      0.5 * c_c_warm10 + 0.5 * d_c_warm10, 1.0
    ) AS warm10_score,
    SAFE_DIVIDE(
      0.5 * c_c_warm15 + 0.5 * d_c_warm15, 1.0
    ) AS warm15_score,
    SAFE_DIVIDE(
      0.5 * c_c_warm20 + 0.5 * d_c_warm20, 1.0
    ) AS warm20_score,

    SAFE_DIVIDE(
      0.6 * c_c_warm_anomaly + 0.4 * d_c_warm_anomaly, 1.0
    ) AS warm_anomaly_score,

    SAFE_DIVIDE(
      0.6 * c_c_optimal + 0.4 * d_c_optimal, 1.0
    ) AS optimal_insect_score

  FROM
    (SELECT *,
        0.5 * d_tmean_avg + 0.5 * d_tmean_peak AS d_c_tmean,
        0.5 * d_tmin_avg + 0.5 * d_tmin_peak AS d_c_tmin,
        0.5 * d_tmax_avg + 0.5 * d_tmax_peak AS d_c_tmax,
        0.5 * d_precip_avg + 0.5 * d_precip_peak AS d_c_precip,
        0.5 * d_srad_avg + 0.5 * d_srad_peak AS d_c_srad,
        0.5 * d_warm10_avg + 0.5 * d_warm10_peak AS d_c_warm10,
        0.5 * d_warm15_avg + 0.5 * d_warm15_peak AS d_c_warm15,
        0.5 * d_warm20_avg + 0.5 * d_warm20_peak AS d_c_warm20,
        0.5 * d_warm_anomaly_avg + 0.5 * d_warm_anomaly_peak AS d_c_warm_anomaly,
        0.5 * d_optimal_avg + 0.5 * d_optimal_peak AS d_c_optimal
     FROM daily
    ) d
  FULL OUTER JOIN
    (SELECT *,
        0.5 * c_tmean_avg + 0.5 * c_tmean_peak AS c_c_tmean,
        0.5 * c_tmin_avg + 0.5 * c_tmin_peak AS c_c_tmin,
        0.5 * c_tmax_avg + 0.5 * c_tmax_peak AS c_c_tmax,
        0.5 * c_precip_avg + 0.5 * c_precip_peak AS c_c_precip,
        0.5 * c_srad_avg + 0.5 * c_srad_peak AS c_c_srad,
        0.5 * c_warm10_avg + 0.5 * c_warm10_peak AS c_c_warm10,
        0.5 * c_warm15_avg + 0.5 * c_warm15_peak AS c_c_warm15,
        0.5 * c_warm20_avg + 0.5 * c_warm20_peak AS c_c_warm20,
        0.5 * c_warm_anomaly_avg + 0.5 * c_warm_anomaly_peak AS c_c_warm_anomaly,
        0.5 * c_optimal_avg + 0.5 * c_optimal_peak AS c_c_optimal
     FROM chunk
    ) c
  USING (bird, arrival_year, location_name)
)

SELECT * FROM final