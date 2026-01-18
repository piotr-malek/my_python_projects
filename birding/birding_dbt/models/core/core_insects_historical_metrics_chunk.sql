{{ config(materialized='table') }}

-- Historical insect metrics aggregated at the chunk level across the prior 10 years
-- Mirrors mart_historical_metrics_chunk, but for insect-oriented metrics

WITH hist_daily AS (
  SELECT
    p.bird,
    p.arrival_year,
    p.location_name,
    p.chunk_id,
    p.effective_year,
    p.weather_date,
    w.temperature_2m_mean AS tmean,
    w.temperature_2m_min AS tmin,
    w.temperature_2m_max AS tmax,
    w.precipitation_sum AS precip,
    w.shortwave_radiation_sum AS srad
  FROM {{ ref('stg_historical_weather_periods') }} p
  LEFT JOIN {{ source('birding_raw', 'pl_ma_spring_weather_data') }} w
    ON p.bird = w.bird
   AND p.weather_date = w.date
   AND p.location_name = w.location_name
),

period_srad_p75 AS (
  SELECT
    bird,
    location_name,
    arrival_year,
    effective_year,
    CAST(APPROX_QUANTILES(srad, 100)[OFFSET(75)] AS FLOAT64) AS p75_srad
  FROM hist_daily
  WHERE srad IS NOT NULL
  GROUP BY 1,2,3,4
),

with_thresholds AS (
  SELECT
    d.*,
    d.tmean >= 10 AS warm10,
    d.tmean >= 15 AS warm15,
    d.tmean >= 20 AS warm20,
    d.tmean >= (
      AVG(d.tmean) OVER (PARTITION BY d.bird, d.location_name, d.arrival_year, d.effective_year)
      + STDDEV(d.tmean) OVER (PARTITION BY d.bird, d.location_name, d.arrival_year, d.effective_year)
    ) AS warm_anomaly,
    (
      (d.tmean BETWEEN 15 AND 25)
      AND d.precip < 2
      AND d.srad >= GREATEST(5.0, COALESCE(ps.p75_srad, 5.0))
    ) AS optimal_insect_day
  FROM hist_daily d
  LEFT JOIN period_srad_p75 ps
    USING (bird, location_name, arrival_year, effective_year)
),

hist_yearly_chunk AS (
  SELECT
    chunk_id,
    effective_year,

    AVG(tmean) AS tmean_avg_year,
    AVG(tmin) AS tmin_avg_year,
    AVG(tmax) AS tmax_avg_year,
    SUM(precip) AS precip_total_year,
    AVG(srad) AS srad_avg_year,

    COUNTIF(warm10) AS warm10_days_year,
    COUNTIF(warm15) AS warm15_days_year,
    COUNTIF(warm20) AS warm20_days_year,
    COUNTIF(warm_anomaly) AS warm_anomaly_days_year,
    COUNTIF(optimal_insect_day) AS optimal_insect_days_year
  FROM with_thresholds
  GROUP BY 1,2
)

SELECT
  chunk_id,

  AVG(tmean_avg_year) AS tmean_avg_10yr_avg,
  STDDEV_SAMP(tmean_avg_year) AS tmean_avg_10yr_sd,

  AVG(tmin_avg_year) AS tmin_avg_10yr_avg,
  STDDEV_SAMP(tmin_avg_year) AS tmin_avg_10yr_sd,

  AVG(tmax_avg_year) AS tmax_avg_10yr_avg,
  STDDEV_SAMP(tmax_avg_year) AS tmax_avg_10yr_sd,

  AVG(precip_total_year) AS precip_total_10yr_avg,
  STDDEV_SAMP(precip_total_year) AS precip_total_10yr_sd,

  AVG(srad_avg_year) AS srad_avg_10yr_avg,
  STDDEV_SAMP(srad_avg_year) AS srad_avg_10yr_sd,

  AVG(warm10_days_year) AS warm10_days_10yr_avg,
  STDDEV_SAMP(warm10_days_year) AS warm10_days_10yr_sd,

  AVG(warm15_days_year) AS warm15_days_10yr_avg,
  STDDEV_SAMP(warm15_days_year) AS warm15_days_10yr_sd,

  AVG(warm20_days_year) AS warm20_days_10yr_avg,
  STDDEV_SAMP(warm20_days_year) AS warm20_days_10yr_sd,

  AVG(warm_anomaly_days_year) AS warm_anomaly_days_10yr_avg,
  STDDEV_SAMP(warm_anomaly_days_year) AS warm_anomaly_days_10yr_sd,

  AVG(optimal_insect_days_year) AS optimal_insect_days_10yr_avg,
  STDDEV_SAMP(optimal_insect_days_year) AS optimal_insect_days_10yr_sd
FROM hist_yearly_chunk
GROUP BY 1




