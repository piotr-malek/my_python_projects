WITH daily AS (
  SELECT
    p.weather_date,
    p.chunk_id,
    p.location_name,
    p.bird,
    p.arrival_year,
    w.temperature_2m_mean AS tmean,
    w.temperature_2m_min AS tmin,
    w.temperature_2m_max AS tmax,
    w.precipitation_sum AS precip,
    w.shortwave_radiation_sum AS srad
  FROM {{ ref('stg_arrival_weather_periods') }} p
  LEFT JOIN {{ source('birding_raw', 'pl_ma_spring_weather_data') }} w
    ON p.weather_date = w.date
   AND p.location_name = w.location_name
),
period_srad_p75 AS (
  SELECT
    d.bird,
    d.location_name,
    d.arrival_year,
    CAST(APPROX_QUANTILES(d.srad, 100)[OFFSET(75)] AS FLOAT64) AS p75_srad
  FROM daily d
  WHERE d.srad IS NOT NULL
  GROUP BY 1,2,3
),
with_thresholds AS (
  SELECT
    d.*,
    d.tmean >= 10 AS warm10,
    d.tmean >= 15 AS warm15,
    d.tmean >= 20 AS warm20,
    d.tmean >= (
      AVG(d.tmean) OVER (PARTITION BY d.bird, d.location_name, d.arrival_year)
      + STDDEV(d.tmean) OVER (PARTITION BY d.bird, d.location_name, d.arrival_year)
    ) AS warm_anomaly,
    (
      (d.tmean BETWEEN 15 AND 25)
      AND d.precip < 2
      AND d.srad >= GREATEST(5.0, COALESCE(ps.p75_srad, 5.0))
    ) AS optimal_insect_day
  FROM daily d
  LEFT JOIN period_srad_p75 ps
    ON d.bird = ps.bird
   AND d.location_name = ps.location_name
   AND d.arrival_year = ps.arrival_year
)
SELECT
  wt.bird,
  wt.arrival_year,
  wt.location_name,
  wt.chunk_id,
  MIN(wt.weather_date) AS chunk_start,
  MAX(wt.weather_date) AS chunk_end,
  AVG(wt.tmean) AS tmean_avg,
  AVG(wt.tmin) AS tmin_avg,
  AVG(wt.tmax) AS tmax_avg,
  SUM(wt.precip) AS precip_total,
  AVG(wt.srad) AS srad_avg,
  COUNTIF(wt.warm10) AS warm10_days,
  COUNTIF(wt.warm15) AS warm15_days,
  COUNTIF(wt.warm20) AS warm20_days,
  COUNTIF(wt.warm_anomaly) AS warm_anomaly_days,
  COUNTIF(wt.optimal_insect_day) AS optimal_insect_days
FROM with_thresholds wt
GROUP BY 1,2,3,4