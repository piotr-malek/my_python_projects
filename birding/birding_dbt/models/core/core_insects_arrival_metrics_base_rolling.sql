{{ config(materialized='table') }}

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
base_metrics AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY d.bird, d.arrival_year, d.location_name, d.chunk_id
      ORDER BY d.weather_date
    ) AS day_idx,
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
  m.*,
  -- Add 3-day rolling averages
  AVG(m.tmean) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS tmean_rolling,
  AVG(m.tmin) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS tmin_rolling,
  AVG(m.tmax) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS tmax_rolling,
  AVG(m.precip) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS precip_rolling,
  AVG(m.srad) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS srad_rolling,
  -- Add 3-day counts for boolean metrics
  COUNT(CASE WHEN m.warm10 THEN 1 END) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS warm10_3day_count,
  COUNT(CASE WHEN m.warm15 THEN 1 END) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS warm15_3day_count,
  COUNT(CASE WHEN m.warm20 THEN 1 END) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS warm20_3day_count,
  COUNT(CASE WHEN m.warm_anomaly THEN 1 END) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS warm_anomaly_3day_count,
  COUNT(CASE WHEN m.optimal_insect_day THEN 1 END) OVER (
    PARTITION BY m.bird, m.arrival_year, m.location_name, m.chunk_id
    ORDER BY m.weather_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS optimal_insect_3day_count
FROM base_metrics m