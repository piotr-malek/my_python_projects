{{ config(materialized='table') }}

WITH base AS (
  SELECT
    l.weather_date,
    l.bird,
    l.arrival_year,
    l.location_name,
    l.chunk_id,

    (w.temperature_2m_max + w.temperature_2m_min) / 2 AS daily_mean,
    IFNULL(SAFE_DIVIDE(w.precipitation_sum, w.precipitation_hours), 0) AS rainfall_intensity,
    sunshine_duration / 60 AS sunshine_duration_minutes,
    wind_speed_10m_max * COS((wind_direction_10m_dominant - l.migration_direction) * ACOS(-1) / 180) AS aligned_wind,

    w.precipitation_sum >= 10 AS rain_10mm_plus_day
  FROM {{ ref('stg_arrival_weather_periods') }} l
  LEFT JOIN {{ source('birding_raw', 'pl_ma_spring_weather_data') }} w
    ON l.bird = w.bird
   AND l.weather_date = w.date
   AND l.location_name = w.location_name
  WHERE NOT l.insect_relevant_date
),

with_roll AS (
  SELECT
    *,
    AVG(daily_mean) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS daily_mean_rolling,

    AVG(rainfall_intensity) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rainfall_intensity_rolling,

    AVG(sunshine_duration_minutes) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS sunshine_duration_minutes_rolling,

    AVG(aligned_wind) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS aligned_wind_rolling,

    COUNTIF(aligned_wind >= 5) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS num_days_aligned_wind_5_plus_rolling,

    COUNTIF(rain_10mm_plus_day) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS num_days_rain_10mm_plus_rolling,

    COUNTIF(rainfall_intensity >= 1) OVER (
      PARTITION BY bird, arrival_year, location_name
      ORDER BY weather_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS num_days_moderate_rain_rolling
  FROM base
)

SELECT
  with_roll.*,
  ROW_NUMBER() OVER (
    PARTITION BY bird, arrival_year, location_name, chunk_id
    ORDER BY weather_date
  ) AS day_idx
FROM with_roll