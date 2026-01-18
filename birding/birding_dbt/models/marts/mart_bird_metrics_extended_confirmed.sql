{{ config(materialized='table') }}

WITH 
first_sightings AS (
  SELECT 
    REPLACE(LOWER(common_name), ' ', '_') as bird,
    year as arrival_year,
    status,
    DATE(observation_date) as first_sighting_date,
    EXTRACT(DAYOFYEAR FROM DATE(observation_date)) as day_of_year
  FROM {{ source('birding_raw', 'pl_ma_first_sightings') }}
  WHERE status = 'normal'
),

bird_stats AS (
  SELECT
    bird,
    APPROX_QUANTILES(day_of_year, 2)[OFFSET(1)] as median_day_of_year,
    STDDEV(day_of_year) as stddev_day_of_year
  FROM first_sightings
  GROUP BY 1
)

SELECT
  m.*,
  f.first_sighting_date,
  f.status,
  ml.location_type,
  ml.order_nr as location_order_nr,
  f.day_of_year as actual_day_of_year,
  s.median_day_of_year,
  DATE_ADD(DATE(EXTRACT(YEAR FROM f.first_sighting_date), 1, 1), INTERVAL CAST(s.median_day_of_year - 1 AS INT64) DAY) as median_sighting_date_adjusted,
  CASE 
    WHEN s.stddev_day_of_year > 0 THEN
      (f.day_of_year - s.median_day_of_year) / s.stddev_day_of_year
    ELSE NULL
  END as arrival_z_score
FROM {{ ref('mart_bird_metrics_unified') }} m
JOIN first_sightings f USING(bird, arrival_year)
JOIN bird_stats s USING(bird)
JOIN {{ ref('stg_migration_locations') }} ml USING(bird, location_name)