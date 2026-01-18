{{
    config(
        materialized='view'
    )
}}

WITH base AS (
  SELECT
    location_name,
    bird,
    location_type,
    order_nr,
    migration_direction_arrival,
    migration_direction_departure,
    CAST(weather_check_start as int64) as weather_check_start,
    weather_check_end,
    CAST(weather_check_start + 14 as int64) as insect_check_start,
    migration_direction_arrival <> migration_direction_departure as multi_direction_location
  FROM {{ ref('stg_migration_locations') }}
)

SELECT
  location_name,
  bird,
  location_type,
  order_nr,
  weather_check_start,
  IF(multi_direction_location, 
    CAST(weather_check_start - CEIL((weather_check_start - weather_check_end + 1) / 2) + 1 as int64),
    CAST(weather_check_end as int64)) as weather_check_end,
  insect_check_start,
  migration_direction_arrival as migration_direction
FROM base

UNION ALL

SELECT
  location_name,
  bird,
  location_type,
  order_nr,
  CAST(weather_check_start - CEIL((weather_check_start - weather_check_end + 1) / 2) as int64) weather_check_start,
  weather_check_end,
  CAST(NULL as int64) as insect_check_start,
  migration_direction_departure as migration_direction
FROM base
WHERE multi_direction_location