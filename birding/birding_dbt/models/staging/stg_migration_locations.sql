{{
    config(
        materialized='view'
    )
}}

SELECT
  bird,
  REPLACE(REGEXP_REPLACE(NORMALIZE(LOWER(location_name), NFD), r'\pM', ''), " ", "_") as location_name,
  LOWER(REPLACE(country, ' ', '_')) as country,
  SPLIT(LOWER(type), ' ')[SAFE_OFFSET(0)] as location_type,
  order_nr,
  lat,
  lon,
  weather_check_start,
  weather_check_end,
  migration_direction_arrival,
  migration_direction_departure
FROM {{ ref('migration_locations') }}