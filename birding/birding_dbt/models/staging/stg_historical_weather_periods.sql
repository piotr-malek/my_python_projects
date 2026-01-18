{{
    config(
        materialized='view'
    )
}}

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 1 YEAR) as weather_date,
  arrival_year - 1 as effective_year,
  1 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 2 YEAR) as weather_date,
  arrival_year - 2 as effective_year,
  2 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 3 YEAR) as weather_date,
  arrival_year - 3 as effective_year,
  3 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 4 YEAR) as weather_date,
  arrival_year - 4 as effective_year,
  4 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 5 YEAR) as weather_date,
  arrival_year - 5 as effective_year,
  5 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 6 YEAR) as weather_date,
  arrival_year - 6 as effective_year,
  6 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 7 YEAR) as weather_date,
  arrival_year - 7 as effective_year,
  7 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 8 YEAR) as weather_date,
  arrival_year - 8 as effective_year,
  8 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 9 YEAR) as weather_date,
  arrival_year - 9 as effective_year,
  9 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 

UNION ALL

SELECT 
  * EXCEPT(weather_date),
  DATE_SUB(weather_date, INTERVAL 10 YEAR) as weather_date,
  arrival_year - 10 as effective_year,
  10 as offset
FROM {{ ref('stg_arrival_weather_periods') }} 