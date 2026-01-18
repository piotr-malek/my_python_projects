{{
    config(
        materialized='view'
    )
}}

WITH migration_locations AS (
    SELECT
        REPLACE(LOWER(s.common_name), " ", "_") AS bird,
        s.year AS arrival_year,
        l.location_name,
        l.location_type,
        l.migration_direction,
        s.status as arrival_status,
        DATE(s.observation_date) AS arrival_date,
        DATE_SUB(DATE(s.observation_date), INTERVAL l.weather_check_start DAY) AS weather_date_start,
        DATE_SUB(DATE(s.observation_date), INTERVAL l.weather_check_end DAY) AS weather_date_end,
        DATE_SUB(DATE(s.observation_date), INTERVAL l.insect_check_start DAY) AS insect_date_start
    FROM {{ source('birding_raw', 'pl_ma_first_sightings') }} s
    JOIN {{ ref('stg_migration_locations_transformed') }} l
        ON REPLACE(LOWER(s.common_name), " ", "_") = l.bird
),

all_dates AS (
    SELECT
        *,
        d AS weather_date,
        d < weather_date_start AS insect_relevant_date
    FROM migration_locations ml,
         UNNEST(GENERATE_DATE_ARRAY(COALESCE(insect_date_start, weather_date_start), weather_date_end)) AS d
),

numbered_dates AS (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY bird, arrival_year, location_name, insect_relevant_date
             ORDER BY weather_date
           ) AS rn,
           COUNT(*) OVER (
             PARTITION BY bird, arrival_year, location_name, insect_relevant_date
           ) AS total_dates_in_partition
    FROM all_dates
),

chunked_dates AS (
    SELECT *,
           CASE 
               WHEN total_dates_in_partition <= 2 THEN 1
               WHEN total_dates_in_partition <= 4 THEN 1
               WHEN MOD(total_dates_in_partition, 3) = 1 THEN 
                   -- If dividing by 3 leaves remainder 1, use one fewer chunk (creates 4-element chunks instead)
                   CAST((total_dates_in_partition - 1) / 3 AS INT64)
               ELSE CAST(CEIL(total_dates_in_partition / 3.0) AS INT64)
           END AS total_chunks
    FROM numbered_dates
),

final_chunked_dates AS (
    SELECT *,
           CASE 
               WHEN total_dates_in_partition <= 4 THEN 1
               ELSE CAST(CEIL(rn * 1.0 / CEIL(total_dates_in_partition * 1.0 / total_chunks)) AS INT64)
           END AS chunk_id
    FROM chunked_dates
)

SELECT 
    bird,
    arrival_year,
    location_name,
    location_type,
    migration_direction,
    arrival_status,
    weather_date,
    insect_relevant_date,
    bird || "_" || arrival_year || "_" || location_name || "_" || 
    CASE WHEN insect_relevant_date THEN "insect" ELSE "weather" END || "_" || chunk_id AS chunk_id
FROM final_chunked_dates