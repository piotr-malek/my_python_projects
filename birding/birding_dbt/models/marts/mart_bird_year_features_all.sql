{{ config(materialized='table') }}

WITH base AS (
  SELECT
    bird, arrival_year, location_type, location_order_nr,
    arrival_z_score,
    temp_score, sun_score, wind_alignment_score, aligned_wind_5plus_score,
    rain_intensity_score, rain_10mm_plus_score, moderate_rain_score,
    insect_presence_score
  FROM {{ ref('mart_bird_metrics_extended_all') }}
)
SELECT
  bird,
  arrival_year,

  MAX(arrival_z_score) AS arrival_z_score,

  -- Arrival site
  MAX(CASE WHEN location_order_nr = 4 THEN rain_intensity_score END) AS arrival_rain_intensity_score,
  MAX(CASE WHEN location_order_nr = 4 THEN wind_alignment_score END)  AS arrival_wind_alignment_score,
  MAX(CASE WHEN location_order_nr = 4 THEN sun_score END)             AS arrival_sun_score,
  MAX(CASE WHEN location_order_nr = 4 THEN temp_score END)            AS arrival_temp_score,
  MAX(CASE WHEN location_order_nr = 4 THEN aligned_wind_5plus_score END) AS arrival_aligned_wind_5plus_score,
  MAX(CASE WHEN location_order_nr = 4 THEN rain_10mm_plus_score END)     AS arrival_rain_10mm_plus_score,
  MAX(CASE WHEN location_order_nr = 4 THEN moderate_rain_score END)      AS arrival_moderate_rain_score,
  MAX(CASE WHEN location_order_nr = 4 THEN insect_presence_score END) AS arrival_insect_score,

  -- Wintering
  MAX(CASE WHEN location_order_nr = 1 THEN rain_intensity_score END) AS winter_rain_intensity_score,
  MAX(CASE WHEN location_order_nr = 1 THEN wind_alignment_score END) AS winter_wind_alignment_score,
  MAX(CASE WHEN location_order_nr = 1 THEN sun_score END)            AS winter_sun_score,
  MAX(CASE WHEN location_order_nr = 1 THEN temp_score END)           AS winter_temp_score,
  MAX(CASE WHEN location_order_nr = 1 THEN aligned_wind_5plus_score END) AS winter_aligned_wind_5plus_score,
  MAX(CASE WHEN location_order_nr = 1 THEN rain_10mm_plus_score END)     AS winter_rain_10mm_plus_score,
  MAX(CASE WHEN location_order_nr = 1 THEN moderate_rain_score END)      AS winter_moderate_rain_score,
  MAX(CASE WHEN location_order_nr = 1 THEN insect_presence_score END)    AS winter_insect_score,

  -- Stopover 1
  MAX(CASE WHEN location_order_nr = 2 THEN rain_intensity_score END) AS stop1_rain_intensity_score,
  MAX(CASE WHEN location_order_nr = 2 THEN wind_alignment_score END) AS stop1_wind_alignment_score,
  MAX(CASE WHEN location_order_nr = 2 THEN sun_score END)            AS stop1_sun_score,
  MAX(CASE WHEN location_order_nr = 2 THEN temp_score END)           AS stop1_temp_score,
  MAX(CASE WHEN location_order_nr = 2 THEN aligned_wind_5plus_score END) AS stop1_aligned_wind_5plus_score,
  MAX(CASE WHEN location_order_nr = 2 THEN rain_10mm_plus_score END)     AS stop1_rain_10mm_plus_score,
  MAX(CASE WHEN location_order_nr = 2 THEN moderate_rain_score END)      AS stop1_moderate_rain_score,
  MAX(CASE WHEN location_order_nr = 2 THEN insect_presence_score END)    AS stop1_insect_score,

  -- Stopover 2
  MAX(CASE WHEN location_order_nr = 3 THEN rain_intensity_score END) AS stop2_rain_intensity_score,
  MAX(CASE WHEN location_order_nr = 3 THEN wind_alignment_score END) AS stop2_wind_alignment_score,
  MAX(CASE WHEN location_order_nr = 3 THEN sun_score END)            AS stop2_sun_score,
  MAX(CASE WHEN location_order_nr = 3 THEN temp_score END)           AS stop2_temp_score,
  MAX(CASE WHEN location_order_nr = 3 THEN aligned_wind_5plus_score END) AS stop2_aligned_wind_5plus_score,
  MAX(CASE WHEN location_order_nr = 3 THEN rain_10mm_plus_score END)     AS stop2_rain_10mm_plus_score,
  MAX(CASE WHEN location_order_nr = 3 THEN moderate_rain_score END)      AS stop2_moderate_rain_score,
  MAX(CASE WHEN location_order_nr = 3 THEN insect_presence_score END)    AS stop2_insect_score,

FROM base
GROUP BY 1,2