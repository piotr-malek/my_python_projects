SELECT
  REPLACE(LOWER(s.common_name), " ", "_") as bird,
  s.year as arrival_year,
  l.location_name,
  AVG((w.temperature_2m_max + w.temperature_2m_min) / 2) AS avg_daily_mean,
  AVG(SAFE_DIVIDE(w.precipitation_sum, w.precipitation_hours)) as avg_rainfall_intensity,
  AVG(sunshine_duration/60) avg_sunshine_duration_minutes,
  COUNTIF(w.precipitation_sum >= 10) AS num_days_rain_10mm_plus,
  COUNTIF(SAFE_DIVIDE(w.precipitation_sum, w.precipitation_hours) >= 1) AS num_days_moderate_rain,
  AVG(wind_speed_10m_max * COS((wind_direction_10m_dominant - l.migration_direction) * ACOS(-1) / 180)) as avg_aligned_wind
FROM `birding-460212.birding_raw.pl_ma_first_sightings` s
JOIN `birding-460212.birding_dbt_core.stg_migration_locations` l
  ON REPLACE(LOWER(s.common_name), " ", "_") = l.bird
LEFT JOIN `birding-460212.birding_raw.pl_ma_spring_weather_data` w
  ON REPLACE(LOWER(s.common_name), " ", "_") = w.bird
  AND s.year = w.arrival_year
GROUP BY 1,2,3
ORDER BY 1,2,3