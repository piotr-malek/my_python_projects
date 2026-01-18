{{ config(materialized='table') }}

WITH
-- 1) Daily: average + peak-emphasis
daily AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    -- temp
    AVG(daily_mean_rolling_flag) AS d_temp_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(daily_mean_rolling_flag)) * daily_mean_rolling_flag),
                SUM(EXP(0.7 * ABS(daily_mean_rolling_flag)))) AS d_temp_peak,

    -- rain
    AVG(rainfall_intensity_rolling_flag) AS d_rain_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(rainfall_intensity_rolling_flag)) * rainfall_intensity_rolling_flag),
                SUM(EXP(0.7 * ABS(rainfall_intensity_rolling_flag)))) AS d_rain_peak,

    -- sun
    AVG(sunshine_duration_minutes_rolling_flag) AS d_sun_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(sunshine_duration_minutes_rolling_flag)) * sunshine_duration_minutes_rolling_flag),
                SUM(EXP(0.7 * ABS(sunshine_duration_minutes_rolling_flag)))) AS d_sun_peak,

    -- wind
    AVG(aligned_wind_rolling_flag) AS d_wind_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(aligned_wind_rolling_flag)) * aligned_wind_rolling_flag),
                SUM(EXP(0.7 * ABS(aligned_wind_rolling_flag)))) AS d_wind_peak,

    -- wind 5+
    AVG(p_aligned_wind_5_plus_flag) AS d_wind5_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(p_aligned_wind_5_plus_flag)) * p_aligned_wind_5_plus_flag),
                SUM(EXP(0.7 * ABS(p_aligned_wind_5_plus_flag)))) AS d_wind5_peak,

    -- rain 10+
    AVG(p_rain_10mm_plus_flag) AS d_rain10_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(p_rain_10mm_plus_flag)) * p_rain_10mm_plus_flag),
                SUM(EXP(0.7 * ABS(p_rain_10mm_plus_flag)))) AS d_rain10_peak,

    -- moderate rain
    AVG(p_moderate_rain_flag) AS d_rainmod_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(p_moderate_rain_flag)) * p_moderate_rain_flag),
                SUM(EXP(0.7 * ABS(p_moderate_rain_flag)))) AS d_rainmod_peak
  FROM {{ ref('mart_daily_flags') }}
  GROUP BY bird, arrival_year, location_name
),

-- collapse daily into single score per metric
daily_scores AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    0.5 * d_temp_avg + 0.5 * d_temp_peak AS daily_temp_score,
    0.5 * d_rain_avg + 0.5 * d_rain_peak AS daily_rain_score,
    0.5 * d_sun_avg  + 0.5 * d_sun_peak  AS daily_sun_score,
    0.5 * d_wind_avg + 0.5 * d_wind_peak AS daily_wind_score,
    0.5 * d_wind5_avg + 0.5 * d_wind5_peak AS daily_wind5_score,
    0.5 * d_rain10_avg + 0.5 * d_rain10_peak AS daily_rain10_score,
    0.5 * d_rainmod_avg + 0.5 * d_rainmod_peak AS daily_rainmod_score
  FROM daily
),

-- 2) Chunk: average + peak
chunk_join AS (
  SELECT
    f.bird,
    f.arrival_year,
    f.location_name,
    c.*
  FROM {{ ref('mart_daily_flags') }} f
  JOIN {{ ref('mart_chunk_flags') }} c USING (chunk_id)
),

chunk AS (
  SELECT
    bird,
    arrival_year,
    location_name,

    AVG(daily_mean_chunk_flag) AS c_temp_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(daily_mean_chunk_flag)) * daily_mean_chunk_flag),
                SUM(EXP(0.7 * ABS(daily_mean_chunk_flag)))) AS c_temp_peak,

    AVG(rainfall_intensity_chunk_flag) AS c_rain_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(rainfall_intensity_chunk_flag)) * rainfall_intensity_chunk_flag),
                SUM(EXP(0.7 * ABS(rainfall_intensity_chunk_flag)))) AS c_rain_peak,

    AVG(sunshine_duration_minutes_chunk_flag) AS c_sun_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(sunshine_duration_minutes_chunk_flag)) * sunshine_duration_minutes_chunk_flag),
                SUM(EXP(0.7 * ABS(sunshine_duration_minutes_chunk_flag)))) AS c_sun_peak,

    AVG(aligned_wind_chunk_flag) AS c_wind_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(aligned_wind_chunk_flag)) * aligned_wind_chunk_flag),
                SUM(EXP(0.7 * ABS(aligned_wind_chunk_flag)))) AS c_wind_peak,

    AVG(num_days_aligned_wind_5_plus_chunk_flag) AS c_wind5_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(num_days_aligned_wind_5_plus_chunk_flag)) * num_days_aligned_wind_5_plus_chunk_flag),
                SUM(EXP(0.7 * ABS(num_days_aligned_wind_5_plus_chunk_flag)))) AS c_wind5_peak,

    AVG(num_days_rain_10mm_plus_chunk_flag) AS c_rain10_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(num_days_rain_10mm_plus_chunk_flag)) * num_days_rain_10mm_plus_chunk_flag),
                SUM(EXP(0.7 * ABS(num_days_rain_10mm_plus_chunk_flag)))) AS c_rain10_peak,

    AVG(num_days_moderate_rain_chunk_flag) AS c_rainmod_avg,
    SAFE_DIVIDE(SUM(EXP(0.7 * ABS(num_days_moderate_rain_chunk_flag)) * num_days_moderate_rain_chunk_flag),
                SUM(EXP(0.7 * ABS(num_days_moderate_rain_chunk_flag)))) AS c_rainmod_peak
  FROM chunk_join
  GROUP BY bird, arrival_year, location_name
),

chunk_scores AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    0.5 * c_temp_avg + 0.5 * c_temp_peak AS chunk_temp_score,
    0.5 * c_rain_avg + 0.5 * c_rain_peak AS chunk_rain_score,
    0.5 * c_sun_avg  + 0.5 * c_sun_peak  AS chunk_sun_score,
    0.5 * c_wind_avg + 0.5 * c_wind_peak AS chunk_wind_score,
    0.5 * c_wind5_avg + 0.5 * c_wind5_peak AS chunk_wind5_score,
    0.5 * c_rain10_avg + 0.5 * c_rain10_peak AS chunk_rain10_score,
    0.5 * c_rainmod_avg + 0.5 * c_rainmod_peak AS chunk_rainmod_score
  FROM chunk
),

-- 3) Periods: already aggregated, just average if multiple rows
periods AS (
  SELECT
    bird,
    arrival_year,
    location_name,
    AVG(daily_mean_period_flag) AS period_temp_score,
    AVG(rainfall_intensity_period_flag) AS period_rain_score,
    AVG(sunshine_duration_minutes_period_flag) AS period_sun_score,
    AVG(aligned_wind_period_flag) AS period_wind_score,
    AVG(num_days_aligned_wind_5_plus_period_flag) AS period_wind5_score,
    AVG(num_days_rain_10mm_plus_period_flag) AS period_rain10_score,
    AVG(num_days_moderate_rain_period_flag) AS period_rainmod_score
  FROM {{ ref('mart_period_flags') }}
  GROUP BY bird, arrival_year, location_name
),

-- 4) Combine into unified scores
final AS (
  SELECT
    COALESCE(d.bird, c.bird, p.bird) AS bird,
    COALESCE(d.arrival_year, c.arrival_year, p.arrival_year) AS arrival_year,
    COALESCE(d.location_name, c.location_name, p.location_name) AS location_name,

    -- Sustained drivers
    SAFE_DIVIDE(
      0.50 * p.period_temp_score + 0.30 * c.chunk_temp_score + 0.20 * d.daily_temp_score,
      (0.50 * IF(p.period_temp_score IS NOT NULL,1,0) +
       0.30 * IF(c.chunk_temp_score IS NOT NULL,1,0) +
       0.20 * IF(d.daily_temp_score IS NOT NULL,1,0))
    ) AS temp_score,

    SAFE_DIVIDE(
      0.50 * p.period_sun_score + 0.30 * c.chunk_sun_score + 0.20 * d.daily_sun_score,
      (0.50 * IF(p.period_sun_score IS NOT NULL,1,0) +
       0.30 * IF(c.chunk_sun_score IS NOT NULL,1,0) +
       0.20 * IF(d.daily_sun_score IS NOT NULL,1,0))
    ) AS sun_score,

    -- Event drivers
    SAFE_DIVIDE(
      0.30 * p.period_wind_score + 0.35 * c.chunk_wind_score + 0.35 * d.daily_wind_score,
      (0.30 * IF(p.period_wind_score IS NOT NULL,1,0) +
       0.35 * IF(c.chunk_wind_score IS NOT NULL,1,0) +
       0.35 * IF(d.daily_wind_score IS NOT NULL,1,0))
    ) AS wind_alignment_score,

    SAFE_DIVIDE(
      0.30 * p.period_wind5_score + 0.35 * c.chunk_wind5_score + 0.35 * d.daily_wind5_score,
      (0.30 * IF(p.period_wind5_score IS NOT NULL,1,0) +
       0.35 * IF(c.chunk_wind5_score IS NOT NULL,1,0) +
       0.35 * IF(d.daily_wind5_score IS NOT NULL,1,0))
    ) AS aligned_wind_5plus_score,

    -- INVERT rain metrics: multiply by -1 so higher scores = less rain = more favorable
    SAFE_DIVIDE(
      0.30 * (-1 * p.period_rain_score) + 0.35 * (-1 * c.chunk_rain_score) + 0.35 * (-1 * d.daily_rain_score),
      (0.30 * IF(p.period_rain_score IS NOT NULL,1,0) +
       0.35 * IF(c.chunk_rain_score IS NOT NULL,1,0) +
       0.35 * IF(d.daily_rain_score IS NOT NULL,1,0))
    ) AS rain_intensity_score,

    SAFE_DIVIDE(
      0.30 * (-1 * p.period_rain10_score) + 0.35 * (-1 * c.chunk_rain10_score) + 0.35 * (-1 * d.daily_rain10_score),
      (0.30 * IF(p.period_rain10_score IS NOT NULL,1,0) +
       0.35 * IF(c.chunk_rain10_score IS NOT NULL,1,0) +
       0.35 * IF(d.daily_rain10_score IS NOT NULL,1,0))
    ) AS rain_10mm_plus_score,

    SAFE_DIVIDE(
      0.30 * (-1 * p.period_rainmod_score) + 0.35 * (-1 * c.chunk_rainmod_score) + 0.35 * (-1 * d.daily_rainmod_score),
      (0.30 * IF(p.period_rainmod_score IS NOT NULL,1,0) +
       0.35 * IF(c.chunk_rainmod_score IS NOT NULL,1,0) +
       0.35 * IF(d.daily_rainmod_score IS NOT NULL,1,0))
    ) AS moderate_rain_score

  FROM daily_scores d
  FULL OUTER JOIN chunk_scores c
    ON d.bird = c.bird AND d.arrival_year = c.arrival_year AND d.location_name = c.location_name
  FULL OUTER JOIN periods p
    ON COALESCE(d.bird, c.bird) = p.bird
   AND COALESCE(d.arrival_year, c.arrival_year) = p.arrival_year
   AND COALESCE(d.location_name, c.location_name) = p.location_name
)

SELECT * FROM final