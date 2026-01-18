# Weighted Weather & Insect Metrics Methodology

## Overview

This document details the methodology used in two key metric tables:
- `mart_arrival_metrics_condensed`: Bird arrival/weather metrics
- `mart_insect_metrics_condensed`: Insect activity metrics

Both tables use weighted combinations of average and peak-emphasis values to capture sustained trends and extreme events that may influence bird migration or insect presence.

## 1. Daily Metrics: Average + Peak

### Source Tables
- Birds: `mart_daily_flags`
- Insects: `mart_insects_daily_flags`

### Calculation Logic

1. **Average Component**
   ```sql
   AVG(metric_flag)
   ```
   Captures the overall deviation from historical norms for each day within the period.

2. **Peak Component**
   ```sql
   weighted_peak = SUM(EXP(0.7 * ABS(flag)) * flag) / SUM(EXP(0.7 * ABS(flag)))
   ```
   - Emphasizes extreme deviations using exponential weighting
   - Positive extremes → highly favorable conditions
   - Negative extremes → highly unfavorable conditions
   - Weighting factor 0.7 balances peak influence without over-amplifying outliers

3. **Combined Daily Score**
   ```sql
   daily_score = 0.5 * avg + 0.5 * peak
   ```
   Produces one daily score per metric per (bird, year, location).

### Interpretation
- Values range approximately -2 to 2 (z-score scale)
- Higher values = more favorable conditions
- Captures short-term fluctuations and unusual days that may trigger biological responses

## 2. Chunk Metrics: Average + Peak

### Source Tables
- Birds: `mart_chunk_flags`
- Insects: `mart_insects_chunk_flags`

### Calculation Logic
1. Each "chunk" represents 3-4 days to capture medium-term trends
2. Daily flags are aggregated into:
   - Chunk averages: `AVG(chunk_flag)`
   - Peak-emphasis scores: Same weighted_peak formula as daily
3. Combined chunk score:
   ```sql
   chunk_score = 0.5 * avg_chunk + 0.5 * peak_chunk
   ```

### Interpretation
- Values range approximately -2 to 2 (z-score scale)
- Highlights sustained favorable/unfavorable conditions over multiple days
- Complements daily scores by smoothing out single-day noise

## 3. Period Metrics (Bird Arrival Only)

### Source Table
- `mart_period_flags`

### Characteristics
- Already aggregated over full migration period
- Uses average values only: `AVG(metric_flag)`
- Represents long-term sustained conditions during migration period

## 4. Final Score Combinations

### Bird Arrival Metrics

#### Weighting Schema

| Metric Type | Weighting | Rationale |
|------------|-----------|-----------|
| Sustained drivers (temp, sun) | 50% period, 30% chunk, 20% daily | Longer-term trends more influential than short-term spikes |
| Event drivers (wind, rain, extremes) | 30% period, 35% chunk, 35% daily | Short-term events can trigger migration; sustained trends less critical |

Implementation uses `SAFE_DIVIDE` with conditional weighting to handle missing values.

### Insect Activity Metrics

#### Weighting Schema

| Metric Type | Weighting | Rationale |
|------------|-----------|-----------|
| Temperature (tmean, tmin, tmax), anomaly, optimal days | 60% chunk, 40% daily | Sustained trends stronger but daily peaks matter for emergence |
| Other weather metrics (precip, solar, warm thresholds) | 50/50 | Balanced contribution of daily variability and multi-day trends |

## 5. Final Score Interpretation

### Scale
- Approximately -2 to 2 (z-score scale)
- High positive score: Conditions significantly more favorable than historical norms
- High negative score: Conditions significantly less favorable than historical norms

### Usage
1. **Regression Models**
   - Identify which metrics have strongest influence on migration timing or insect activity
   - Compare relative importance of different weather factors

2. **Comparative Studies**
   - Distinguish impact of short-term events vs. long-term sustained trends
   - Analyze year-over-year variations in migration patterns

### Notes
- All metrics are normalized relative to historical data
- Scores combine both magnitude and duration of favorable/unfavorable conditions
- Missing data is handled gracefully through weighted averaging
