-- Query to retrieve risk assessments with alert status and context for LLM interpretation
-- Returns data for the latest assessment date (typically yesterday)

WITH daily_assessments AS (
  SELECT 
    date, 
    region, 
    disaster_type, 
    risk_score, 
    risk_level, 
    risk_changed,
    last_risk_change_date, 
    last_risk_change_from,
    contributing_factors, 
    key_metrics
  FROM `{project_id}.{dataset_id}.risk_assessments`
),
lagged AS (
  SELECT *,
    LAG(risk_score) OVER (PARTITION BY region, disaster_type ORDER BY date) as prev_risk_score
  FROM daily_assessments
),
with_alert_status AS (
  SELECT *, 
    CASE
      WHEN prev_risk_score IS NULL AND risk_score >= 2 THEN 'alert_raised'  -- First run with medium/high risk
      WHEN prev_risk_score < 2 AND risk_score >= 2 THEN 'alert_raised'
      WHEN prev_risk_score >= 2 AND risk_score < 2 THEN 'alert_removed'
      WHEN prev_risk_score >= 2 AND risk_score >= 2 THEN 'alert_remains'
      ELSE 'no_alert'
    END as alert_status,
    CASE 
      WHEN risk_changed THEN NULL
      ELSE CONCAT(
        'Risk level ', risk_score, ' (', risk_level, ') maintained since ', 
        CAST(last_risk_change_date AS STRING), 
        ', previously was ', 
        COALESCE(CAST(prev_risk_score AS STRING), 'N/A'),
        CASE 
          WHEN prev_risk_score = 0 THEN ' (no_risk)'
          WHEN prev_risk_score = 1 THEN ' (low)'
          WHEN prev_risk_score = 2 THEN ' (medium)'
          WHEN prev_risk_score = 3 THEN ' (high)'
          ELSE ''
        END
      )
    END as stability_context
  FROM lagged
)
SELECT * 
FROM with_alert_status
WHERE date = (SELECT MAX(date) FROM `{project_id}.{dataset_id}.risk_assessments`)
ORDER BY region, disaster_type
