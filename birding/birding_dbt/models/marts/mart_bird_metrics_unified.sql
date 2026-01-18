{{ config(materialized='table') }}

SELECT 
    b.*,
    i.insect_presence_score
FROM {{ ref('mart_arrival_metrics_condensed') }} b
LEFT JOIN {{ ref('mart_insect_scores_unified') }} i USING(bird, arrival_year, location_name)