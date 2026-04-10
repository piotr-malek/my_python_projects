#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.bq_utils import load_from_bigquery
from config import get_project_id

PROJECT_ID = get_project_id()
DATASET_ID = "daily_ingestion"


def get_latest_assessments(project_id: str = None, dataset_id: str = None, date: pd.Timestamp = None):
    if project_id is None:
        project_id = PROJECT_ID
    if dataset_id is None:
        dataset_id = DATASET_ID
    
    date_filter = ""
    if date is not None:
        date_filter = f"WHERE date = DATE('{date.strftime('%Y-%m-%d')}')"
    
    query = f"""
    WITH daily_assessments AS (
      SELECT 
        date, 
        region, 
        disaster_type, 
        risk_score, 
        risk_level, 
        risk_changed,
        created_at
      FROM `{project_id}.{dataset_id}.risk_assessments`
      {date_filter}
    ),
    lagged AS (
      SELECT *,
        LAG(risk_score) OVER (PARTITION BY region, disaster_type ORDER BY created_at) as prev_risk_score
      FROM daily_assessments
    ),
    with_alert_status AS (
      SELECT *, 
        CASE
          WHEN prev_risk_score IS NULL AND risk_score >= 2 THEN 'alert_raised'
          WHEN prev_risk_score < 2 AND risk_score >= 2 THEN 'alert_raised'
          WHEN prev_risk_score >= 2 AND risk_score < 2 THEN 'alert_removed'
          WHEN prev_risk_score >= 2 AND risk_score >= 2 THEN 'alert_remains'
          ELSE 'no_alert'
        END as alert_status
      FROM lagged
    )
    SELECT * 
    FROM with_alert_status
    """
    
    if date is None:
        # Get latest assessment run (by created_at, not date)
        # This ensures we get the most recent assessment even if we reassessed an older date
        query = query + f"""
    WHERE created_at = (SELECT MAX(created_at) FROM `{project_id}.{dataset_id}.risk_assessments`)
        """
    
    query = query + """
    ORDER BY region, disaster_type
    """
    
    try:
        return load_from_bigquery(query)
    except Exception as e:
        # Handle first run - table doesn't exist yet
        if "not found" in str(e).lower() or "does not exist" in str(e).lower():
            print(f"Table {project_id}.{dataset_id}.risk_assessments does not exist yet (first run)")
            return pd.DataFrame()
        raise


def get_assessments_for_llm(project_id: str = None, dataset_id: str = None, date: pd.Timestamp = None):
    """
    Get assessments that should be sent to LLM for interpretation.
    
    LLM should be called when:
    - risk_changed=True AND risk_score >= 2 (risk increased to medium/high - alert)
    - risk_changed=True AND risk_score < 2 AND prev_risk_score >= 2 (risk decreased from medium/high - good news)
    
    Returns:
        DataFrame filtered to only assessments that need LLM interpretation
    """
    df = get_latest_assessments(project_id, dataset_id, date)
    
    if df is None or df.empty:
        return pd.DataFrame()
    
    # Filter for LLM-worthy assessments
    llm_filter = (
        (df['risk_changed'] == True) & 
        (
            (df['risk_score'] >= 2) |  # Increased to medium/high
            ((df['risk_score'] < 2) & (df['prev_risk_score'] >= 2))  # Decreased from medium/high
        )
    )
    
    return df[llm_filter].copy()


if __name__ == "__main__":
    # Test query
    print("Testing risk assessment queries...")
    df = get_latest_assessments()
    if df is not None and not df.empty:
        print(f"Retrieved {len(df)} assessments")
        print(f"\nSample data:")
        print(df[['date', 'region', 'disaster_type', 'risk_level', 'alert_status']].head())
        
        llm_df = get_assessments_for_llm()
        print(f"\nAssessments for LLM: {len(llm_df)}")
        if not llm_df.empty:
            print(llm_df[['region', 'disaster_type', 'risk_level', 'alert_status']].head())
    else:
        print("No assessments found")
