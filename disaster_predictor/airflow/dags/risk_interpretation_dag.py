"""
Risk Interpretation DAG for disaster prediction.

Reads daily_evaluation rows where risk_score > 1 (with recent_outlook and
forecast_outlook JSON columns). Skips assessment_ids already in weather_outlook
(risk_assessment dedupes daily_evaluation on date+region+hazard upstream).
Two LLM calls per assessment: one sentence recent, one sentence forecast. Appends
to weather_outlook. Creates weather_outlook table if missing.

Schedule: None (triggered by run_daily.py after job_digest; was Dataset on risk_assessment_complete).
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add airflow root and dags/ to path for imports
_dags_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(_dags_dir.parent))
sys.path.insert(0, str(_dags_dir))


def interpret_weather_outlook_task(**context):
    """Task wrapper: high-risk assessments → LLM → weather_outlook table."""
    from path_setup import setup_airflow_paths
    setup_airflow_paths()
    from risk_assessment.weather_outlook_interpretation import process_weather_outlook
    process_weather_outlook()




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # More retries for LLM processing (can be flaky)
    'retry_delay': timedelta(minutes=5),
}

# Force DAG refresh - v2
with DAG(
    dag_id='risk_interpretation',
    default_args=default_args,
    description='LLM narrative for ML-only risk scores using recent and forecast outlook context',
    schedule=None,  # Manual trigger via run_daily.py (after job_digest, post risk_assessment)
    start_date=datetime(2026, 2, 7),
    catchup=False,
    max_active_runs=1,
    tags=['risk', 'llm', 'interpretation'],
) as dag:
    
    # Weather outlook interpretation: daily_evaluation (risk_score > 1) → LLM → weather_outlook
    interpret_weather_outlook = PythonOperator(
        task_id='interpret_weather_outlook',
        python_callable=interpret_weather_outlook_task,
    )
    
    # Define the dataset that will be updated upon completion
    interpretation_complete = Dataset("disaster_predictor/risk_interpretation_complete")
    
    # Final task to signal completion and update the dataset
    complete = EmptyOperator(
        task_id='complete',
        outlets=[interpretation_complete],
    )
    
    # Set dependencies: run interpretation, then signal completion
    interpret_weather_outlook >> complete
