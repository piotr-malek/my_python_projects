"""
Risk Interpretation DAG for disaster prediction.

Reads daily_evaluation rows where risk_score > 1 (with recent_outlook and
forecast_outlook JSON columns). Skips assessment_ids already in weather_outlook
(risk_assessment dedupes daily_evaluation on date+region+hazard upstream).
Two LLM calls per assessment: one sentence recent, one sentence forecast. Appends
to weather_outlook. Creates weather_outlook table if missing.

Schedule: Triggered by risk_assessment DAG completion (via Dataset).
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def setup_airflow_paths():
    """
    Set up sys.path for Airflow task execution.
    Handles both production (/usr/local/airflow) and local development environments.
    Loads .env file and adds include/ to sys.path for module imports.
    """
    import sys
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    
    # Determine actual airflow root (could be /usr/local/airflow or local dev path)
    dag_dir = Path(__file__).resolve().parent
    airflow_root_dir = dag_dir.parent
    
    # Load .env file - try airflow root first (works in Docker), then project root (local dev fallback)
    for env_candidate in [airflow_root_dir / ".env", airflow_root_dir.parent / ".env"]:
        if env_candidate.is_file():
            load_dotenv(dotenv_path=env_candidate, override=True)
            break
    
    # Add airflow root to sys.path (for include/ modules and direct imports)
    airflow_root_str = str(airflow_root_dir)
    if airflow_root_str not in sys.path:
        sys.path.insert(0, airflow_root_str)
    
    # Add include/ directory to sys.path (for module imports like risk_assessment, utils, etc.)
    include_path = airflow_root_dir / 'include'
    include_path_str = str(include_path)
    if include_path.exists() and include_path_str not in sys.path:
        sys.path.insert(0, include_path_str)


def interpret_weather_outlook_task(**context):
    """Task wrapper: high-risk assessments → LLM → weather_outlook table."""
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
    schedule=[Dataset("disaster_predictor/risk_assessment_complete")],  # Triggered by assessment completion
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
