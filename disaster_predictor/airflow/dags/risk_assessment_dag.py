"""
Risk Assessment DAG for disaster prediction.

Processes daily risk assessments using ML predictions as the sole risk score source.
Assesses risks for all regions and disaster types, saving results to risk_assessment dataset.

Schedule: Triggered by daily_ingestion completion (via Dataset); waits for ingestion on same date.
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
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


def assess_all_hazards_task(**context):
    """Task wrapper for all hazard risk assessments in a single task to save memory."""
    setup_airflow_paths()
    from risk_assessment.ml_risk_assessment import assess_daily_risks
    # Process all hazards sequentially in one task. 
    # This preloads data ONCE instead of 4 times in parallel.
    # We can use more workers here since we only have one task running.
    return assess_daily_risks(disaster_types=['fire', 'flood', 'landslide', 'drought'], max_workers=10)


def get_ingestion_run_date(logical_date, **kwargs):
    """
    Wait for daily_ingestion to have run successfully on the same date.
    Ingestion runs once per day at 09:30, so that's the run we wait for.
    """
    return logical_date.replace(hour=9, minute=30, second=0, microsecond=0)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Force DAG refresh - v3
with DAG(
    dag_id='risk_assessment',
    default_args=default_args,
    description='Daily risk assessment with ML-only scoring; skips region+date+hazard already in daily_evaluation',
    schedule=[Dataset("disaster_predictor/daily_ingestion_complete")],  # Triggered by ingestion completion
    start_date=datetime(2026, 2, 7),
    catchup=False,
    max_active_runs=1,
    tags=['risk', 'ml', 'assessment'],
) as dag:
    
    # Wait for daily_ingestion to have run successfully on the same date (ingestion runs at 09:30).
    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_daily_ingestion',
        external_dag_id='daily_ingestion',
        external_task_id='complete',
        timeout=3600,
        poke_interval=60,
        mode='reschedule',
        execution_date_fn=get_ingestion_run_date,
    )
    
    # Single risk assessment task for all hazards (sequential hazards, parallel regions)
    # This is much more memory efficient than 4 parallel tasks.
    assess_risks = PythonOperator(
        task_id='assess_all_risks',
        python_callable=assess_all_hazards_task,
    )
    
    # Define the dataset that will be updated upon completion
    assessment_complete = Dataset("disaster_predictor/risk_assessment_complete")
    
    # Final task to signal completion and update the dataset
    complete = EmptyOperator(
        task_id='complete',
        outlets=[assessment_complete],
    )
    
    # Set dependencies: wait for ingestion, then run assessments, then signal completion
    wait_for_ingestion >> assess_risks >> complete
