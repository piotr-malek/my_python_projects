"""
Daily Ingestion DAG for disaster prediction data.

Fetches recent data from multiple sources and stores in daily_ingestion dataset:
- ERA5: Weather data (32 days) — fire, drought, flood, landslide
- VIIRS: Fire hotspot data (9 days) — fire labels
- Landsat: NDVI data (32 days, 16-day periods) — fire, drought
- OpenMeteo: Archive weather plus GloFAS river_discharge on openmeteo_weather; 7d forecast plus GloFAS on openmeteo_forecast — drought, backup, flood context
- River discharge: Flood API (30d + 7d) merged into openmeteo_weather and openmeteo_forecast — flood, landslide modifiers/outlook

Schedule: Daily at 09:30 UTC
"""

from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add parent directory and include/ to path for imports (include/ needed for daily_ingestion at parse time)
_dag_dir = Path(__file__).resolve().parent
_airflow_root = _dag_dir.parent
sys.path.insert(0, str(_airflow_root))
if (_airflow_root / "include").exists():
    sys.path.insert(0, str(_airflow_root / "include"))


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
    
    # Add include/ directory to sys.path (for module imports like daily_ingestion, utils, etc.)
    include_path = airflow_root_dir / 'include'
    include_path_str = str(include_path)
    if include_path.exists() and include_path_str not in sys.path:
        sys.path.insert(0, include_path_str)


def log_task_progress(task_label: str, idx: int, total: int) -> None:
    print(f"[{idx:02d}/{total:02d}] {task_label}")


def _merge_context_and_kwargs(*args, **kwargs):
    """Merge Airflow context (if passed as first arg) with op_kwargs so chunk_index/total_chunks are always forwarded. Works with Astro/Docker and standard Airflow."""
    merged = dict(kwargs)
    if args and isinstance(args[0], dict):
        merged = {**args[0], **merged}
    return merged


def fetch_era5_task(*args, **kwargs):
    """Task wrapper for ERA5 data fetching (supports chunk_index/total_chunks for chunked runs). Compatible with Astro/Docker Airflow."""
    setup_airflow_paths()
    from daily_ingestion.fetch_data import fetch_era5_data
    return fetch_era5_data(**_merge_context_and_kwargs(*args, **kwargs))


def fetch_viirs_task(*args, **kwargs):
    """Task wrapper for VIIRS data fetching."""
    setup_airflow_paths()
    from daily_ingestion.fetch_data import fetch_viirs_data
    return fetch_viirs_data(**_merge_context_and_kwargs(*args, **kwargs))


def fetch_landsat_task(*args, **kwargs):
    """Task wrapper for Landsat data fetching (supports chunk_index/total_chunks for chunked runs). Compatible with Astro/Docker Airflow."""
    setup_airflow_paths()
    from daily_ingestion.fetch_data import fetch_landsat_data
    return fetch_landsat_data(**_merge_context_and_kwargs(*args, **kwargs))


def fetch_openmeteo_task(*args, **kwargs):
    """Task wrapper for OpenMeteo data fetching."""
    setup_airflow_paths()
    from daily_ingestion.fetch_data import fetch_openmeteo_data
    return fetch_openmeteo_data(**_merge_context_and_kwargs(*args, **kwargs))


def truncate_openmeteo_forecast_task(*args, **kwargs):
    """Truncate the openmeteo_forecast table before chunked ingestion starts."""
    setup_airflow_paths()
    from utils.bq_utils import execute_sql, load_from_bigquery
    from config import get_project_id
    project_id = get_project_id()
    dataset_id = "daily_ingestion"
    table_id = "openmeteo_forecast"
    
    try:
        df = load_from_bigquery(f"SELECT COUNT(*) as cnt FROM `{project_id}.{dataset_id}.{table_id}`", project_id=project_id)
        count = df.iloc[0]['cnt'] if not df.empty else 0
        print(f"Table {project_id}.{dataset_id}.{table_id} currently has {count} rows.")
    except Exception as e:
        print(f"Could not get row count (table might not exist yet): {e}")

    sql = f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table_id}`"
    print(f"Executing: {sql}")
    execute_sql(sql, project_id=project_id)
    print(f"Successfully truncated {project_id}.{dataset_id}.{table_id}.")
    return True


# Chunked ingestion: number of parallel tasks per source (from fetch_data)
try:
    from daily_ingestion.fetch_data import (
        INGESTION_ERA5_CHUNKS,
        INGESTION_LANDSAT_CHUNKS,
        INGESTION_OPENMETEO_CHUNKS,
        INGESTION_VIIRS_CHUNKS
    )
except ImportError:
    INGESTION_ERA5_CHUNKS = 5
    INGESTION_LANDSAT_CHUNKS = 5
    INGESTION_OPENMETEO_CHUNKS = 5
    INGESTION_VIIRS_CHUNKS = 5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_ingestion',
    default_args=default_args,
    description='Daily data ingestion for disaster prediction (ERA5, VIIRS, Landsat, OpenMeteo)',
    schedule='30 9 * * *',  # Daily at 09:30 UTC
    start_date=datetime(2026, 2, 7),
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'data'],
) as dag:
    # ERA5: chunked tasks (each appends to same BQ table)
    fetch_era5_tasks = [
        PythonOperator(
            task_id=f'fetch_era5_chunk_{i}',
            python_callable=fetch_era5_task,
            op_kwargs={'chunk_index': i, 'total_chunks': INGESTION_ERA5_CHUNKS},
        )
        for i in range(INGESTION_ERA5_CHUNKS)
    ]
    # VIIRS: chunked tasks
    fetch_viirs_tasks = [
        PythonOperator(
            task_id=f'fetch_viirs_chunk_{i}',
            python_callable=fetch_viirs_task,
            op_kwargs={'chunk_index': i, 'total_chunks': INGESTION_VIIRS_CHUNKS},
        )
        for i in range(INGESTION_VIIRS_CHUNKS)
    ]
    # Landsat: chunked tasks
    fetch_landsat_tasks = [
        PythonOperator(
            task_id=f'fetch_landsat_chunk_{i}',
            python_callable=fetch_landsat_task,
            op_kwargs={'chunk_index': i, 'total_chunks': INGESTION_LANDSAT_CHUNKS},
        )
        for i in range(INGESTION_LANDSAT_CHUNKS)
    ]
    # OpenMeteo: chunked tasks
    truncate_openmeteo_forecast = PythonOperator(
        task_id='truncate_openmeteo_forecast',
        python_callable=truncate_openmeteo_forecast_task,
    )
    fetch_openmeteo_tasks = [
        PythonOperator(
            task_id=f'fetch_openmeteo_chunk_{i}',
            python_callable=fetch_openmeteo_task,
            op_kwargs={'chunk_index': i, 'total_chunks': INGESTION_OPENMETEO_CHUNKS},
        )
        for i in range(INGESTION_OPENMETEO_CHUNKS)
    ]
    truncate_openmeteo_forecast >> fetch_openmeteo_tasks
    ingestion_complete = Dataset("disaster_predictor/daily_ingestion_complete")
    complete = EmptyOperator(
        task_id='complete',
        outlets=[ingestion_complete],
    )
    all_fetch_tasks = fetch_era5_tasks + fetch_viirs_tasks + fetch_landsat_tasks + fetch_openmeteo_tasks
    all_fetch_tasks >> complete
