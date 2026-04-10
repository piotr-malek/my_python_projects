import os
import json
from google.oauth2 import service_account
from google.cloud import bigquery
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

KEY_PATH = str(Path(__file__).resolve().parent.parent / "config" / "service_account.json")

def save_to_bigquery(df, project_id=None, dataset_id=None, table_id=None, mode='WRITE_TRUNCATE', schema=None):
    if not project_id:
        with open(KEY_PATH, "r") as f:
            project_id = json.load(f).get("project_id")
    
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=project_id)

    destination = f"{project_id}.{dataset_id}.{table_id}"   
    dataset_ref = client.dataset(dataset_id, project=project_id)
    dataset = client.get_dataset(dataset_ref)

    loc = os.getenv("BQ_LOCATION", dataset.location)

    job_config = bigquery.LoadJobConfig(
        write_disposition=mode,
        autodetect=(schema is None),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ],
    )
    if schema is not None:
        job_config.schema = schema

    job = client.load_table_from_dataframe(
        df,
        destination,
        job_config=job_config,
        location=loc
    )
    result = job.result()
    if job.error_result or job.errors:
        raise RuntimeError(f"BigQuery load failed: {job.error_result or job.errors}")
    return result

def load_from_bigquery(query, location=None, project_id=None):
    if not project_id:
        with open(KEY_PATH, "r") as f:
            project_id = json.load(f).get("project_id")
    
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=project_id)

    loc = location or os.getenv("BQ_LOCATION")
    
    query_job = client.query(query, location=loc)
    return query_job.result().to_dataframe()

def execute_sql(sql, project_id=None, location=None):
    """Execute SQL statement (DDL/DML) in BigQuery."""
    if not project_id:
        with open(KEY_PATH, "r") as f:
            project_id = json.load(f).get("project_id")
    
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=project_id)

    loc = location or os.getenv("BQ_LOCATION")
    
    query_job = client.query(sql, location=loc)
    return query_job.result()