from dotenv import load_dotenv
from google.cloud import bigquery
import os
import json

load_dotenv()

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
GOOGLE_CREDENTIALS_PATH = os.path.abspath(
    os.path.join(BASE_DIR, "..", "birding_dbt", "config", "bq_service_account.json")
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH

def save_to_bigquery(df, dataset_id, table_id, mode='WRITE_TRUNCATE', location=None, project_id=None):
    if not project_id:
        with open(GOOGLE_CREDENTIALS_PATH, "r") as f:
            project_id = json.load(f).get("project_id")
    
    client = bigquery.Client(project=project_id)
    loc = location or os.getenv("BQ_LOCATION")
    
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=mode,
        autodetect=True,
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config, location=loc)
    result = job.result()
    
    if job.errors:
        raise RuntimeError(f"BigQuery job failed with errors: {job.errors}")
    
    return result

def load_from_bigquery(query, location=None, project_id=None):
    if not project_id:
        with open(GOOGLE_CREDENTIALS_PATH, "r") as f:
            project_id = json.load(f).get("project_id")
    
    client = bigquery.Client(project=project_id)
    loc = location or os.getenv("BQ_LOCATION")
    
    query_job = client.query(query, location=loc)
    return query_job.result().to_dataframe()