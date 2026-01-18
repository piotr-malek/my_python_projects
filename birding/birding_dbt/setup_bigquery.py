#!/usr/bin/env python
"""
Script to set up BigQuery datasets for the birding_dbt project.
This creates all necessary datasets in the US location.
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import os
import time

# Path to the service account key file
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
key_path = os.path.join(BASE_DIR, "..", "..", "config", "bq_service_account.json")

# Create credentials object
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Project ID from credentials
project_id = credentials.project_id
print(f"Using project ID: {project_id}")

# Create BigQuery client with explicit location
client = bigquery.Client(
    credentials=credentials,
    project=project_id,
    location="US"  # Set default location to US
)

# Define datasets to create
datasets = [
    "birding_dbt",  # Main dataset
    "birding_dbt_staging",  # Staging models
    "birding_dbt_intermediate",  # Intermediate models
    "birding_dbt_core",  # Core models
    "birding_dbt_seed"  # Seed data
]

# First, try to delete any existing datasets in EU location
print("Checking for existing datasets in EU location...")
try:
    # List all datasets
    all_datasets = list(client.list_datasets())
    for dataset in all_datasets:
        dataset_id = dataset.dataset_id
        if dataset_id in datasets:
            # Get full dataset to check location
            full_dataset = client.get_dataset(dataset_id)
            if full_dataset.location == "EU":
                print(f"Found dataset {dataset_id} in EU location. Attempting to delete...")
                try:
                    client.delete_dataset(
                        dataset_id, 
                        delete_contents=True,  # Delete tables too
                        not_found_ok=True
                    )
                    print(f"Deleted dataset {dataset_id} from EU location")
                    # Wait a moment for deletion to propagate
                    time.sleep(2)
                except Exception as e:
                    print(f"Error deleting dataset {dataset_id}: {e}")
except Exception as e:
    print(f"Error listing datasets: {e}")

# Create datasets in US location
print("Creating datasets in US location...")
for dataset_id in datasets:
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"  # Explicitly set location to US
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        # Verify the location after creation
        created_dataset = client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} created or already exists in location {created_dataset.location}")
    except Exception as e:
        print(f"Error creating dataset {dataset_id}: {e}")

print("BigQuery setup complete!")
