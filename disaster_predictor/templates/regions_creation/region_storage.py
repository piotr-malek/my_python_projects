"""
BigQuery storage operations for region creation.

This module handles saving subregions to BigQuery tables.
"""

import os
from pathlib import Path
from typing import Dict
import pandas as pd
from google.cloud import bigquery
import ee

from utils.earth_engine_utils import KEY_PATH, get_parent_region


def save_subregions_to_bq(
    subregions: Dict[str, ee.Geometry],
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> None:
    """
    Save subregion information to BigQuery.
    
    Args:
        subregions: Dict mapping subregion_id -> ee.Geometry
        project_id: BigQuery project ID (uses env var if None)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"

    print()
    print("=" * 80)
    print("SAVING TO BIGQUERY")
    print("=" * 80)

    all_data = []
    total = len(subregions)

    for i, (subregion_id, geom) in enumerate(subregions.items(), 1):
        parent_region = get_parent_region(subregion_id)
        print(f"[{i}/{total}] Processing {subregion_id} (parent: {parent_region})...")

        try:
            info = _get_geometry_info(geom)
            record = {
                "region": subregion_id,
                "parent_region": parent_region,
                "area_km2": info["area_km2"],
                "lon_min": info["lon_min"],
                "lat_min": info["lat_min"],
                "lon_max": info["lon_max"],
                "lat_max": info["lat_max"],
                "centroid_lon": info["centroid_lon"],
                "centroid_lat": info["centroid_lat"],
                "created_at": pd.Timestamp.now(),
            }
            all_data.append(record)
            print(f"  ✓ Area: {info['area_km2']:,.2f} km²")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        print()

    if not all_data:
        print("No data to save.")
        return

    df = pd.DataFrame(all_data)
    client = bigquery.Client.from_service_account_json(KEY_PATH, project=project_id)
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"

    print(f"Project: {project_id}")
    print(f"Table: {table_id_full}")
    print()

    schema = [
        bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("parent_region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("area_km2", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("lon_min", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("lat_min", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("lon_max", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("lat_max", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("centroid_lon", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("centroid_lat", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    table_ref = bigquery.TableReference.from_string(table_id_full)
    try:
        client.get_table(table_ref)
        print(f"Table {table_id_full} exists. Using WRITE_APPEND mode...")
        write_mode = "WRITE_APPEND"
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {table_id_full}")
        write_mode = "WRITE_TRUNCATE"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        create_disposition="CREATE_IF_NEEDED",
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    print()
    print("✓ Data saved successfully!")
    print(f"  Records: {len(df)}")
    print(f"  Mode: {write_mode}")
    print(f"  Table: {table_id_full}")


def _get_geometry_info(geom: ee.Geometry) -> dict:
    """Extract useful information from a geometry."""
    area_m2 = geom.area(maxError=1000).getInfo()
    area_km2 = area_m2 / 1_000_000

    bounds = geom.bounds()
    bounds_info = bounds.getInfo()["coordinates"][0]
    lons = [c[0] for c in bounds_info]
    lats = [c[1] for c in bounds_info]
    lon_min, lat_min = min(lons), min(lats)
    lon_max, lat_max = max(lons), max(lats)

    centroid = geom.centroid(maxError=1000)
    centroid_info = centroid.getInfo()
    centroid_coords = centroid_info["coordinates"]
    centroid_lon, centroid_lat = centroid_coords[0], centroid_coords[1]

    return {
        "area_km2": area_km2,
        "lon_min": lon_min,
        "lat_min": lat_min,
        "lon_max": lon_max,
        "lat_max": lat_max,
        "centroid_lon": centroid_lon,
        "centroid_lat": centroid_lat,
    }
