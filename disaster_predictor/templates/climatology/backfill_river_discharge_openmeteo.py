#!/usr/bin/env python3
"""
GloFAS historical backfill using Open-Meteo Flood API.
Fetches raw daily data for 1984-01-01 through 2022-07-31.

Optimizations:
- Resumable: Skips regions already in BigQuery.
- Rate Limit Aware: Handles minutely (1 req/min) and hourly/daily limits.
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env", override=False)

from utils.datasets.openmeteo_utils import fetch_openmeteo_flood_discharge
from utils.bq_utils import load_from_bigquery, save_to_bigquery, execute_sql

# Configuration
BACKFILL_START = "1984-01-01"
BACKFILL_END = "2022-07-31"
DATASET_ID = "daily_ingestion"
TABLE_ID = "openmeteo_weather"
DELAY_BETWEEN_REGIONS_SEC = 65

def ensure_bq_table(project_id: str):
    execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.{DATASET_ID}.{TABLE_ID}` (
          date DATE,
          region_name STRING,
          river_discharge FLOAT64,
          river_discharge_mean FLOAT64,
          river_discharge_median FLOAT64,
          river_discharge_max FLOAT64,
          river_discharge_min FLOAT64,
          river_discharge_p25 FLOAT64,
          river_discharge_p75 FLOAT64,
          source_system_version STRING,
          source_product_type STRING,
          created_at TIMESTAMP
        )
        CLUSTER BY region_name
        """,
        project_id=project_id,
    )

def get_existing_regions(project_id: str) -> set:
    try:
        query = f"SELECT DISTINCT region_name FROM `{project_id}.{DATASET_ID}.{TABLE_ID}`"
        df = load_from_bigquery(query, project_id=project_id)
        if df is not None and not df.empty:
            return set(df["region_name"].astype(str).tolist())
    except Exception:
        pass
    return set()

def save_region_to_bq(df: pd.DataFrame, region_name: str, project_id: str):
    df = df.copy()
    df["region_name"] = region_name
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["source_system_version"] = "openmeteo_glofas_v4"
    df["source_product_type"] = "reanalysis"
    df["created_at"] = pd.Timestamp.utcnow()
    save_to_bigquery(df, project_id=project_id, dataset_id=DATASET_ID, table_id=TABLE_ID, mode="WRITE_APPEND")

def main():
    project_id = os.getenv("PROJECT_ID")
    ensure_bq_table(project_id)
    
    from utils.earth_engine_utils import regions_openmeteo
    regions = regions_openmeteo(project_id=project_id)
    region_list = list(regions.items())
    total_regions = len(region_list)
    
    existing = get_existing_regions(project_id)
    print(f"Starting Open-Meteo backfill for {total_regions} regions.")
    if existing:
        print(f"Skipping {len(existing)} already processed regions.")
    
    t_start = time.time()
    processed_count = 0
    
    for i, (name, coords) in enumerate(region_list, 1):
        if name in existing:
            continue
            
        print(f"[{i}/{total_regions}] Processing {name} (Open-Meteo)...", end="", flush=True)
        t0 = time.time()
        try:
            df = fetch_openmeteo_flood_discharge(
                lat=coords["lat"], lon=coords["lon"],
                start_date=BACKFILL_START, end_date=BACKFILL_END
            )
            if df is None or df.empty:
                print(" ✗ [No data]")
            else:
                save_region_to_bq(df, name, project_id)
                processed_count += 1
                print(f" ✓ Finished in {time.time() - t0:.1f}s")
        except Exception as e:
            err_msg = str(e).lower()
            if "daily api request limit exceeded" in err_msg:
                print("\n✗ [DAILY LIMIT] API Limit Exceeded. Stopping.")
                break
            print(f" ✗ [ERROR]: {e}")
            
        if i < total_regions:
            time.sleep(DELAY_BETWEEN_REGIONS_SEC)
            
    print(f"\nOpen-Meteo Session complete. Processed {processed_count} regions in {(time.time() - t_start) / 3600:.2f} hours.")

if __name__ == "__main__":
    main()
