#!/usr/bin/env python3
"""
GloFAS historical backfill using Copernicus EWDS API.
Fetches daily data for 1984-01-01 through 2022-07-31 using yearly super-clusters.

Optimizations:
- Resumable: Skips regions already in BigQuery (river_discharge_daily_alt).
- Super-Clustering: Groups regions into 3 large blocks to minimize queue wait time.
- Yearly Fetching: Fetches a full year at once for efficiency.
"""

import os
import sys
import time
import tempfile
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env", override=False)

from utils.bq_utils import load_from_bigquery, save_to_bigquery, execute_sql

# Configuration
BACKFILL_START_YEAR = 1984
BACKFILL_END_YEAR = 2022
DATASET_ID = "daily_ingestion"
TABLE_ID = "river_discharge_daily_alt"
HYDROLOGICAL_MODEL = "lisflood"
SYSTEM_VERSION = "version_4_0"
PRODUCT_TYPE = "consolidated"
VARIABLES = ["river_discharge_in_the_last_24_hours"]

def ensure_bq_table(project_id: str):
    execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.{DATASET_ID}.{TABLE_ID}` (
          date DATE,
          region STRING,
          river_discharge FLOAT64,
          source_system_version STRING,
          source_product_type STRING,
          created_at TIMESTAMP
        )
        CLUSTER BY region
        """,
        project_id=project_id,
    )

def get_existing_data_map(project_id: str) -> Dict[str, set]:
    """Returns a map of region -> set of years already in BQ."""
    try:
        query = f"SELECT region, EXTRACT(YEAR FROM date) as year FROM `{project_id}.{DATASET_ID}.{TABLE_ID}` GROUP BY 1, 2"
        df = load_from_bigquery(query, project_id=project_id)
        if df is not None and not df.empty:
            res = {}
            for _, row in df.iterrows():
                r = str(row["region"])
                y = int(row["year"])
                if r not in res: res[r] = set()
                res[r].add(y)
            return res
    except Exception:
        pass
    return {}

def aggregate_to_regions(nc_path: Path, regions: List[Dict[str, Any]]):
    ds = xr.open_dataset(nc_path)
    var_name = "dis24" if "dis24" in ds.data_vars else "river_discharge_in_the_last_24_hours"
    
    lat_name = "latitude" if "latitude" in ds.coords else "lat"
    lon_name = "longitude" if "longitude" in ds.coords else "lon"
    time_name = "valid_time" if "valid_time" in ds.coords else "time"

    recs = []
    for region in regions:
        region_name = region["region"]
        lat_min, lat_max = region["lat_min"], region["lat_max"]
        lon_min, lon_max = region["lon_min"], region["lon_max"]

        lat_slice = slice(lat_max, lat_min) if ds[lat_name][0] > ds[lat_name][-1] else slice(lat_min, lat_max)
        
        try:
            sub = ds.sel({lat_name: lat_slice, lon_name: slice(lon_min, lon_max)})
            if sub[var_name].size == 0:
                sub = ds.sel({lat_name: (lat_min + lat_max)/2, lon_name: (lon_min + lon_max)/2}, method="nearest")
        except:
            sub = ds.sel({lat_name: (lat_min + lat_max)/2, lon_name: (lon_min + lon_max)/2}, method="nearest")

        region_ds = sub[var_name].mean(dim=[lat_name, lon_name]) if lat_name in sub.dims else sub[var_name]
        df_region = region_ds.to_dataframe(name="river_discharge").reset_index()
        df_region["region"] = region_name
        recs.append(df_region)

    out = pd.concat(recs, ignore_index=True)
    out = out.rename(columns={time_name: "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out

def process_task(task: Dict[str, Any], project_id: str, client: Any, tmp_dir: Path):
    hyear = task["year"]
    cluster = task["cluster"]
    idx = task["idx"]
    num_super = task["num_super"]
    
    print(f"Processing Year {hyear}, Cluster {idx+1}/{num_super} ({len(cluster)} regions)...", flush=True)
    t0 = time.time()
    
    lats = [r["lat_min"] for r in cluster] + [r["lat_max"] for r in cluster]
    lons = [r["lon_min"] for r in cluster] + [r["lon_max"] for r in cluster]
    area = [max(lats) + 0.5, min(lons) - 0.5, min(lats) - 0.5, max(lons) + 0.5]

    request = {
        "variable": VARIABLES,
        "hydrological_model": [HYDROLOGICAL_MODEL],
        "product_type": [PRODUCT_TYPE],
        "system_version": [SYSTEM_VERSION],
        "hyear": [str(hyear)],
        "hmonth": [f"{m:02d}" for m in range(1, 13)],
        "hday": [f"{d:02d}" for d in range(1, 32)],
        "area": area,
        "data_format": ["netcdf"],
        "download_format": ["unarchived"],
    }

    try:
        target = tmp_dir / f"copernicus_{hyear}_{idx}.nc"
        remote = client.submit("cems-glofas-historical", request)
        results = client.get_results(remote.request_id)
        results.download(str(target))
        
        df = aggregate_to_regions(target, cluster)
        df["source_system_version"] = f"copernicus_glofas_{SYSTEM_VERSION}"
        df["source_product_type"] = PRODUCT_TYPE
        df["created_at"] = pd.Timestamp.utcnow()
        
        # Ensure only expected columns are sent to BigQuery
        expected_cols = ["date", "region", "river_discharge", "source_system_version", "source_product_type", "created_at"]
        df = df[expected_cols]
        
        save_to_bigquery(df, project_id, DATASET_ID, TABLE_ID, mode="WRITE_APPEND")
        if target.exists():
            target.unlink()
        print(f" ✓ Finished Year {hyear}, Cluster {idx+1} in {time.time() - t0:.1f}s", flush=True)
    except Exception as e:
        print(f" ✗ [ERROR] Year {hyear}, Cluster {idx+1}: {e}", flush=True)
        time.sleep(60)

def main():
    project_id = os.getenv("PROJECT_ID")
    ensure_bq_table(project_id)
    
    # Get bounding boxes for aggregation
    query = f"SELECT region, lon_min, lat_min, lon_max, lat_max FROM `{project_id}.google_earth.regions_info`"
    bbox_df = load_from_bigquery(query, project_id=project_id)
    regions_info = bbox_df.to_dict('records')
    
    existing_map = get_existing_data_map(project_id)
    
    # Identify missing (region, year) pairs
    missing_pairs = []
    for r in regions_info:
        for hyear in range(BACKFILL_START_YEAR, BACKFILL_END_YEAR + 1):
            if r["region"] not in existing_map or hyear not in existing_map[r["region"]]:
                missing_pairs.append((r, hyear))
    
    if not missing_pairs:
        print("All regions and years already processed in Copernicus table.")
        return

    # Group missing pairs by Year to minimize Copernicus requests
    # Within each year, we still cluster regions to avoid fetching the whole world if only a few regions are missing.
    year_to_missing_regions = {}
    for r, hyear in missing_pairs:
        if hyear not in year_to_missing_regions:
            year_to_missing_regions[hyear] = []
        year_to_missing_regions[hyear].append(r)

    tasks = []
    num_super = 3
    for hyear, regions in year_to_missing_regions.items():
        # Split the missing regions for THIS year into 3 clusters
        clusters = [regions[i::num_super] for i in range(num_super)]
        for idx, cluster in enumerate(clusters):
            if not cluster: continue
            tasks.append({
                "year": hyear,
                "cluster": cluster,
                "idx": idx,
                "num_super": num_super
            })

    print(f"Starting Copernicus backfill with 3 workers. Total tasks: {len(tasks)}")
    
    token = os.getenv("COPERNICUS_TOKEN")
    from ecmwf.datastores import Client
    client = Client(url="https://ewds.climate.copernicus.eu/api", key=token)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(lambda t: process_task(t, project_id, client, tmp_dir), tasks))

    print("\nCopernicus Session complete.")

if __name__ == "__main__":
    main()
