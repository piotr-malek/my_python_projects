#!/usr/bin/env python3
"""
Terrain static data collection for 10k km² subregions.
One-time fetch of slope, aspect, elevation from SRTM DEM for landslide risk assessment.
"""

import os
import sys
import time
from pathlib import Path

import ee
import pandas as pd
from dotenv import load_dotenv

# Project root for .env when run as __main__ or standalone
_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=_ROOT / ".env", override=False)

from ..earth_engine_utils import init_ee, KEY_PATH, get_subregions_from_bq, get_info_with_timeout
from ..bq_utils import save_to_bigquery
from ..incremental_save_utils import (
    SLEEP_BETWEEN_SUBREGIONS,
    process_with_incremental_save,
)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "climatology"
TABLE_ID = "terrain_static"

SRTM_DEM_ID = "USGS/SRTMGL1_003"


def compute_terrain_stats(geom: ee.Geometry, scale_m: int = 90) -> dict:
    """
    Compute terrain statistics from SRTM DEM.

    Uses a single getInfo() on the full reduceRegion result to avoid timeouts and
    null returns from multiple separate requests. Scale 90m keeps pixel count
    manageable for ~10k km² regions while preserving useful resolution.

    Args:
        geom: Earth Engine geometry
        scale_m: Scale in meters (90m default for reliable reduceRegion; SRTM is 30m)

    Returns:
        Dictionary with terrain statistics
    """
    dem = ee.Image(SRTM_DEM_ID).select("elevation")
    slope = ee.Terrain.slope(dem)
    aspect = ee.Terrain.aspect(dem)
    combined = dem.addBands([slope, aspect]).rename(["elevation", "slope", "aspect"])
    stats = combined.reduceRegion(
        reducer=ee.Reducer.mean()
        .combine(ee.Reducer.stdDev(), sharedInputs=True)
        .combine(ee.Reducer.minMax(), sharedInputs=True)
        .combine(ee.Reducer.percentile([25, 50, 75]), sharedInputs=True),
        geometry=geom,
        scale=scale_m,
        bestEffort=True,
        maxPixels=1_000_000_000,
        tileScale=8,
    )
    raw = get_info_with_timeout(stats, timeout_seconds=120)
    if raw is None:
        return {}
    result = {}
    for metric in ["elevation", "slope", "aspect"]:
        for stat_name in ["mean", "stdDev", "min", "max", "p25", "p50", "p75"]:
            key = f"{metric}_{stat_name}"
            result[key] = raw.get(key)
    return result


def fetch_terrain_data(
    geom: ee.Geometry, start_date: str = None, end_date: str = None, **kwargs
) -> pd.DataFrame:
    """Wrapper for compute_terrain_stats to match fetch_function signature."""
    stats = compute_terrain_stats(geom, scale_m=90)
    if not stats or not any(v is not None for v in stats.values()):
        return pd.DataFrame()
    return pd.DataFrame([stats])


def fetch_all_subregions_terrain(
    project_id: str = None,
    dataset_id: str = None,
    table_id: str = None,
) -> pd.DataFrame:
    """
    Fetch terrain statistics for all 10k km² subregions.

    Args:
        project_id: GCP project ID (if provided, uses incremental saves)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID

    Returns:
        DataFrame with terrain statistics for all subregions
    """
    print("=" * 80)
    print("TERRAIN STATIC DATA COLLECTION FOR 10k km² SUBREGIONS")
    print("=" * 80)
    print("Source: SRTM DEM 30m (USGS/SRTMGL1_003)")
    print("Metrics: Elevation, Slope, Aspect (mean, stddev, min, max, percentiles)")
    print("Use case: Landslide susceptibility assessment")
    print()

    print("Loading subregions from BigQuery...")
    subregions = get_subregions_from_bq()
    print(f"Total subregions loaded: {len(subregions)}")
    print()

    if project_id and dataset_id and table_id:
        print("Using incremental save mode...")
        return process_with_incremental_save(
            subregions=subregions,
            fetch_function=fetch_terrain_data,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            start_date="2000-01-01",
            end_date="2000-01-02",
            skip_existing=False,
        )

    print("Using legacy collection mode (no incremental saves)")
    records = []
    total = len(subregions)

    for i, (subregion_id, geom) in enumerate(subregions.items(), 1):
        parent_region = (
            subregion_id.rsplit("_", 1)[0] if "_" in subregion_id else subregion_id
        )
        print(f"[{i}/{total}] Processing {subregion_id} (parent: {parent_region})...")
        try:
            stats = compute_terrain_stats(geom, scale_m=90)
            stats["region"] = subregion_id
            records.append(stats)
            slope_mean = stats.get("slope_mean", 0)
            elev_mean = stats.get("elevation_mean", 0)
            print(f"  ✓ Slope: {slope_mean:.1f}°, Elevation: {elev_mean:.0f}m")
        except Exception as e:
            print(f"  ✗ Error: {e}")
            records.append({"region": subregion_id})
        if i < total:
            time.sleep(SLEEP_BETWEEN_SUBREGIONS)

    df = pd.DataFrame(records)
    print()
    print(f"Data collection complete. Total records: {len(df)}")
    return df


if __name__ == "__main__":
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))

    print("Initializing Earth Engine...")
    init_ee(KEY_PATH)
    print("Earth Engine initialized.")
    print()

    df = fetch_all_subregions_terrain()

    if df is not None and not df.empty:
        print()
        print("=" * 80)
        print("SAVING TO BIGQUERY")
        print("=" * 80)
        print(f"Dataset: {DATASET_ID}")
        print(f"Table: {TABLE_ID}")
        print(f"Records: {len(df)}")
        print()
        save_to_bigquery(df, PROJECT_ID, DATASET_ID, TABLE_ID, mode="WRITE_TRUNCATE")
        print()
        print("✓ Data saved successfully!")
    else:
        print("No data to save.")
