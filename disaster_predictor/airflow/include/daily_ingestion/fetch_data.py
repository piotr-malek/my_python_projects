#!/usr/bin/env python3
"""
Daily data fetching functions for ingestion DAG.
Fetches ERA5, VIIRS, Landsat, and OpenMeteo data for the daily_ingestion dataset.
"""

import os
import sys
import datetime
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import pandas as pd

# Suppress pandas FutureWarnings about concatenation
warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')

# Suppress Earth Engine warnings
os.environ['GLOG_minloglevel'] = '2'  # Suppress Earth Engine INFO/WARNING logs

# Load environment variables - look in project root
project_root = Path(__file__).resolve().parent.parent.parent.parent
load_dotenv(dotenv_path=project_root / ".env", override=False)

# Repo root (utils, config) + airflow/include (daily_ingestion package)
_include_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(_include_path))

from utils.bq_utils import save_to_bigquery, load_from_bigquery, execute_sql
from utils.earth_engine_utils import init_ee, KEY_PATH, regions_ee, regions_openmeteo
# Import daily_utils
from daily_ingestion.daily_utils import (
    get_era5_safe_date, get_viirs_safe_date,
    get_landsat_safe_date, format_date,
    calculate_fetch_date_range, log_fetch_range,
)
from utils.datasets.era5_utils import fetch_era5_daily
from utils.datasets.viirs_utils import fetch_all_regions_viirs, fetch_viirs_daily
from utils.datasets.landsat_utils import fetch_landsat_daily_ndvi, aggregate_landsat_to_16day
from utils.datasets.openmeteo_utils import (
    sync_openmeteo_all_regions,
    fetch_openmeteo_forecast,
    fetch_openmeteo_historical,
    fetch_openmeteo_forecast_hourly_with_soil_moisture,
    fetch_openmeteo_flood_discharge,
    fetch_openmeteo_historical_batch,
    fetch_openmeteo_forecast_batch,
    fetch_openmeteo_flood_discharge_batch,
    fetch_openmeteo_forecast_hourly_batch,
)
from config import get_project_id

PROJECT_ID = get_project_id()
DATASET_ID = "daily_ingestion"
# Copernicus GloFAS historical in BigQuery (strict fallback when Open-Meteo Flood API fails)
GLOFAS_BQ_DATASET = "climatology"
GLOFAS_BQ_TABLE = "copernicus_glofas"
BACKFILL_DISCHARGE_START = datetime.date(2024, 1, 1)

# In-task parallelism: tuned for GEE quota (40 concurrent requests per project, see
# https://developers.google.com/earth-engine/guides/usage). With 5 ERA5 + 5 Landsat
# chunk tasks running in parallel, total concurrent = 5*ERA5_MAX_WORKERS + 5*LANDSAT_MAX_WORKERS
# must stay <= 40. Using 4+3 per source keeps 5*4+5*3 = 35.
ERA5_MAX_WORKERS = 4
LANDSAT_MAX_WORKERS = 3
OPENMETEO_FORECAST_MAX_WORKERS = 2
VIIRS_MAX_WORKERS = 4


# Chunked ingestion: number of parallel tasks per source (B).
INGESTION_ERA5_CHUNKS = 5
INGESTION_LANDSAT_CHUNKS = 5
INGESTION_OPENMETEO_CHUNKS = 5
INGESTION_VIIRS_CHUNKS = 5



def _dedupe_by_keys(df: pd.DataFrame, key_cols: list) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.drop_duplicates(subset=key_cols, keep='last')


def get_latest_bq_date(table_name: str) -> datetime.date:
    """Get the latest date from BigQuery for a table."""
    try:
        query = f"SELECT MAX(DATE(date)) AS max_date FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        df = load_from_bigquery(query, project_id=PROJECT_ID)
        if df is not None and not df.empty and len(df) > 0:
            max_date = df["max_date"].iloc[0]
            if pd.notna(max_date) and max_date is not None:
                # Convert to date if it's a datetime or Timestamp
                if isinstance(max_date, (pd.Timestamp, datetime.datetime)):
                    return max_date.date()
                elif isinstance(max_date, datetime.date):
                    return max_date
    except Exception as e:
        # Table might not exist yet, which is fine
        pass
    return None


def get_landsat_period_start(target_date: datetime.date) -> datetime.date:
    """Calculate the 16-day period start date for a given date (Landsat aggregation)."""
    # Periods start from Jan 1 of the year
    year_start = datetime.date(target_date.year, 1, 1)
    days_since_year_start = (target_date - year_start).days
    period_offset = (days_since_year_start // 16) * 16
    return year_start + datetime.timedelta(days=period_offset)


def check_landsat_period_available(target_date: datetime.date) -> bool:
    """Check if we have a 16-day period in BQ that covers the target date."""
    try:
        # Calculate which period the target date falls into
        period_start = get_landsat_period_start(target_date)
        period_start_str = format_date(period_start)
        
        # Check if we have this period (or a later one) in BQ
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `{PROJECT_ID}.{DATASET_ID}.landsat`
        WHERE DATE(date) >= DATE('{period_start_str}')
        """
        df = load_from_bigquery(query, project_id=PROJECT_ID)
        if df is not None and not df.empty and len(df) > 0:
            count = df["cnt"].iloc[0]
            return count > 0
    except Exception as e:
        # Table might not exist yet, or query failed
        pass
    return False


def fetch_era5_data(**context):
    """Fetch ERA5 data for last 32 days. Flood/landslide use river_discharge from openmeteo_weather."""
    print("\n=== Fetching ERA5 Data (32 days) ===")
    
    # Initialize Earth Engine before any safe-date checks so GEE queries work correctly
    try:
        init_ee(KEY_PATH)
    except Exception:
        # If already initialized or running in an environment without EE, proceed;
        # safe-date helpers will fall back gracefully if needed.
        pass
    
    # Calculate date range using shared utility (uses GEE via safe-date helpers)
    safe_date_str = get_era5_safe_date()
    start_date, end_date, skip_fetch, latest_bq_date, target_date, safe_date = calculate_fetch_date_range(
        table_name="era5",
        lookback_days=32,
        get_latest_bq_date_fn=get_latest_bq_date,
        safe_date_str=safe_date_str
    )
    
    log_fetch_range(
        "ERA5",
        safe_date_str,
        latest_bq_date,
        start_date,
        end_date,
        skip_fetch,
        skip_reason="Skipping fetch - no new data available from GEE" if skip_fetch else None,
    )
    if skip_fetch:
        return True

    # Resolve region list: optional chunk (for chunked DAG tasks) or all regions
    all_regions = regions_ee()
    chunk_index = context.get("chunk_index") if isinstance(context, dict) else None
    total_chunks = context.get("total_chunks") if isinstance(context, dict) else None
    if chunk_index is not None and total_chunks is not None and total_chunks > 0:
        sorted_regions = sorted(all_regions.keys())
        n = len(sorted_regions)
        chunk_size = (n + total_chunks - 1) // total_chunks
        start = chunk_index * chunk_size
        end = min(start + chunk_size, n)
        regions_to_fetch = sorted_regions[start:end]
        print(f"ERA5 chunk {chunk_index + 1}/{total_chunks}: {len(regions_to_fetch)} regions")
    else:
        regions_to_fetch = [r for r in all_regions.keys() if r in all_regions]
    n_regions = len(regions_to_fetch)
    workers = min(ERA5_MAX_WORKERS, n_regions) if n_regions else 1
    print(f"Fetching ERA5 for {n_regions} region(s) with {workers} worker(s)")

    def _fetch_era5_one(region_name):
        geom = all_regions[region_name]
        df = fetch_era5_daily(geom, format_date(start_date), format_date(end_date), include_runoff=False)
        if df is not None and not df.empty:
            df = df.copy()
            df['region'] = region_name
            return (region_name, df)
        return (region_name, None)

    frames = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_region = {executor.submit(_fetch_era5_one, r): r for r in regions_to_fetch}
        for idx, future in enumerate(as_completed(future_to_region), 1):
            region_name = future_to_region[future]
            try:
                _, df = future.result()
                if df is not None:
                    frames.append(df)
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✓")
                else:
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗")
            except Exception as e:
                print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗ {e}")

    if not frames:
        print("✗ No ERA5 data fetched")
        return False
    
    # Filter out any empty DataFrames before concatenation to avoid warnings
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        print("✗ No ERA5 data fetched")
        return False
    
    df = pd.concat(frames, ignore_index=True).sort_values(['region', 'date'])
    df = _dedupe_by_keys(df, ['date', 'region'])
    
    if df is not None and not df.empty:
        # Ensure we have all required columns
        required_cols = ['date', 'region', 'temp_2m_mean_C', 'precipitation_sum_mm', 
                        'sm1_mean', 'sm2_mean']
        
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.NA
        
        print(f"  Saving to BigQuery...", end=" ", flush=True)
        save_start = time.time()
        save_to_bigquery(df[required_cols], PROJECT_ID, DATASET_ID, "era5", mode="WRITE_APPEND")
        print(f"✓ ({time.time() - save_start:.1f}s)")
        print(f"✓ ERA5 fetch complete: {len(df)} records")
        return True
    
    print("✗ No ERA5 data fetched")
    return False


def fetch_viirs_data(**context):
    """Fetch VIIRS data for last 9 days."""
    print("\n=== Fetching VIIRS Data (9 days) ===")
    
    # Initialize Earth Engine before any safe-date checks so GEE queries work correctly
    try:
        init_ee(KEY_PATH)
    except Exception:
        # If already initialized or running in an environment without EE, proceed;
        # safe-date helpers will fall back gracefully if needed.
        pass
    
    # Calculate date range using shared utility (uses GEE via safe-date helpers)
    safe_date_str = get_viirs_safe_date()
    start_date, end_date, skip_fetch, latest_bq_date, target_date, safe_date = calculate_fetch_date_range(
        table_name="viirs",
        lookback_days=9,
        get_latest_bq_date_fn=get_latest_bq_date,
        safe_date_str=safe_date_str
    )
    
    gee_warn = f"⚠ Warning: Can only fetch up to {end_date} from GEE (VIIRS has no backup)" if (not skip_fetch and end_date < target_date) else None
    log_fetch_range(
        "VIIRS",
        safe_date_str,
        latest_bq_date,
        start_date,
        end_date,
        skip_fetch,
        skip_reason="Skipping fetch - no new data available from GEE" if skip_fetch else None,
        gee_warning=gee_warn,
    )
    if skip_fetch:
        return True

    # Resolve region list: optional chunk (for chunked DAG tasks) or all regions
    all_regions = regions_ee()
    chunk_index = context.get("chunk_index") if isinstance(context, dict) else None
    total_chunks = context.get("total_chunks") if isinstance(context, dict) else None
    if chunk_index is not None and total_chunks is not None and total_chunks > 0:
        sorted_regions = sorted(all_regions.keys())
        n = len(sorted_regions)
        chunk_size = (n + total_chunks - 1) // total_chunks
        start = chunk_index * chunk_size
        end = min(start + chunk_size, n)
        regions_to_fetch = sorted_regions[start:end]
        print(f"VIIRS chunk {chunk_index + 1}/{total_chunks}: {len(regions_to_fetch)} regions")
    else:
        regions_to_fetch = [r for r in all_regions.keys() if r in all_regions]
    
    n_regions = len(regions_to_fetch)
    workers = min(VIIRS_MAX_WORKERS, n_regions) if n_regions else 1
    print(f"Fetching VIIRS for {n_regions} region(s) with {workers} worker(s)")

    def _fetch_viirs_one(region_name):
        geom = all_regions[region_name]
        df = fetch_viirs_daily(geom, format_date(start_date), format_date(end_date))
        if df is not None and not df.empty:
            df = df.copy()
            df['region'] = region_name
            return (region_name, df)
        return (region_name, None)

    frames = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_region = {executor.submit(_fetch_viirs_one, r): r for r in regions_to_fetch}
        for idx, future in enumerate(as_completed(future_to_region), 1):
            region_name = future_to_region[future]
            try:
                _, df = future.result()
                if df is not None:
                    frames.append(df)
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✓")
                else:
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗")
            except Exception as e:
                print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗ {e}")

    if not frames:
        print("✗ No VIIRS data fetched")
        return False
    
    # Filter out any empty DataFrames before concatenation to avoid warnings
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        print("✗ No VIIRS data fetched")
        return False
    
    df = pd.concat(frames, ignore_index=True).sort_values(['region', 'date'])
    
    if df is not None and not df.empty:
        # Ensure date column is datetime type (not date object)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        df = _dedupe_by_keys(df, ['date', 'region'])
        
        print(f"  Saving to BigQuery...", end=" ", flush=True)
        save_start = time.time()
        save_to_bigquery(df, PROJECT_ID, DATASET_ID, "viirs", mode="WRITE_APPEND")
        print(f"✓ ({time.time() - save_start:.1f}s)")
        print(f"✓ VIIRS fetch complete: {len(df)} records")
        return True
    else:
        print("✗ No VIIRS data fetched")
        return False


def fetch_landsat_data(**context):
    """Fetch Landsat data for last 32 days."""
    print("\n=== Fetching Landsat Data (32 days) ===")
    
    # Calculate date range using shared utility
    safe_date_str = get_landsat_safe_date()
    start_date, end_date, skip_fetch, latest_bq_date, target_date, safe_date = calculate_fetch_date_range(
        table_name="landsat",
        lookback_days=32,
        get_latest_bq_date_fn=get_latest_bq_date,
        safe_date_str=safe_date_str
    )
    
    # Check if we already have a 16-day period covering end_date
    if check_landsat_period_available(end_date):
        log_fetch_range(
            "Landsat",
            safe_date_str,
            latest_bq_date,
            start_date,
            end_date,
            True,
            skip_reason="Skipping fetch - period already in BQ (no new period from GEE)",
        )
        return True

    gee_warn = f"⚠ Warning: Can only fetch up to {end_date} from GEE (Landsat has no backup)" if (not skip_fetch and end_date < target_date) else None
    log_fetch_range(
        "Landsat",
        safe_date_str,
        latest_bq_date,
        start_date,
        end_date,
        skip_fetch,
        skip_reason="Skipping fetch - no new data available from GEE" if skip_fetch else None,
        gee_warning=gee_warn,
    )
    if skip_fetch:
        return True
    
    # Initialize Earth Engine if not already done
    try:
        init_ee(KEY_PATH)
    except Exception:
        pass  # Already initialized

    all_regions_list = sorted(regions_ee().keys())
    chunk_index = context.get("chunk_index") if isinstance(context, dict) else None
    total_chunks = context.get("total_chunks") if isinstance(context, dict) else None
    if chunk_index is not None and total_chunks is not None and total_chunks > 0:
        n_all = len(all_regions_list)
        chunk_size = (n_all + total_chunks - 1) // total_chunks
        start = chunk_index * chunk_size
        end = min(start + chunk_size, n_all)
        regions_to_fetch = all_regions_list[start:end]
        print(f"Landsat chunk {chunk_index + 1}/{total_chunks}: {len(regions_to_fetch)} regions")
    else:
        regions_to_fetch = all_regions_list
    n_regions = len(regions_to_fetch)
    workers = min(LANDSAT_MAX_WORKERS, n_regions) if n_regions else 1
    print(f"Fetching Landsat for {n_regions} region(s) with {workers} worker(s)")

    def _fetch_landsat_one(region_name):
        df = fetch_landsat_daily_ndvi(region_name, format_date(start_date), format_date(end_date))
        return (region_name, df if (df is not None and not df.empty) else None)

    frames = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_region = {executor.submit(_fetch_landsat_one, r): r for r in regions_to_fetch}
        for idx, future in enumerate(as_completed(future_to_region), 1):
            region_name = future_to_region[future]
            try:
                _, df = future.result()
                if df is not None:
                    frames.append(df)
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✓")
                else:
                    print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗")
            except Exception as e:
                print(f"[{idx:02d}/{n_regions:02d}] {region_name}: ✗ {e}")

    if frames:
        # Filter out any empty DataFrames before concatenation to avoid warnings
        frames = [f for f in frames if f is not None and not f.empty]
        if frames:
            print(f"  Aggregating to 16-day periods...", end=" ", flush=True)
            agg_start = time.time()
            all_data = pd.concat(frames, ignore_index=True)
            aggregated = aggregate_landsat_to_16day(all_data)
            print(f"✓ ({time.time() - agg_start:.1f}s)")
        else:
            print("✗ No Landsat data fetched")
            return False
        
        print(f"  Saving to BigQuery...", end=" ", flush=True)
        save_start = time.time()
        aggregated = _dedupe_by_keys(aggregated, ['date', 'region'])
        save_to_bigquery(aggregated, PROJECT_ID, DATASET_ID, "landsat", mode="WRITE_APPEND")
        print(f"✓ ({time.time() - save_start:.1f}s)")
        print(f"✓ Landsat fetch complete: {len(aggregated)} records")
        return True
    else:
        print("✗ No Landsat data fetched")
        return False


def fetch_openmeteo_data(**context):
    """Fetch archive weather + GloFAS river_discharge into openmeteo_weather; forecast + GloFAS into openmeteo_forecast."""
    print("\n=== Fetching OpenMeteo Data (30 days + forecast) ===")
    
    today = datetime.datetime.now(datetime.timezone.utc).date()
    
    # Calculate date range using shared utility
    start_date, end_date, skip_fetch, latest_bq_date, target_date, safe_date = calculate_fetch_date_range(
        table_name="openmeteo_weather",
        lookback_days=30,
        get_latest_bq_date_fn=get_latest_bq_date,
        safe_date_str=None,  # No safe date limit for OpenMeteo API
        min_lookback_days=30  # Always fetch at least 30 days for consistency
    )
    
    skip_historical = skip_fetch
    yesterday_str = format_date(today - datetime.timedelta(days=1))
    if skip_fetch:
        log_fetch_range("OpenMeteo", yesterday_str, latest_bq_date, start_date, end_date, True, skip_reason="Skipping historical fetch - fetching forecast only")
    else:
        log_fetch_range("OpenMeteo", yesterday_str, latest_bq_date, start_date, end_date, False)
    
    # Resolve region list: optional chunk (for chunked DAG tasks) or all regions
    all_regions = regions_openmeteo()
    chunk_index = context.get("chunk_index") if isinstance(context, dict) else None
    total_chunks = context.get("total_chunks") if isinstance(context, dict) else None
    
    if chunk_index is not None and total_chunks is not None and total_chunks > 0:
        sorted_regions = sorted(all_regions.keys())
        n = len(sorted_regions)
        chunk_size = (n + total_chunks - 1) // total_chunks
        start = chunk_index * chunk_size
        end = min(start + chunk_size, n)
        regions_to_fetch_names = sorted_regions[start:end]
        print(f"OpenMeteo chunk {chunk_index + 1}/{total_chunks}: {len(regions_to_fetch_names)} regions")
    else:
        regions_to_fetch_names = list(all_regions.keys())
    
    n_regions = len(regions_to_fetch_names)
    if n_regions == 0:
        print("No regions to fetch for this chunk.")
        return True

    lats = [all_regions[name]['lat'] for name in regions_to_fetch_names]
    lons = [all_regions[name]['lon'] for name in regions_to_fetch_names]

    # 1. Fetch historical data for regions in this chunk (Batch)
    import utils.datasets.openmeteo_utils as om_utils
    if not skip_historical:
        print(f"  Fetching historical data for {n_regions} region(s) in batch...")
        
        try:
            # Fetch historical weather in batch
            hist_weather_list = om_utils.fetch_openmeteo_historical_batch(lats, lons, format_date(start_date), format_date(end_date))
            
            # Fetch historical discharge in batch
            hist_flood_list = om_utils.fetch_openmeteo_flood_discharge_batch(lats, lons, start_date=format_date(start_date), end_date=format_date(end_date))
            
            hist_data = []
            for i, name in enumerate(regions_to_fetch_names):
                h_weather = hist_weather_list[i]
                h_flood = hist_flood_list[i]
                
                if not h_weather.empty:
                    # Merge discharge onto weather
                    if not h_flood.empty and "river_discharge" in h_flood.columns:
                        f = h_flood[["date", "river_discharge"]].copy()
                        h_weather["date_norm"] = pd.to_datetime(h_weather["date"]).dt.normalize()
                        f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
                        h_weather = h_weather.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left").drop(columns=["date_norm"])
                    else:
                        h_weather["river_discharge"] = pd.NA
                    
                    h_weather['region_name'] = name
                    hist_data.append(h_weather)
            
            if hist_data:
                df_hist = pd.concat(hist_data, ignore_index=True)
                save_to_bigquery(df_hist, PROJECT_ID, DATASET_ID, "openmeteo_weather", mode="WRITE_APPEND")
                print(f"✓ Saved {len(df_hist)} historical weather records")
        except Exception as e:
            print(f"✗ Error in historical batch fetch: {e}")
            raise

    # 2. Fetch forecast data for regions in this chunk (Batch)
    forecast_end = today + datetime.timedelta(days=6)  # 7 days ahead
    print(f"Fetching forecast data for {format_date(today)} to {format_date(forecast_end)}")
    print(f"  Fetching forecast for {n_regions} region(s) in batch...")

    try:
        # Fetch forecast weather in batch
        fore_weather_list = om_utils.fetch_openmeteo_forecast_batch(lats, lons)
        
        # Fetch forecast soil moisture in batch
        fore_sm_list = om_utils.fetch_openmeteo_forecast_hourly_batch(lats, lons, past_days=0, forecast_days=7, include_soil_temperature=False, include_sm1_sm2_equivalent=True)
        
        # Fetch forecast discharge in batch
        fore_flood_list = om_utils.fetch_openmeteo_flood_discharge_batch(lats, lons, past_days=0, forecast_days=7)
        
        forecast_data = []
        for i, name in enumerate(regions_to_fetch_names):
            f_weather = fore_weather_list[i]
            f_sm = fore_sm_list[i]
            f_flood = fore_flood_list[i]
            
            if not f_weather.empty:
                # Merge soil moisture
                if not f_sm.empty and 'date' in f_sm.columns and 'sm1_mean' in f_sm.columns and 'sm2_mean' in f_sm.columns:
                    f_weather = f_weather.merge(f_sm[['date', 'sm1_mean', 'sm2_mean']], on='date', how='left')
                
                # Merge discharge
                if not f_flood.empty and "river_discharge" in f_flood.columns:
                    f = f_flood[["date", "river_discharge"]].copy()
                    f_weather["date_norm"] = pd.to_datetime(f_weather["date"]).dt.normalize()
                    f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
                    f_weather = f_weather.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left").drop(columns=["date_norm"])
                else:
                    f_weather["river_discharge"] = pd.NA
                
                f_weather['region_name'] = name
                forecast_data.append(f_weather)

        if forecast_data:
            df_forecast = pd.concat(forecast_data, ignore_index=True)
            df_forecast = _dedupe_by_keys(df_forecast, ['date', 'region_name'])
            save_to_bigquery(df_forecast, PROJECT_ID, DATASET_ID, "openmeteo_forecast", mode="WRITE_APPEND")
            print(f"✓ Saved {len(df_forecast)} forecast records")
            
    except Exception as e:
        print(f"✗ Error in forecast batch fetch: {e}")
        raise
    
    print(f"✓ OpenMeteo fetch complete for this chunk")
    return True


# Table for river discharge (Flood API); used by flood/landslide modifiers and outlook


# Table for river discharge (Flood API); used by flood/landslide modifiers and outlook
