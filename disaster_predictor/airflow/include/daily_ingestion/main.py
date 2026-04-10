#!/usr/bin/env python3
import os
import sys
import datetime
import time
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

# Set up paths to find utils module - same approach as other files in this directory
# This adds the include/ directory to sys.path, making utils importable if it's at include/utils/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.bq_utils import save_to_bigquery, load_from_bigquery
from utils.earth_engine_utils import init_ee, KEY_PATH
from .daily_utils import (
    get_era5_safe_date, get_viirs_safe_date, 
    get_landsat_safe_date, format_date
)
from utils.datasets.era5_utils import fetch_all_regions_era5, update_era5_spi
from utils.datasets.viirs_utils import fetch_all_regions_viirs
from utils.datasets.firms_utils import sync_firms_incremental
from utils.datasets.landsat_utils import fetch_landsat_daily_ndvi, aggregate_landsat_to_16day
from utils.datasets.openmeteo_utils import sync_openmeteo_all_regions
from utils.earth_engine_utils import regions_ee, regions_openmeteo
from config import get_project_id

PROJECT_ID = get_project_id()
DATASET_ID = "daily_ingestion"

def get_latest_bq_date(table_name: str) -> datetime.date:
    """Get the latest date from BigQuery for a table."""
    try:
        query = f"SELECT MAX(DATE(date)) AS max_date FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        df = load_from_bigquery(query)
        if df is not None and not df.empty and df["max_date"].iloc[0] is not None:
            return df["max_date"].iloc[0]
    except Exception:
        pass
    return None

def fetch_era5_daily():
    """Fetch ERA5 daily data incrementally."""
    print("\n=== ERA5 Daily Data ===")
    
    max_date = get_latest_bq_date("era5")
    safe_date_str = get_era5_safe_date()
    safe_date = datetime.datetime.strptime(safe_date_str, "%Y-%m-%d").date()
    
    if max_date is None:
        start_date = safe_date - datetime.timedelta(days=30)
        print(f"No existing data - fetching from {format_date(start_date)} to {safe_date_str}")
    elif safe_date > max_date:
        start_date = max_date + datetime.timedelta(days=1)
        print(f"Filling gap: {format_date(start_date)} to {safe_date_str} (last: {max_date})")
    else:
        print(f"Up-to-date (last: {max_date}, safe: {safe_date_str})")
        return
    
    df = fetch_all_regions_era5(format_date(start_date), safe_date_str, daily=True)
    
    if df is not None and not df.empty:
        save_to_bigquery(df, PROJECT_ID, DATASET_ID, "era5", mode="WRITE_APPEND")
        print(f"✓ Saved {len(df)} ERA5 records")
        print("Computing SPI metrics...")
        update_era5_spi(PROJECT_ID, DATASET_ID, "era5_spi")
    else:
        print("No ERA5 data fetched")

def fetch_viirs_daily():
    """Fetch VIIRS daily data incrementally."""
    print("\n=== VIIRS Daily Data ===")
    
    max_date = get_latest_bq_date("viirs")
    safe_date_str = get_viirs_safe_date()
    safe_date = datetime.datetime.strptime(safe_date_str, "%Y-%m-%d").date()
    
    if max_date is None:
        start_date = safe_date - datetime.timedelta(days=7)
        print(f"No existing data - fetching from {format_date(start_date)} to {safe_date_str}")
    elif safe_date > max_date:
        start_date = max_date + datetime.timedelta(days=1)
        print(f"Filling gap: {format_date(start_date)} to {safe_date_str} (last: {max_date})")
    else:
        print(f"Up-to-date (last: {max_date}, safe: {safe_date_str})")
        return
    
    df = fetch_all_regions_viirs(format_date(start_date), safe_date_str, daily=True)
    
    if df is not None and not df.empty:
        save_to_bigquery(df, PROJECT_ID, DATASET_ID, "viirs", mode="WRITE_APPEND")
        print(f"✓ Saved {len(df)} VIIRS records")
    else:
        print("No VIIRS data fetched")

def fetch_firms_daily():
    """Fetch FIRMS data incrementally."""
    print("\n=== FIRMS Daily Data ===")
    for region_name in regions_ee().keys():
        sync_firms_incremental(PROJECT_ID, DATASET_ID, region_name, "firms")

def fetch_landsat_daily():
    """Fetch Landsat daily data incrementally."""
    print("\n=== Landsat Daily Data ===")
    
    max_date = get_latest_bq_date("landsat")
    safe_date_str = get_landsat_safe_date()
    safe_date = datetime.datetime.strptime(safe_date_str, "%Y-%m-%d").date()
    
    if max_date is None:
        start_date = safe_date - datetime.timedelta(days=90)
        print(f"No existing data - fetching from {format_date(start_date)} to {safe_date_str}")
    elif safe_date > max_date:
        start_date = max_date + datetime.timedelta(days=1)
        print(f"Filling gap: {format_date(start_date)} to {safe_date_str} (last: {max_date})")
    else:
        print(f"Up-to-date (last: {max_date}, safe: {safe_date_str})")
        return
    
    frames = []
    for region_name in regions_ee().keys():
        print(f"Processing region: {region_name}")
        df = fetch_landsat_daily_ndvi(region_name, format_date(start_date), safe_date_str)
        if df is not None and not df.empty:
            frames.append(df)
    
    if frames:
        all_data = pd.concat(frames, ignore_index=True)
        aggregated = aggregate_landsat_to_16day(all_data)
        save_to_bigquery(aggregated, PROJECT_ID, DATASET_ID, "landsat", mode="WRITE_APPEND")
        print(f"✓ Saved {len(aggregated)} Landsat records")
    else:
        print("No Landsat data fetched")

def fetch_openmeteo_daily():
    """Fetch OpenMeteo weather and forecast data."""
    print("\n=== OpenMeteo Weather Data ===")
    
    max_date = get_latest_bq_date("openmeteo_weather")
    today = datetime.datetime.now(datetime.timezone.utc).date()
    yesterday = today - datetime.timedelta(days=1)
    
    if max_date is None:
        start_date = yesterday - datetime.timedelta(days=120)
    elif max_date < yesterday:
        start_date = max_date + datetime.timedelta(days=1)
    else:
        start_date = None
    
    if start_date:
        sync_openmeteo_all_regions(PROJECT_ID, DATASET_ID, format_date(start_date), format_date(yesterday))
    else:
        print("Historical data up-to-date")
        from utils.datasets.openmeteo_utils import (
            fetch_openmeteo_forecast,
            merge_glofas_river_discharge_onto_openmeteo_forecast,
        )
        regions = regions_openmeteo()
        forecast_end = today + datetime.timedelta(days=6)  # 7 days ahead
        print(f"Fetching forecast data for {format_date(today)} to {format_date(forecast_end)}:")
        forecast_data = []
        for name, coords in regions.items():
            print(f"- processing region {name}")
            df = fetch_openmeteo_forecast(coords['lat'], coords['lon'])
            if not df.empty:
                df = merge_glofas_river_discharge_onto_openmeteo_forecast(
                    df, coords["lat"], coords["lon"]
                )
                df['region_name'] = name
                forecast_data.append(df)
        
        if forecast_data:
            df_forecast = pd.concat(forecast_data, ignore_index=True)
            save_to_bigquery(df_forecast, PROJECT_ID, DATASET_ID, "openmeteo_forecast", mode="WRITE_TRUNCATE")
            print(f"✓ Saved {len(df_forecast)} forecast records")

def daily_ingestion():
    """Main daily ingestion function."""
    print("=== Daily Ingestion ===")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}\n")
    
    start_time = time.time()
    
    print("Initializing Earth Engine...")
    init_ee(KEY_PATH)
    print("✓ Earth Engine initialized\n")
    
    fetch_era5_daily()
    time.sleep(2)
    fetch_viirs_daily()
    time.sleep(2)
    fetch_firms_daily()
    time.sleep(2)
    fetch_landsat_daily()
    time.sleep(2)
    fetch_openmeteo_daily()
    
    elapsed = time.time() - start_time
    print(f"\n=== Complete in {elapsed:.2f} seconds ===")

if __name__ == "__main__":
    daily_ingestion()
