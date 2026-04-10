import os
import pandas as pd
import ee
import datetime
from typing import Optional
from ..earth_engine_utils import regions_ee, standard_execution_flow
from ..bq_utils import load_from_bigquery, save_to_bigquery, execute_sql

FIRMS_COLLECTION = "FIRMS"

def fetch_firms_fire_data(region_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch NASA FIRMS fire data for a specific region and date range."""
    geom = regions_ee()[region_name]
    start = ee.Date(start_date)
    end = ee.Date(end_date)
    
    firms_ic = ee.ImageCollection(FIRMS_COLLECTION).filterDate(start, end).filterBounds(geom)
    collection_size = firms_ic.size().getInfo()

    if collection_size == 0:
        return pd.DataFrame(columns=['latitude', 'longitude', 'confidence', 'brightness_temp', 'date', 'region'])

    all_fire_records = []
    try:
        img_list = firms_ic.toList(collection_size)
        for i in range(collection_size):
            img = ee.Image(img_list.get(i))
            img_date = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()

            sample_fc = img.select(['T21', 'confidence']).updateMask(img.select('T21').gt(300)).sample(
                region=geom,
                scale=1000,
                geometries=True
            )

            sample_info = sample_fc.getInfo()
            if not sample_info or 'features' not in sample_info:
                continue

            for feature in sample_info['features']:
                coords = feature.get('geometry', {}).get('coordinates', [])
                props = feature.get('properties', {})
                if len(coords) >= 2:
                    all_fire_records.append({
                        'longitude': coords[0],
                        'latitude': coords[1],
                        'confidence': props.get('confidence'),
                        'brightness_temp': props.get('T21'),
                        'date': img_date,
                        'region': region_name
                    })
    except Exception as e:
        print(f"Error fetching FIRMS for {region_name}: {e}")

    if not all_fire_records:
        return pd.DataFrame(columns=['latitude', 'longitude', 'confidence', 'brightness_temp', 'date', 'region'])

    df = pd.DataFrame(all_fire_records)
    df['date'] = pd.to_datetime(df['date'])
    return df

def sync_firms_incremental(project_id: str, dataset_id: str, region_name: str, table_id: str = "firms"):
    """
    Fetch FIRMS data with smart incremental updates and deduplication.
    Fetches last 3 days to ensure no data is missed due to reporting delays.
    """
    # 1. Get safe date range
    today = datetime.datetime.now(datetime.timezone.utc).date()
    end_date = today - datetime.timedelta(days=1)
    start_date = today - datetime.timedelta(days=3)
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    print(f"  Syncing FIRMS for {region_name} from {start_str} to {end_str}")
    
    # 2. Fetch new data
    new_data = fetch_firms_fire_data(region_name, start_str, end_str)
    if new_data.empty:
        print("  No new FIRMS data found.")
        return

    # 3. Handle Deduplication
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    
    # Check if table exists and has data for this range
    try:
        overlap_query = f"""
        SELECT * FROM `{full_table_id}`
        WHERE date >= '{start_str}' AND region = '{region_name}'
        """
        existing_data = load_from_bigquery(overlap_query)
        
        if existing_data is not None and not existing_data.empty:
            # Combine and deduplicate
            combined = pd.concat([existing_data, new_data], ignore_index=True)
            # Normalize date to string for deduplication if needed, or keep as datetime
            combined['date'] = pd.to_datetime(combined['date']).dt.date
            deduped = combined.drop_duplicates(subset=['date', 'region', 'latitude', 'longitude', 'confidence'])
            
            # Delete existing overlap and re-insert deduped
            delete_query = f"DELETE FROM `{full_table_id}` WHERE date >= '{start_str}' AND region = '{region_name}'"
            execute_sql(delete_query)
            
            save_to_bigquery(deduped, project_id, dataset_id, table_id, mode="WRITE_APPEND")
            print(f"  Inserted {len(deduped)} deduplicated FIRMS records.")
        else:
            save_to_bigquery(new_data, project_id, dataset_id, table_id, mode="WRITE_APPEND")
            print(f"  Inserted {len(new_data)} new FIRMS records.")
            
    except Exception as e:
        print(f"  FIRMS sync failed, falling back to simple append: {e}")
        save_to_bigquery(new_data, project_id, dataset_id, table_id, mode="WRITE_APPEND")
