"""
Utilities for fetching NASA Global Landslide Catalog (GLC) events from Google Earth Engine.
"""

import pandas as pd
import ee
from datetime import datetime
from typing import Optional
from ..earth_engine_utils import get_info_with_timeout

# GEE Dataset ID for NASA Global Landslide Catalog
GLC_DATASET_ID = 'projects/sat-io/open-datasets/events/global_landslide_1970-2019'


def parse_glc_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string from GLC dataset.
    
    Format: MM/DD/YYYY HH:MM:SS AM/PM
    Example: "07/03/2017 06:04:00 PM"
    
    Args:
        date_str: Date string from GLC event_date property
        
    Returns:
        datetime object or None if parsing fails
    """
    if not date_str:
        return None
    try:
        # Extract date part (before space)
        date_part = date_str.split()[0]
        return datetime.strptime(date_part, '%m/%d/%Y')
    except (ValueError, IndexError):
        return None


def fetch_glc_events_for_region(
    region_geom: ee.Geometry,
    start_date: str = '1970-01-01',
    end_date: str = '2019-12-31',
    timeout_seconds: int = 300
) -> pd.DataFrame:
    """
    Fetch NASA GLC landslide events for a specific region.
    
    Note: GLC stores dates as strings, so we filter by geometry in GEE,
    then parse and filter dates in Python.
    
    Args:
        region_geom: Earth Engine geometry for the region
        start_date: Start date for filtering (YYYY-MM-DD)
        end_date: End date for filtering (YYYY-MM-DD)
        timeout_seconds: Timeout for GEE getInfo() calls
        
    Returns:
        DataFrame with columns:
            - region: Region ID (not included, add when calling)
            - date: Event date (YYYY-MM-DD)
            - event_id: GLC event ID
            - latitude: Event latitude
            - longitude: Event longitude
            - location: Location description
            - country: Country name
            - fatality_count: Number of fatalities
            - event_title: Event title
            - event_desc: Event description (truncated)
            - source: Data source ('NASA_GLC')
    """
    # Load GLC dataset
    glc_fc = ee.FeatureCollection(GLC_DATASET_ID)
    
    # Filter by geometry (GEE can do this efficiently)
    region_landslides = glc_fc.filterBounds(region_geom)
    
    # Get count first
    count = region_landslides.size().getInfo()
    
    if count == 0:
        return pd.DataFrame()
    
    # Fetch events (limit for performance)
    max_fetch = min(count, 1000)  # Reasonable limit
    events_fc = region_landslides.limit(max_fetch)
    
    # Get event data
    events_info = get_info_with_timeout(events_fc, timeout_seconds=timeout_seconds)
    
    if 'features' not in events_info or not events_info['features']:
        return pd.DataFrame()
    
    # Parse dates and filter
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    events = []
    for feature in events_info['features']:
        props = feature.get('properties', {})
        geom = feature.get('geometry', {})
        
        # Parse event date
        event_date_str = props.get('event_date', '')
        event_date = parse_glc_date(event_date_str)
        
        # Filter by date range
        if event_date and start_dt <= event_date <= end_dt:
            # Extract coordinates
            coords = geom.get('coordinates', [])
            latitude = coords[1] if len(coords) >= 2 else props.get('latitude')
            longitude = coords[0] if len(coords) >= 2 else props.get('longitude')
            
            events.append({
                'date': event_date.strftime('%Y-%m-%d'),
                'event_id': props.get('event_id'),
                'latitude': latitude,
                'longitude': longitude,
                'location': props.get('location_d', ''),
                'country': props.get('country_na', ''),
                'fatality_count': props.get('fatality_c'),
                'event_title': props.get('event_titl', ''),
                'event_desc': (props.get('event_desc', '')[:200] if props.get('event_desc') else ''),
                'source': 'NASA_GLC'
            })
    
    if not events:
        return pd.DataFrame()
    
    df = pd.DataFrame(events)
    return df


def fetch_all_glc_events(
    start_date: str = '1970-01-01',
    end_date: str = '2019-12-31',
    timeout_seconds: int = 600,
    batch_size: int = 4000
) -> pd.DataFrame:
    """
    Fetch ALL NASA GLC landslide events (not filtered by region).
    
    Note: GEE has a limit of 5000 elements per query, so we fetch in batches
    using spatial filtering (world divided into grid cells).
    
    Args:
        start_date: Start date for filtering (YYYY-MM-DD)
        end_date: End date for filtering (YYYY-MM-DD)
        timeout_seconds: Timeout for GEE getInfo() calls
        batch_size: Maximum events per batch (must be < 5000)
        
    Returns:
        DataFrame with columns:
            - date: Event date (YYYY-MM-DD)
            - event_id: GLC event ID
            - latitude: Event latitude
            - longitude: Event longitude
            - location: Location description
            - country: Country name
            - fatality_count: Number of fatalities
            - event_title: Event title
            - event_desc: Event description (truncated)
            - source: Data source ('NASA_GLC')
    """
    # Load GLC dataset
    glc_fc = ee.FeatureCollection(GLC_DATASET_ID)
    
    # Get total count
    print("Getting total count of GLC events...")
    total_count = glc_fc.size().getInfo()
    print(f"Total GLC events in dataset: {total_count:,}")
    
    if total_count == 0:
        return pd.DataFrame()
    
    # Parse date range
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Strategy: Divide world into grid cells and fetch events for each cell
    # This avoids the 5000 element limit
    print("\nFetching events in batches using spatial grid...")
    
    # Create a grid covering the world (lat: -90 to 90, lon: -180 to 180)
    # Use 10x10 grid = 100 cells, each should have < 5000 events
    grid_size = 10
    lat_step = 180.0 / grid_size
    lon_step = 360.0 / grid_size
    
    all_events = []
    event_ids_seen = set()  # Track to avoid duplicates at cell boundaries
    
    for lat_idx in range(grid_size):
        for lon_idx in range(grid_size):
            lat_min = -90 + lat_idx * lat_step
            lat_max = lat_min + lat_step
            lon_min = -180 + lon_idx * lon_step
            lon_max = lon_min + lon_step
            
            # Create bounding box for this grid cell
            cell_geom = ee.Geometry.Rectangle([lon_min, lat_min, lon_max, lat_max])
            
            # Filter events in this cell
            cell_events_fc = glc_fc.filterBounds(cell_geom).limit(batch_size)
            
            try:
                cell_count = cell_events_fc.size().getInfo()
                if cell_count == 0:
                    continue
                
                print(f"  Cell [{lat_idx},{lon_idx}]: {cell_count:,} events", end=' ... ')
                
                # Fetch events for this cell
                cell_info = get_info_with_timeout(cell_events_fc, timeout_seconds=timeout_seconds)
                
                if 'features' not in cell_info or not cell_info['features']:
                    print("0 parsed")
                    continue
                
                # Parse events
                cell_events = []
                for feature in cell_info['features']:
                    props = feature.get('properties', {})
                    geom = feature.get('geometry', {})
                    
                    event_id = props.get('event_id')
                    if event_id in event_ids_seen:
                        continue  # Skip duplicates
                    event_ids_seen.add(event_id)
                    
                    # Parse event date
                    event_date_str = props.get('event_date', '')
                    event_date = parse_glc_date(event_date_str)
                    
                    # Filter by date range
                    if event_date and start_dt <= event_date <= end_dt:
                        # Extract coordinates
                        coords = geom.get('coordinates', [])
                        latitude = coords[1] if len(coords) >= 2 else props.get('latitude')
                        longitude = coords[0] if len(coords) >= 2 else props.get('longitude')
                        
                        cell_events.append({
                            'date': event_date.strftime('%Y-%m-%d'),
                            'event_id': event_id,
                            'latitude': latitude,
                            'longitude': longitude,
                            'location': props.get('location_d', ''),
                            'country': props.get('country_na', ''),
                            'fatality_count': props.get('fatality_c'),
                            'event_title': props.get('event_titl', ''),
                            'event_desc': (props.get('event_desc', '')[:200] if props.get('event_desc') else ''),
                            'source': 'NASA_GLC'
                        })
                
                all_events.extend(cell_events)
                print(f"{len(cell_events):,} in date range (total: {len(all_events):,})")
                
            except Exception as e:
                print(f"ERROR: {e}")
                continue
    
    if not all_events:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_events)
    print(f"\nTotal events in date range: {len(df):,}")
    return df


def get_glc_table_name() -> str:
    """
    Get the BigQuery table name for GLC events.
    
    Returns:
        Table name: 'glc'
    """
    return 'glc'
