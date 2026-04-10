import os
# Suppress Earth Engine warnings before importing ee module
os.environ['GLOG_minloglevel'] = '2'  # Suppress INFO/WARNING logs

import json
import sys
import contextlib
import concurrent.futures
import pandas as pd
import ee # type: ignore
from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path
from dotenv import load_dotenv
from .bq_utils import save_to_bigquery
from config import get_region_name

# Default timeout (seconds) for GEE getInfo() calls. Prevents silent hangs when
# GEE is overloaded, rate-limiting, or the request is queued indefinitely.
GEE_GETINFO_TIMEOUT = 600  # 10 minutes per year/chunk

# Load environment variables after all imports
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

KEY_PATH = str(Path(__file__).resolve().parent.parent / "config" / "service_account.json")
CLIMATOLOGY_END_DATE = "2025-12-31"

# REGION_NAMES is now loaded from BigQuery - BigQuery is the single source of truth

def get_region_names_from_bq(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> list:
    """
    Load region names from BigQuery.
    
    BigQuery is the single source of truth for region definitions.
    Raises an error if BigQuery is unavailable.
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    from .bq_utils import load_from_bigquery
    
    query = f"""
    SELECT region
    FROM `{project_id}.{dataset_id}.{table_id}`
    ORDER BY region
    """
    
    df = load_from_bigquery(query)
    if df is None or df.empty:
        raise ValueError(
            f"Could not load region names from BigQuery table {project_id}.{dataset_id}.{table_id}. "
            "BigQuery is the single source of truth for region definitions. "
            "Please ensure the table exists and is populated."
        )
    
    return df['region'].tolist()

# For backward compatibility, create a function that returns REGION_NAMES
def get_region_names(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> list:
    """Get region names (alias for get_region_names_from_bq for backward compatibility)."""
    return get_region_names_from_bq(project_id, dataset_id, table_id)

# For backward compatibility with code that uses REGION_NAMES as a list
# We'll create a cached version that loads once
_REGION_NAMES_CACHE = None

def REGION_NAMES():
    """
    Get region names from BigQuery (cached).
    
    This function loads region names from google_earth.regions_info table.
    BigQuery is the single source of truth - no fallbacks.
    
    Usage:
        regions = REGION_NAMES()  # Returns list of region names
        for region in REGION_NAMES():
            ...
    
    Raises:
        ValueError: If BigQuery table is unavailable or empty
    """
    global _REGION_NAMES_CACHE
    if _REGION_NAMES_CACHE is None:
        _REGION_NAMES_CACHE = get_region_names_from_bq()
    return _REGION_NAMES_CACHE

def init_ee(key_path: str):
    """Initialize Earth Engine, suppressing initialization warnings."""
    with open(key_path, "r") as f:
        sa_data = json.load(f)
        client_email = sa_data["client_email"]
        project_id = sa_data["project_id"]
    credentials = ee.ServiceAccountCredentials(client_email, key_path)
    
    # Redirect stderr during initialization to suppress absl/GCP warnings
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stderr(devnull):
            ee.Initialize(credentials, project=project_id)


def get_info_with_timeout(ee_object, timeout_seconds: int = GEE_GETINFO_TIMEOUT):
    """
    Run ee_object.getInfo() in a thread with a timeout.

    GEE getInfo() blocks on HTTP until the server responds. When GEE is overloaded,
    rate-limiting, or requests are queued, it can hang indefinitely with no error.
    This wrapper raises concurrent.futures.TimeoutError after timeout_seconds so
    callers can retry or fail fast instead of hanging.

    Args:
        ee_object: Any Earth Engine computed value (e.g. FeatureCollection).
        timeout_seconds: Max wait time. Default GEE_GETINFO_TIMEOUT (10 min).

    Returns:
        The result of ee_object.getInfo().

    Raises:
        concurrent.futures.TimeoutError: If the call exceeds timeout_seconds.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(ee_object.getInfo)
        return future.result(timeout=timeout_seconds)


def regions_ee(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> dict:
    """
    Get region geometries (subregions) from BigQuery.
    
    Loads region definitions from google_earth.regions_info table and reconstructs
    Earth Engine geometries. BigQuery is the single source of truth.
    
    NOTE: This returns all subregions from regions_info. Parent regions are just for reference.
    
    Raises:
        ValueError: If BigQuery table is unavailable or empty
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    from .bq_utils import load_from_bigquery
    
    # regions_info stores subregions - return all of them
    query = f"""
    SELECT 
        region,
        lon_min,
        lat_min,
        lon_max,
        lat_max
    FROM `{project_id}.{dataset_id}.{table_id}`
    ORDER BY region
    """
    
    df = load_from_bigquery(query)
    if df is None or df.empty:
        raise ValueError(
            f"Could not load regions from BigQuery table {project_id}.{dataset_id}.{table_id}. "
            "BigQuery is the single source of truth for region definitions. "
            "Please ensure the table exists and is populated."
        )
    
    regions = {}
    for _, row in df.iterrows():
        region_name = row['region']
        # Reconstruct geometry from bounding box
        geom = ee.Geometry.Rectangle([
            row['lon_min'],
            row['lat_min'],
            row['lon_max'],
            row['lat_max']
        ])
        regions[region_name] = geom
    
    return regions

def regions_openmeteo(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> dict:
    """
    Get OpenMeteo coordinates for regions (subregions) from BigQuery.
    
    Loads from google_earth.regions_info table. BigQuery is the single source of truth.
    Uses centroid coordinates from regions_info.
    
    Raises:
        ValueError: If BigQuery table is unavailable or empty
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    from .bq_utils import load_from_bigquery
    
    # regions_info stores subregions - return all of them with their centroids
    query = f"""
    SELECT 
        region,
        centroid_lat,
        centroid_lon
    FROM `{project_id}.{dataset_id}.{table_id}`
    ORDER BY region
    """
    
    df = load_from_bigquery(query)
    if df is None or df.empty:
        raise ValueError(
            f"Could not load OpenMeteo coordinates from BigQuery table {project_id}.{dataset_id}.{table_id}. "
            "BigQuery is the single source of truth for region definitions. "
            "Please ensure the table exists and is populated."
        )
    
    regions = {}
    for _, row in df.iterrows():
        region_name = row['region']
        # Handle special case: Central_Kalahari_Ghanzi -> Ghanzi in openmeteo
        if region_name == "Central_Kalahari_Ghanzi":
            regions["Ghanzi"] = {"lat": row['centroid_lat'], "lon": row['centroid_lon']}
        regions[region_name] = {"lat": row['centroid_lat'], "lon": row['centroid_lon']}
    
    return regions

def get_region_geometry_actual(
    region_name: str,
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> ee.Geometry:
    """
    Get actual region geometry (not bounds) for a region from BigQuery.
    
    Loads from google_earth.regions_info table. BigQuery is the single source of truth.
    Returns rectangle geometry from bounding box coordinates.
    
    NOTE: This works with subregions stored in regions_info.
    
    Raises:
        ValueError: If region not found in BigQuery or table is unavailable
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    from .bq_utils import load_from_bigquery
    
    # regions_info stores subregions - direct lookup
    query = f"""
    SELECT 
        lon_min,
        lat_min,
        lon_max,
        lat_max
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE region = '{region_name}'
    LIMIT 1
    """
    
    df = load_from_bigquery(query)
    
    if df is None or df.empty:
        raise ValueError(
            f"Could not load region geometry for '{region_name}' from BigQuery table {project_id}.{dataset_id}.{table_id}. "
            "BigQuery is the single source of truth for region definitions. "
            "Please ensure the region exists in the table."
        )
    
    row = df.iloc[0]
    # Always return rectangle from bounding box (regions_info doesn't store geometry_type)
    return ee.Geometry.Rectangle([
            row['lon_min'],
            row['lat_min'],
            row['lon_max'],
            row['lat_max']
        ])

def subdivide_region_to_target_size(
    region_name: str,
    region_geom: ee.Geometry,
    target_size_km2: float = 10_000,
    min_size_km2: float = 5_000,
    max_depth: int = 5
) -> dict:
    """
    Recursively subdivide a region into subregions of approximately target_size_km2.
    Uses recursive binary splitting along the longest dimension.
    
    Args:
        region_name: Name of the parent region
        region_geom: Actual region geometry
        target_size_km2: Target size for each subregion (default: 10,000 km²)
        min_size_km2: Minimum size before stopping subdivision (default: 5,000 km²)
        max_depth: Maximum recursion depth to prevent infinite loops
    
    Returns:
        Dict mapping subregion_id -> ee.Geometry
    """
    # Allow some flexibility around target size (stop if within 20% of target)
    tolerance = 0.2  # 20% tolerance
    min_acceptable = target_size_km2 * (1 - tolerance)  # 8,000 km²
    max_acceptable = target_size_km2 * (1 + tolerance)  # 12,000 km²
    
    def _subdivide_recursive(
        geom: ee.Geometry,
        name_prefix: str,
        depth: int = 0
    ) -> dict:
        if depth >= max_depth:
            # Stop recursion at max depth
            return {name_prefix: geom}
        
        # Calculate area
        area_m2 = geom.area(maxError=1000).getInfo()
        area_km2 = area_m2 / 1_000_000
        
        # Stop if area is within acceptable range (8k-12k km²) or too small
        if (min_acceptable <= area_km2 <= max_acceptable) or area_km2 < min_size_km2:
            return {name_prefix: geom}
        
        # Only split if significantly larger than target (need room for 2+ subregions)
        # Use 1.2x target to ensure we can create at least 2 subregions
        if area_km2 <= target_size_km2 * 1.2:
            return {name_prefix: geom}
        
        # Get bounding box to determine split direction
        bounds = geom.bounds()
        bounds_info = bounds.getInfo()['coordinates'][0]
        lons = [c[0] for c in bounds_info]
        lats = [c[1] for c in bounds_info]
        lon_min, lat_min = min(lons), min(lats)
        lon_max, lat_max = max(lons), max(lats)
        
        # Split along the longest dimension
        lon_span = lon_max - lon_min
        lat_span = lat_max - lat_min
        
        if lon_span > lat_span:
            # Split vertically (along longitude)
            lon_mid = (lon_min + lon_max) / 2
            left_rect = ee.Geometry.Rectangle([lon_min, lat_min, lon_mid, lat_max])
            right_rect = ee.Geometry.Rectangle([lon_mid, lat_min, lon_max, lat_max])
            left_geom = geom.intersection(left_rect, maxError=1000)
            right_geom = geom.intersection(right_rect, maxError=1000)
        else:
            # Split horizontally (along latitude)
            lat_mid = (lat_min + lat_max) / 2
            bottom_rect = ee.Geometry.Rectangle([lon_min, lat_min, lon_max, lat_mid])
            top_rect = ee.Geometry.Rectangle([lon_min, lat_mid, lon_max, lat_max])
            left_geom = geom.intersection(bottom_rect, maxError=1000)
            right_geom = geom.intersection(top_rect, maxError=1000)
        
        # Check if splits are valid (have meaningful area)
        left_area = left_geom.area(maxError=1000).getInfo() / 1_000_000
        right_area = right_geom.area(maxError=1000).getInfo() / 1_000_000
        
        # If one split is too small, return original
        if left_area < min_size_km2 or right_area < min_size_km2:
            return {name_prefix: geom}
        
        # Recursively subdivide both halves
        result = {}
        result.update(_subdivide_recursive(left_geom, f"{name_prefix}_A", depth + 1))
        result.update(_subdivide_recursive(right_geom, f"{name_prefix}_B", depth + 1))
        return result
    
    # Start recursive subdivision
    subregions_raw = _subdivide_recursive(region_geom, region_name)
    
    # Rename with sequential numbers (01, 02, 03, ...)
    subregions = {}
    for i, (old_name, geom) in enumerate(sorted(subregions_raw.items()), 1):
        new_name = f"{region_name}_{i:02d}"
        subregions[new_name] = geom
    
    return subregions

def get_all_subregions_10k(target_size_km2: float = 10_000) -> dict:
    """
    Get all subregions for all regions, subdivided to approximately target_size_km2.
    
    Args:
        target_size_km2: Target size for each subregion (default: 10,000 km²)
    
    Returns:
        Dict mapping subregion_id -> ee.Geometry
    """
    all_subregions = {}
    for region_name in REGION_NAMES():
        region_geom = get_region_geometry_actual(region_name)
        subregions = subdivide_region_to_target_size(region_name, region_geom, target_size_km2)
        all_subregions.update(subregions)
    return all_subregions

def get_subregions_from_bq(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> dict:
    """
    Load subregions from BigQuery and reconstruct geometries.
    
    All regions are in regions_info (expansion was merged). Queries only the specified table.
    
    Args:
        project_id: BigQuery project ID (uses env var if None)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID (default: regions_info).
    
    Returns:
        Dict mapping subregion_id -> ee.Geometry
    
    The geometries are reconstructed from bounding box coordinates (lon_min, lat_min, lon_max, lat_max)
    stored in the BigQuery table.
    """
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    from .bq_utils import load_from_bigquery
    
    query = f"""
    SELECT
        region,
        lon_min,
        lat_min,
        lon_max,
        lat_max
    FROM `{project_id}.{dataset_id}.{table_id}`
    ORDER BY region
    """
    
    df = load_from_bigquery(query)
    
    if df is None or df.empty:
        raise ValueError(f"No subregions found in {project_id}.{dataset_id}.{table_id}")
    
    subregions = {}
    for _, row in df.iterrows():
        subregion_id = row['region']
        # Reconstruct geometry from bounding box
        geom = ee.Geometry.Rectangle([
            row['lon_min'],
            row['lat_min'],
            row['lon_max'],
            row['lat_max']
        ])
        subregions[subregion_id] = geom
    
    return subregions

def get_parent_region(subregion_id: str) -> str:
    """
    Extract parent region name from subregion_id.
    Handles both old format (e.g., 'Kruger_NP_NW') and new format (e.g., 'Kruger_NP_01').
    """
    # New format: {parent}_01, {parent}_02, etc.
    if '_' in subregion_id:
        parts = subregion_id.rsplit('_', 1)
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0]
    
    # Old format: {parent}_NW, {parent}_NE, etc.
    if subregion_id.endswith(('_NW', '_NE', '_SW', '_SE')):
        return subregion_id[:-3]
    
    return subregion_id

def get_region_area_km2(region: str, project_id: str = None, region_name: str = None):
    """
    Get the area of a region in square kilometers.
    Simple logic: if table exists and has all regions - use cache, otherwise fetch from GEE and truncate.
    """
    if region_name is None:
        region_name = get_region_name()

    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
    
    if _is_cache_complete(project_id, region_name):
        try:
            client = bigquery.Client.from_service_account_json(KEY_PATH, project=project_id)
            cache_table = f"`{project_id}.google_earth.{region_name}_regions`"
            query = f"SELECT area_km2 FROM {cache_table} WHERE region = '{region}'"
            
            query_job = client.query(query)
            results = query_job.result()
            rows = list(results)
            if rows:
                return rows[0].area_km2
        except Exception as e:
            print(f"Could not load from BigQuery cache: {e}")
    
    print(f"Cache incomplete or missing. Fetching all regions from Earth Engine...")
    _populate_cache_from_gee(project_id, region_name)
    
    try:
        client = bigquery.Client.from_service_account_json(KEY_PATH, project=project_id)
        cache_table = f"`{project_id}.google_earth.{region_name}_regions`"
        query = f"SELECT area_km2 FROM {cache_table} WHERE region = '{region}'"
        
        query_job = client.query(query)
        results = query_job.result()
        rows = list(results)
        if rows:
            return rows[0].area_km2
    except Exception as e:
        print(f"Could not load from BigQuery cache after population: {e}")
    
    raise ValueError(f"Could not get area for region '{region}'")

def _is_cache_complete(project_id: str, region_name: str) -> bool:
    """Check if the cache table exists and has all regions."""
    try:
        client = bigquery.Client.from_service_account_json(KEY_PATH, project=project_id)
        table_id = f"{project_id}.google_earth.{region_name}_regions"

        table_ref = bigquery.TableReference.from_string(table_id)
        client.get_table(table_ref)
        
        all_regions = set(REGION_NAMES())
        query = f"SELECT region FROM `{table_id}`"
        query_job = client.query(query)
        results = query_job.result()
        cached_regions = {row.region for row in results}
        
        return all_regions.issubset(cached_regions)
    except Exception:
        return False

def _populate_cache_from_gee(project_id: str, region_name: str):
    """Fetch all regions from Earth Engine and save to BigQuery with truncate."""
    try:
        import ee
        ee.Number(1).getInfo()  # Test if EE is initialized
    except:
        init_ee(KEY_PATH)
        print("Earth Engine initialized for cache population")
    
    client = bigquery.Client.from_service_account_json(KEY_PATH, project=project_id)
    table_id = f"{project_id}.google_earth.{region_name}_regions"
    
    schema = [
        bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("area_km2", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED")
    ]
    
    table_ref = bigquery.TableReference.from_string(table_id)
    try:
        table = client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id}")
    
    all_data = []
    for region in REGION_NAMES():
        print(f"Calculating area for {region}...")
        # Use get_region_geometry_actual to get the actual geometry (not bounds)
        region_geometry = get_region_geometry_actual(region)
        if region_geometry is not None:
            area_m2 = region_geometry.area(maxError=1).getInfo()
            area_km2 = area_m2 / 1_000_000
            all_data.append({
                'region': region,
                'area_km2': area_km2,
                'created_at': pd.Timestamp.now()
            })
    
    if all_data:
        df = pd.DataFrame(all_data)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )
        
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Saved {len(all_data)} regions to BigQuery cache")

def get_region_type_dynamic(region: str, climatology_data=None, fire_activity_data=None):
    """
    Dynamically determine region type based on actual data characteristics.
    This approach works universally for any region worldwide.
    
    Returns: 'desert', 'semi_arid', or 'vegetated'
    """
    if climatology_data is None or fire_activity_data is None:
        # Fallback to moderate classification if no data available
        return 'semi_arid'
    
    avg_ndvi = climatology_data.get('ndvi_mean', 0.3)
    fire_density = fire_activity_data.get('fire_density', 0)  # fires per 1000 km² per year
    
    if avg_ndvi < 0.25 and fire_density < 5.0:
        # Low vegetation and low fire activity = Desert/Arid (more aggressive)
        return 'desert'
    elif fire_density > 20.0 or (avg_ndvi > 0.4 and fire_density > 10.0):
        # High fire activity or high vegetation with good fire activity = Vegetated
        return 'vegetated'
    else:
        # Everything else = Semi-arid
        return 'semi_arid'

def month_starts(start_date: str, end_date: str) -> ee.List:
    s = ee.Date(start_date)
    e = ee.Date(end_date)
    n = e.difference(s, "month").int()
    return ee.List.sequence(0, n.subtract(1)).map(lambda k: s.advance(k, "month"))

def ic_monthly(ic: ee.ImageCollection, start_date: str, end_date: str, reducer=None) -> ee.ImageCollection:
    if reducer is None:
        reducer = ee.Reducer.mean()
    months = month_starts(start_date, end_date)
    def per_m(m):
        m = ee.Date(m)
        img = ic.filterDate(m, m.advance(1, "month")).reduce(reducer)
        return img.set({"system:time_start": m.millis(), "month": m.get("month")})
    return ee.ImageCollection.fromImages(months.map(per_m))

def add_month_prop(ic: ee.ImageCollection) -> ee.ImageCollection:
    return ic.map(lambda img: img.set("month", ee.Date(img.get("system:time_start")).get("month")))

def _reduce_ic_to_df(ic: ee.ImageCollection, geom: ee.Geometry, scale_m: int, band_map: dict) -> pd.DataFrame:
    """
    Reduce ImageCollection to DataFrame. Uses increased tileScale to avoid memory limits.
    """
    def mapper(img):
        stats = img.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=scale_m,
            bestEffort=True,
            maxPixels=1_000_000_000,
            tileScale=8  # Increased from 4 to 8 to reduce memory usage
        )
        props = {"date": ee.Date(img.get("system:time_start")).format("YYYY-MM")}
        for old in band_map.keys():
            props[old] = stats.get(old)
        return ee.Feature(None, props)

    fc = ee.FeatureCollection(ic.map(mapper))
    rows = fc.getInfo()["features"]
    recs = []
    for f in rows:
        p = f["properties"]
        rec = {"date": p["date"]}
        for old, new in band_map.items():
            v = p.get(old)
            rec[new] = float(v) if v is not None else None
        recs.append(rec)
    return pd.DataFrame(recs).sort_values("date").reset_index(drop=True)

def standard_execution_flow(
    data_fetch_function,
    start_date="1981-01-01",
    end_date="2025-09-01",
    project_id=None,
    dataset_id=None,
    table_id=None,
    mode='WRITE_APPEND',
    description="Data collection"
):
    """
    Standard execution flow for any data collection script.
    
    Args:
        data_fetch_function: Function that returns a DataFrame
        start_date: Start date for data collection (YYYY-MM-DD format)
        end_date: End date for data collection (YYYY-MM-DD format)
        project_id: BigQuery project ID (optional, will use env var or service account if not provided)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        mode: Write disposition mode
        description: Description for logging
    """
    if not project_id:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        # If env var not set, read from service account file as fallback
        if not project_id:
            with open(KEY_PATH, "r") as f:
                project_id = json.load(f).get("project_id")
    
    print(f"Starting {description}...")
    print(f"Date range: {start_date} to {end_date}")
    
    # Pass start and end dates to the data fetch function
    df = data_fetch_function(start_date, end_date)
    
    if df is None or df.empty:
        print("No data to load.")
        return None
    
    save_to_bigquery(df, project_id, dataset_id, table_id, mode=mode)
    
    print(f"Loaded {len(df)} rows into {project_id}.{dataset_id}.{table_id}.")
    return df


def map_points_to_regions(
    events_df: pd.DataFrame,
    latitude_col: str = 'latitude',
    longitude_col: str = 'longitude',
    event_id_col: str = 'event_id',
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
    predicate: str = 'within',
    use_bq_geometries: bool = True
) -> pd.DataFrame:
    """
    Map points (events) to regions using spatial intersection.
    
    This is a universal utility for matching any point dataset to regions.
    Works with GLC, Sen12Landslides, or any other point-based event dataset.
    
    Args:
        events_df: DataFrame with event points (must have latitude/longitude columns)
        latitude_col: Name of latitude column (default: 'latitude')
        longitude_col: Name of longitude column (default: 'longitude')
        event_id_col: Name of event ID column (default: 'event_id')
        project_id: BigQuery project ID (uses env var if None)
        dataset_id: BigQuery dataset ID for regions tables
        table_id: BigQuery table ID (default: regions_info).
        predicate: Spatial predicate for matching ('within' or 'intersects', default: 'within')
        use_bq_geometries: If True, use bounding boxes directly from BigQuery (more reliable).
                          If False, reconstruct from Earth Engine geometries (slower).
    
    Returns:
        DataFrame with added 'region' column. Events not matching any region will have region=None.
    
    Example:
        >>> from utils.earth_engine_utils import map_points_to_regions
        >>> df_with_regions = map_points_to_regions(events_df)
        >>> matched_count = df_with_regions['region'].notna().sum()
    """
    import geopandas as gpd
    from shapely.geometry import Point, box
    
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
        if project_id is None:
            project_id = "disaster-predictor-470812"
    
    # Validate input
    if latitude_col not in events_df.columns or longitude_col not in events_df.columns:
        raise ValueError(f"DataFrame must have '{latitude_col}' and '{longitude_col}' columns")
    
    if event_id_col not in events_df.columns:
        # Create a temporary event_id if not present
        events_df = events_df.copy()
        events_df[event_id_col] = range(len(events_df))
    
    # Filter valid coordinates
    valid_coords = events_df[latitude_col].notna() & events_df[longitude_col].notna()
    df_valid = events_df[valid_coords].copy()
    df_invalid = events_df[~valid_coords].copy()
    
    if len(df_valid) == 0:
        events_df['region'] = None
        return events_df
    
    # Load region geometries
    if use_bq_geometries:
        # Method: Load bounding boxes directly from BigQuery (fastest and most reliable)
        from .bq_utils import load_from_bigquery
        
        query = f"""
            SELECT
                region,
                lon_min,
                lat_min,
                lon_max,
                lat_max
            FROM `{project_id}.{dataset_id}.{table_id}`
            ORDER BY region
            """
        
        regions_df = load_from_bigquery(query, project_id=project_id)
        
        if regions_df is None or regions_df.empty:
            raise ValueError(f"No regions found in {project_id}.{dataset_id}.{table_id}")
        
        # Create GeoDataFrame from bounding boxes
        region_data = []
        for _, row in regions_df.iterrows():
            region_geom = box(row['lon_min'], row['lat_min'], row['lon_max'], row['lat_max'])
            region_data.append({
                'region': row['region'],
                'geometry': region_geom
            })
        
        gdf_regions = gpd.GeoDataFrame(region_data, crs='EPSG:4326')
    else:
        # Method: Reconstruct from Earth Engine geometries (slower, but uses actual geometries)
        regions = get_subregions_from_bq(project_id, dataset_id, table_id)
        
        region_data = []
        for subregion_id, geom in regions.items():
            try:
                bounds_info = geom.bounds().getInfo()
                if 'coordinates' in bounds_info:
                    coords = bounds_info['coordinates'][0]
                    lons = [c[0] for c in coords]
                    lats = [c[1] for c in coords]
                    region_geom = box(min(lons), min(lats), max(lons), max(lats))
                    region_data.append({
                        'region': subregion_id,
                        'geometry': region_geom
                    })
            except Exception:
                continue
        
        gdf_regions = gpd.GeoDataFrame(region_data, crs='EPSG:4326')
    
    # Create GeoDataFrame from events (points)
    events_gdf = gpd.GeoDataFrame(
        df_valid,
        geometry=[Point(lon, lat) for lon, lat in zip(
            df_valid[longitude_col],
            df_valid[latitude_col]
        )],
        crs='EPSG:4326'
    )
    
    # Perform spatial join
    matched = gpd.sjoin(
        events_gdf,
        gdf_regions,
        how='left',
        predicate=predicate
    )
    
    # Handle multiple matches (if event is in multiple regions, take first)
    matched = matched.drop_duplicates(subset=[event_id_col], keep='first')
    
    # Add region column to valid events
    # Handle case where 'region' column might not exist if no matches
    if 'region' in matched.columns:
        df_valid['region'] = matched['region'].values
    else:
        df_valid['region'] = None
    
    # Add region column to invalid events (set to None)
    if len(df_invalid) > 0:
        df_invalid['region'] = None
    
    # Combine back
    df_result = pd.concat([df_valid, df_invalid], ignore_index=True)
    
    # Ensure original order is preserved (if events_df had an index)
    if hasattr(events_df, 'index') and len(df_result) == len(events_df):
        df_result = df_result.reindex(events_df.index, fill_value=None)
    
    return df_result
