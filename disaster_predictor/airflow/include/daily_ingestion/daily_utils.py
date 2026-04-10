import datetime
import pandas as pd
import ee
from typing import Optional, Callable, Tuple

# Configuration for safe date checking per collection
SAFE_DATE_CONFIG = {
    "ECMWF/ERA5_LAND/HOURLY": {
        "max_days_back": 14,
        "start_days": 5,
        "fallback_days": 10
    },
    "MODIS/061/MOD13Q1": {
        "max_days_back": 20,
        "start_days": 1,
        "fallback_days": 10
    },
    "NASA/VIIRS/002/VNP14A1": {
        "max_days_back": 7,
        "start_days": 1,
        "fallback_days": 5
    },
    "FIRMS": {
        "max_days_back": 3,
        "start_days": 0,
        "fallback_days": 2
    },
    "LANDSAT/LC09/C02/T1_L2": {
        "max_days_back": 32,
        "start_days": 16,
        "step_days": 16,
        "fallback_days": 16,
        "has_two_phase": True
    }
}

def format_date(date_obj) -> str:
    """Format a date object to YYYY-MM-DD string format."""
    if isinstance(date_obj, str):
        return date_obj
    elif isinstance(date_obj, (datetime.date, datetime.datetime)):
        return date_obj.strftime('%Y-%m-%d')
    else:
        raise ValueError(f"Unsupported date type: {type(date_obj)}")


def inclusive_days(start_date: datetime.date, end_date: datetime.date) -> int:
    """Number of calendar days in [start_date, end_date] (both inclusive)."""
    return (end_date - start_date).days + 1


def log_fetch_range(
    source_name: str,
    safe_date_str: Optional[str],
    latest_bq_date: Optional[datetime.date],
    start_date: datetime.date,
    end_date: datetime.date,
    skip_fetch: bool,
    skip_reason: Optional[str] = None,
    gee_warning: Optional[str] = None,
) -> None:
    """
    Single-line log for fetch date range. Use across ERA5, VIIRS, Landsat, OpenMeteo.
    Convention: start_date and end_date are both inclusive; we fetch exactly inclusive_days(start, end) dates.
    """
    if source_name in ["ERA5", "VIIRS", "Landsat"]:
        source_max_str = f"GEE max: {safe_date_str}" if safe_date_str else "GEE max: N/A"
    else:
        source_max_str = f"Source max: {safe_date_str}" if safe_date_str else ""
    
    bq_str = f"BQ max: {latest_bq_date}" if latest_bq_date is not None else "BQ max: none"
    
    info_parts = [p for p in [source_max_str, bq_str] if p]
    info_str = ", ".join(info_parts)
    
    if skip_fetch:
        msg = f"{source_name} — {info_str}. {skip_reason or 'Skipping fetch (no new data).'}"
        print(msg)
        return
    n_days = inclusive_days(start_date, end_date)
    msg = f"{source_name} — {info_str}. Fetching from {format_date(start_date)} to {format_date(end_date)} ({n_days} day{'s' if n_days != 1 else ''})"
    print(msg)
    if gee_warning:
        print(gee_warning)

def get_yesterday_date() -> str:
    """Get yesterday's date in YYYY-MM-DD format."""
    yesterday = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
    return format_date(yesterday)

def _check_date_has_data(collection_id: str, test_date: datetime.date) -> bool:
    """Check if data exists for a specific date in an Earth Engine collection."""
    try:
        ic = ee.ImageCollection(collection_id).filterDate(
            format_date(test_date), 
            format_date(test_date + datetime.timedelta(days=1))
        )
        count = ic.size().getInfo()
        return count > 0
    except Exception:
        return False

def get_safe_date(collection_id: str, config: dict = None) -> str:
    """
    Get safe date for a collection using progressive checking.
    
    Args:
        collection_id: Earth Engine collection ID
        config: Optional override config dict with max_days_back, start_days, fallback_days, etc.
        
    Returns:
        Safe date in YYYY-MM-DD format
    """
    today = datetime.datetime.now(datetime.timezone.utc).date()
    
    if config is None:
        config = SAFE_DATE_CONFIG.get(collection_id, {
            "max_days_back": 7,
            "start_days": 1,
            "fallback_days": 5
        })
    
    max_days_back = config["max_days_back"]
    start_days = config.get("start_days", 1)
    step_days = config.get("step_days", 1)
    fallback_days = config.get("fallback_days", max_days_back)
    has_two_phase = config.get("has_two_phase", False)
    
    if has_two_phase:
        # Two-phase check (for Landsat)
        for days_back in range(start_days, max_days_back + 1, step_days):
            test_date = today - datetime.timedelta(days=days_back)
            if _check_date_has_data(collection_id, test_date):
                return format_date(test_date)
        
        for days_back in range(1, start_days):
            test_date = today - datetime.timedelta(days=days_back)
            if _check_date_has_data(collection_id, test_date):
                return format_date(test_date)
    else:
        # Single-phase check
        for days_back in range(start_days, max_days_back + 1, step_days):
            test_date = today - datetime.timedelta(days=days_back)
            if _check_date_has_data(collection_id, test_date):
                return format_date(test_date)
    
    fallback_date = today - datetime.timedelta(days=fallback_days)
    return format_date(fallback_date)

def get_daily_data_summary(df: pd.DataFrame) -> dict:
    """Get summary statistics for daily data collection."""
    if df.empty:
        return {"rows": 0, "regions": 0, "date_range": "No data"}
    
    summary = {
        "rows": len(df),
        "regions": df['region'].nunique() if 'region' in df.columns else 0,
        "date_range": f"{df['date'].min()} to {df['date'].max()}",
        "regions_list": sorted(df['region'].unique().tolist()) if 'region' in df.columns else []
    }
    return summary

# Convenience functions for backward compatibility
def get_era5_safe_date() -> str:
    """Get safe date for ERA5 data collection."""
    return get_safe_date("ECMWF/ERA5_LAND/HOURLY")

def get_viirs_safe_date() -> str:
    """Get safe date for VIIRS data collection."""
    return get_safe_date("NASA/VIIRS/002/VNP14A1")

def get_landsat_safe_date() -> str:
    """Get safe date for Landsat data collection."""
    return get_safe_date("LANDSAT/LC09/C02/T1_L2")

# Legacy functions (kept for compatibility but can be removed if not used elsewhere)
def get_collection_latest_date(collection_id: str) -> str:
    """Legacy function - use get_safe_date() instead."""
    return get_safe_date(collection_id)

def get_safe_collection_date(collection_id: str, preferred_days_back: int = 7) -> str:
    """Legacy function - use get_safe_date() instead."""
    return get_safe_date(collection_id)

def get_dataset_safe_dates() -> dict:
    """Get safe dates for all datasets used in daily monitoring."""
    datasets = {
        "era5": "ECMWF/ERA5_LAND/HOURLY",
        "modis_ndvi": "MODIS/061/MOD13Q1",
        "modis_thermal": "MODIS/061/MOD11A1", 
        "viirs_fire": "NASA/VIIRS/002/VNP14A1",
        "firms": "FIRMS"
    }
    
    return {name: get_safe_date(collection_id) for name, collection_id in datasets.items()}


def calculate_fetch_date_range(
    table_name: str,
    lookback_days: int,
    get_latest_bq_date_fn: Callable[[str], Optional[datetime.date]],
    safe_date_str: Optional[str] = None,
    target_date: Optional[datetime.date] = None,
    min_lookback_days: Optional[int] = None
) -> Tuple[datetime.date, datetime.date, bool, Optional[datetime.date], datetime.date, Optional[datetime.date]]:
    """
    Calculate the date range for fetching data, considering BigQuery max date and safe date limits.
    
    This function implements the common pattern:
    1. Determine target end_date (yesterday or safe_date limit, whichever is earlier)
    2. Check if data is already fresh in BigQuery
    3. Calculate start_date (incremental from BQ max or full lookback period)
    
    Args:
        table_name: BigQuery table name to check for latest date
        lookback_days: Number of days to look back from end_date (used when no BQ data exists)
        get_latest_bq_date_fn: Function that takes table_name and returns latest date or None
        safe_date_str: Optional safe date string (YYYY-MM-DD) from GEE to limit end_date
        target_date: Optional target date (defaults to yesterday)
        min_lookback_days: Optional minimum lookback days for incremental fetch (defaults to lookback_days)
                        Used for APIs like OpenMeteo that want to fetch at least N days even if BQ has data
    
    Returns:
        Tuple of (start_date, end_date, skip_fetch, latest_bq_date, target_date, safe_date).
        start_date and end_date are both inclusive; fetchers must return data for every day in [start_date, end_date].
    """
    if target_date is None:
        target_date = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)
    
    # Parse safe_date if provided
    safe_date = None
    if safe_date_str:
        safe_date = datetime.datetime.strptime(safe_date_str, "%Y-%m-%d").date()
    
    # Calculate end_date: min of target_date and safe_date (if provided)
    if safe_date:
        end_date = min(target_date, safe_date)
    else:
        end_date = target_date
    
    # Get latest date from BigQuery
    latest_bq_date = get_latest_bq_date_fn(table_name)
    
    # Check if data is already fresh
    if latest_bq_date is not None and latest_bq_date >= end_date:
        # Data is fresh, calculate a dummy start_date for completeness
        start_date = end_date - datetime.timedelta(days=lookback_days)
        return (start_date, end_date, True, latest_bq_date, target_date, safe_date)
    
    # Data needs to be fetched - calculate start_date
    if latest_bq_date is not None:
        # Incremental fetch: start from day after latest BQ date
        # But respect minimum lookback if specified (for APIs that need full periods)
        if min_lookback_days is not None:
            default_start = end_date - datetime.timedelta(days=min_lookback_days)
            incremental_start = latest_bq_date + datetime.timedelta(days=1)
            start_date = max(incremental_start, default_start)
        else:
            start_date = latest_bq_date + datetime.timedelta(days=1)
    else:
        # No BQ data exists, use full lookback period
        start_date = end_date - datetime.timedelta(days=lookback_days)
    
    return (start_date, end_date, False, latest_bq_date, target_date, safe_date)
