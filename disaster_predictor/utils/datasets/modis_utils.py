import time
import concurrent.futures
import pandas as pd
import ee
from typing import Dict, Any, Optional
from ..earth_engine_utils import month_starts, _reduce_ic_to_df, regions_ee, get_info_with_timeout

# MODIS Collection IDs
NDVI_COLLECTION_ID = "MODIS/061/MOD13Q1"
BURN_COLLECTION_ID = "MODIS/061/MCD64A1"

def ndvi_monthly_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 250) -> pd.DataFrame:
    """
    Compute monthly mean NDVI per region.
    Processes in 5-year chunks to avoid memory limits (consistent with ERA5).
    
    Uses MOD13Q1 (250m, 16-day). For each month, takes mean of available scenes,
    rescales by 0.0001 and reduces spatially by mean over the region.
    
    Args:
        geom: Earth Engine geometry
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        scale_m: Scale in meters for reduction
        
    Returns:
        DataFrame with monthly NDVI data
    """
    from datetime import datetime, timedelta
    import time
    
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    
    all_dfs = []
    
    # Process in 2-year chunks to avoid memory limits and rate limits (more conservative)
    current_start = start_dt
    chunk_years = 2
    total_chunks = ((end_dt.year - start_dt.year) // chunk_years) + 1
    current_chunk = 0
    
    while current_start < end_dt:
        # Process 2 years at a time
        current_end = min(
            datetime(current_start.year + chunk_years, 1, 1),
            end_dt
        )
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_chunk += 1
        
        # Retry logic for chunk
        max_retries = 3
        chunk_success = False
        for attempt in range(max_retries):
            try:
                months = month_starts(chunk_start, chunk_end)

                def per_month(m):
                    m = ee.Date(m)
                    ic = (
                        ee.ImageCollection(NDVI_COLLECTION_ID)
                        .filterDate(m, m.advance(1, "month"))
                        .select(["NDVI"])  # int16, scale 1e-4
                    )
                    size = ic.size()
                    ndvi_img = ee.Image(
                        ee.Algorithms.If(
                            size.gt(0),
                            ic.mean().multiply(0.0001).rename(["ndvi"]),  # data case
                            ee.Image.constant(0).updateMask(ee.Image(0)).rename(["ndvi"])  # no-data -> masked
                        )
                    )
                    return ndvi_img.set({"system:time_start": m.millis(), "month": m.get("month")})

                ic = ee.ImageCollection.fromImages(months.map(per_month))

                df = _reduce_ic_to_df(
                    ic,
                    geom,
                    scale_m,
                    band_map={"ndvi": "ndvi_mean"},
                )
                if df is not None and not df.empty:
                    all_dfs.append(df)
                chunk_success = True
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = min(60, 10 * (2 ** attempt))
                    print(f"✗ (retry {attempt + 1}/{max_retries} in {wait_time}s)", end=" ", flush=True)
                    time.sleep(wait_time)
                else:
                    print(f"✗ Error: {e}")
        
        # Sleep between chunks
        if current_start < end_dt:
            time.sleep(3)
        
        current_start = current_end
    
    if not all_dfs:
        return pd.DataFrame(columns=["date", "ndvi_mean"])
    
    return pd.concat(all_dfs, ignore_index=True).sort_values("date").reset_index(drop=True)

def burned_area_pct_monthly_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 500, region_id: Optional[str] = None) -> pd.DataFrame:
    """
    Compute monthly burned area percent per region.
    Processes in 5-year chunks to avoid memory limits (consistent with ERA5).
    
    Uses MCD64A1 v6.1 monthly product (500m). For each month, create a 0/1 burned mask
    from band 'burn_date' (>0 indicates burned). The regional mean of this mask gives
    the fraction of pixels burned; multiply by 100 to get percent.
    This is efficient and avoids heavy area-weighted computations.
    
    Args:
        geom: Earth Engine geometry
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        scale_m: Scale in meters for reduction
        
    Returns:
        DataFrame with monthly burned area percentage data
    """
    from datetime import datetime, timedelta
    import time

    pref = f"[{region_id}] " if region_id else ""

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    
    all_dfs = []
    
    # Process in 2-year chunks to avoid memory limits and rate limits (more conservative)
    current_start = start_dt
    chunk_years = 2
    total_chunks = ((end_dt.year - start_dt.year) // chunk_years) + 1
    current_chunk = 0
    
    while current_start < end_dt:
        # Process 2 years at a time
        current_end = min(
            datetime(current_start.year + chunk_years, 1, 1),
            end_dt
        )
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_chunk += 1

        max_retries = 3
        chunk_success = False
        for attempt in range(max_retries):
            try:
                months = month_starts(chunk_start, chunk_end)

                def per_month(m):
                    m = ee.Date(m)
                    ic = (
                        ee.ImageCollection(BURN_COLLECTION_ID)
                        .filterDate(m, m.advance(1, "month"))
                        .select(["BurnDate"])  # days since Jan 1 if burned, 0 otherwise
                    )
                    size = ic.size()
                    burned_mask = ee.Image(
                        ee.Algorithms.If(
                            size.gt(0),
                            ic.max().gt(0).unmask(0).rename(["burned_mask"]),  # data case 0/1
                            ee.Image.constant(0).updateMask(ee.Image(0)).rename(["burned_mask"])  # no-data -> masked
                        )
                    )
                    return burned_mask.set({"system:time_start": m.millis(), "month": m.get("month")})

                ic = ee.ImageCollection.fromImages(months.map(per_month))

                df = _reduce_ic_to_df(
                    ic,
                    geom,
                    scale_m,
                    band_map={"burned_mask": "burned_area_pct"},
                )
                if df is not None and not df.empty:
                    df["burned_area_pct"] = df["burned_area_pct"] * 100.0
                    all_dfs.append(df)
                chunk_success = True
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = min(60, 10 * (2 ** attempt))
                    print(f"✗ (retry {attempt + 1}/{max_retries} in {wait_time}s)", end=" ", flush=True)
                    time.sleep(wait_time)
                else:
                    print(f"✗ Error: {e}", flush=True)
        
        # Sleep between chunks
        if current_start < end_dt:
            time.sleep(3)
        
        current_start = current_end
    
    if not all_dfs:
        return pd.DataFrame(columns=["date", "burned_area_pct"])
    
    return pd.concat(all_dfs, ignore_index=True).sort_values("date").reset_index(drop=True)

def ndvi_16day_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 250, region_id: Optional[str] = None) -> pd.DataFrame:
    """
    Compute 16-day NDVI per region from MOD13Q1 (250m, 16-day composites).
    One row per 16-day period start; uses mean of available scenes in each window.
    """
    from datetime import datetime, timedelta
    import time

    pref = f"[{region_id}] " if region_id else ""

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    all_recs = []
    period_days = 16
    current = start_dt

    # Count total periods for progress
    _t = start_dt
    total_periods = 0
    while _t < end_dt:
        _t += timedelta(days=period_days)
        total_periods += 1
    period_idx = 0

    while current < end_dt:
        window_end = min(current + timedelta(days=period_days), end_dt)
        ws = current.strftime("%Y-%m-%d")
        we = window_end.strftime("%Y-%m-%d")
        period_idx += 1
        try:
            ic = (
                ee.ImageCollection(NDVI_COLLECTION_ID)
                .filterDate(ws, we)
                .select(["NDVI"])
            )
            
            # Get size with timeout
            n = None
            for timeout_attempt in range(3):
                try:
                    n = get_info_with_timeout(ic.size())
                    break
                except concurrent.futures.TimeoutError:
                    if timeout_attempt < 2:
                        delay = 30 * (2 ** timeout_attempt)
                        time.sleep(delay)
                    else:
                        print(f"    {pref}MODIS NDVI 16-day {ws}: ✗ GEE timeout after 3 attempts", flush=True)
                        raise
            
            if n == 0:
                all_recs.append({"date": ws, "ndvi_mean": None})
            else:
                img = ic.mean().multiply(0.0001).rename(["ndvi"])
                
                # Get reduce region info with timeout
                reduce_obj = img.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=scale_m,
                    bestEffort=True,
                    maxPixels=1_000_000_000,
                    tileScale=8,
                )
                info = None
                for timeout_attempt in range(3):
                    try:
                        info = get_info_with_timeout(reduce_obj)
                        break
                    except concurrent.futures.TimeoutError:
                        if timeout_attempt < 2:
                            delay = 30 * (2 ** timeout_attempt)
                            time.sleep(delay)
                        else:
                            print(f"    {pref}MODIS NDVI 16-day {ws}: ✗ GEE timeout after 3 attempts", flush=True)
                            raise
                
                v = info.get("ndvi") if info else None
                all_recs.append({"date": ws, "ndvi_mean": float(v) if v is not None else None})
        except Exception as e:
            print(f"    {pref}MODIS NDVI 16-day {ws}: ✗ Error: {e}", flush=True)
            all_recs.append({"date": ws, "ndvi_mean": None})
        current = window_end
        if current < end_dt:
            time.sleep(1)

    if not all_recs:
        return pd.DataFrame(columns=["date", "ndvi_mean"])
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)


def modis_16day_combined_df(geom: ee.Geometry, start: str, end: str, region_id: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
    """
    Fetch MODIS at 16-day resolution: NDVI from MOD13Q1, burned area from MCD64A1 (monthly).
    Each 16-day period gets ndvi_mean; burned_area_pct is from the containing month.
    """
    df_ndvi = ndvi_16day_df(geom, start, end, scale_m=250, region_id=region_id)
    df_burn = burned_area_pct_monthly_df(geom, start, end, scale_m=500, region_id=region_id)

    if df_ndvi is None or df_ndvi.empty:
        return df_burn if df_burn is not None and not df_burn.empty else pd.DataFrame(columns=["date", "ndvi_mean", "burned_area_pct"])
    if df_burn is None or df_burn.empty:
        df_ndvi["burned_area_pct"] = None
        return df_ndvi

    # NDVI dates are YYYY-MM-DD; burned area from GEE is YYYY-MM (monthly)
    df_ndvi["date"] = pd.to_datetime(df_ndvi["date"], format="%Y-%m-%d")
    df_burn["date"] = pd.to_datetime(df_burn["date"], format="%Y-%m")
    df_burn["month"] = df_burn["date"].dt.to_period("M")
    df_ndvi["month"] = df_ndvi["date"].dt.to_period("M")
    out = df_ndvi.merge(df_burn[["month", "burned_area_pct"]].drop_duplicates("month"), on="month", how="left")
    out = out.drop(columns=["month"]).sort_values("date").reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[["date", "ndvi_mean", "burned_area_pct"]]


def get_modis_region_df(
    region_name: str,
    start: str = "2000-02-18",  # MOD13Q1 start; MCD64A1 starts ~2000-11
    end: str = "2025-09-01",
) -> pd.DataFrame:
    """
    Get MODIS data for a specific region.
    
    Args:
        region_name: Name of the region
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with MODIS data for the region
    """
    geom = regions_ee()[region_name]
    df_ndvi = ndvi_monthly_df(geom, start, end, scale_m=250)
    df_burn = burned_area_pct_monthly_df(geom, start, end, scale_m=500)

    if df_ndvi is None or df_ndvi.empty:
        base = df_burn
    elif df_burn is None or df_burn.empty:
        base = df_ndvi
    else:
        base = pd.merge(df_ndvi, df_burn, on="date", how="outer")

    if base is None or base.empty:
        return pd.DataFrame(columns=["date", "ndvi_mean", "burned_area_pct", "region"])

    base = base.sort_values("date").reset_index(drop=True)
    base["region"] = region_name
    return base[["date", "ndvi_mean", "burned_area_pct", "region"]]

def get_all_regions_modis_df(start: str = "2000-02-18", end: str = "2025-09-01") -> pd.DataFrame:
    """
    Get MODIS data for all regions.
    
    Args:
        start: Start date (YYYY-MM-DD)
        end: End date (YYYY-MM-DD)
        
    Returns:
        DataFrame with MODIS data for all regions
    """
    frames = []
    for name, geom in regions_ee().items():
        print(f"Processing region: {name}")
        df_ndvi = ndvi_monthly_df(geom, start, end, scale_m=250)
        df_burn = burned_area_pct_monthly_df(geom, start, end, scale_m=500)

        if df_ndvi is None or df_ndvi.empty:
            combined = df_burn
        elif df_burn is None or df_burn.empty:
            combined = df_ndvi
        else:
            combined = pd.merge(df_ndvi, df_burn, on="date", how="outer")

        if combined is None or combined.empty:
            print(f"No data for region: {name}")
            continue

        combined["region"] = name
        frames.append(combined[["date", "ndvi_mean", "burned_area_pct", "region"]])

    if not frames:
        print("No data collected for any region.")
        return pd.DataFrame(columns=["date", "ndvi_mean", "burned_area_pct", "region"])

    out = pd.concat(frames, ignore_index=True)
    print("Data collection complete.")
    return out.sort_values(["region", "date"]).reset_index(drop=True)
