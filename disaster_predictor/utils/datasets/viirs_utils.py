import os
import time
import datetime
import concurrent.futures
import pandas as pd
import ee
from typing import Dict, Any, List, Optional
from ..earth_engine_utils import regions_ee, standard_execution_flow, get_info_with_timeout

VIIRS_COLLECTION_ID = "NASA/VIIRS/002/VNP14A1"

def viirs_monthly_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 375) -> pd.DataFrame:
    """
    Monthly aggregation per region done server-side.
    Processes months SEQUENTIALLY (one at a time) to avoid "Too many concurrent aggregations" errors.
    This is slower but more reliable than parallel processing.
    """
    start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end, "%Y-%m-%d")
    
    all_records = []
    
    # Generate all months to process
    months_py = pd.date_range(start=start_dt, end=end_dt, freq="MS", inclusive="left").to_pydatetime().tolist()
    total_months = len(months_py)
    
    print(f"    Processing {total_months} months sequentially...", end=" ", flush=True)
    
    # Process each month individually (sequential, not parallel)
    for month_idx, month_dt in enumerate(months_py, 1):
        month_str = month_dt.strftime("%Y-%m-%d")
        month_ee = ee.Date(month_str)
        
        # Retry logic for each month
        max_retries = 3
        month_success = False
        
        for attempt in range(max_retries):
            try:
                # Get images for this month
                ic = (
                    ee.ImageCollection(VIIRS_COLLECTION_ID)
                    .filterDate(month_ee, month_ee.advance(1, "month"))
                    .select(["FireMask", "MaxFRP"])
                )
                
                # Get size with timeout
                size = None
                for timeout_attempt in range(3):
                    try:
                        size = get_info_with_timeout(ic.size())
                        break
                    except concurrent.futures.TimeoutError:
                        if timeout_attempt < 2:
                            delay = 30 * (2 ** timeout_attempt)
                            if month_idx % 10 == 0:
                                print(f"\n    Month {month_idx}/{total_months} GEE timeout, retry {timeout_attempt + 1}/3 in {delay}s...", end=" ", flush=True)
                            time.sleep(delay)
                        else:
                            if month_idx % 10 == 0:
                                print(f"\n    ✗ Month {month_idx}/{total_months} GEE timeout after 3 attempts", flush=True)
                            raise
                
                if size == 0:
                    # No data for this month
                    all_records.append({
                        "date": month_dt.strftime("%Y-%m"),
                        "hotspot_count": None,
                        "frp_mean": None,
                    })
                    month_success = True
                    break
                
                # Create hotspot count image (pixels where FireMask > 7)
                hot_sum_img = (
                    ic.map(lambda img: img.select("FireMask").gt(7).rename(["hot"]))
                    .sum()
                    .rename(["hot"])
                )
                
                # Create FRP mean image (only for pixels where FireMask > 7)
                frp_mean_img = (
                    ic.map(lambda img: img.select("MaxFRP").updateMask(img.select("FireMask").gt(7)))
                    .mean()
                    .rename(["frp_mean"])
                )
                
                # Reduce region for hotspot count with timeout
                hot_count = None
                hot_reduce = hot_sum_img.reduceRegion(
                    reducer=ee.Reducer.sum(),
                    geometry=geom,
                    scale=scale_m,
                    bestEffort=True,
                    maxPixels=1_000_000_000,
                    tileScale=8,
                ).get("hot")
                for timeout_attempt in range(3):
                    try:
                        hot_count = get_info_with_timeout(hot_reduce)
                        break
                    except concurrent.futures.TimeoutError:
                        if timeout_attempt < 2:
                            delay = 30 * (2 ** timeout_attempt)
                            if month_idx % 10 == 0:
                                print(f"\n    Month {month_idx}/{total_months} GEE timeout, retry {timeout_attempt + 1}/3 in {delay}s...", end=" ", flush=True)
                            time.sleep(delay)
                        else:
                            if month_idx % 10 == 0:
                                print(f"\n    ✗ Month {month_idx}/{total_months} GEE timeout after 3 attempts", flush=True)
                            raise
                
                # Reduce region for FRP mean with timeout
                frp_mean_val = None
                frp_reduce = frp_mean_img.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=scale_m,
                    bestEffort=True,
                    maxPixels=1_000_000_000,
                    tileScale=8,
                ).get("frp_mean")
                for timeout_attempt in range(3):
                    try:
                        frp_mean_val = get_info_with_timeout(frp_reduce)
                        break
                    except concurrent.futures.TimeoutError:
                        if timeout_attempt < 2:
                            delay = 30 * (2 ** timeout_attempt)
                            if month_idx % 10 == 0:
                                print(f"\n    Month {month_idx}/{total_months} GEE timeout, retry {timeout_attempt + 1}/3 in {delay}s...", end=" ", flush=True)
                            time.sleep(delay)
                        else:
                            if month_idx % 10 == 0:
                                print(f"\n    ✗ Month {month_idx}/{total_months} GEE timeout after 3 attempts", flush=True)
                            raise
                
                # Store result
                all_records.append({
                    "date": month_dt.strftime("%Y-%m"),
                    "hotspot_count": float(hot_count) if hot_count is not None else None,
                    "frp_mean": float(frp_mean_val) if frp_mean_val is not None else None,
                })
                
                month_success = True
                break
                
            except Exception as e:
                error_str = str(e).lower()
                is_rate_limit = ('429' in error_str or 'quota' in error_str or 
                               'rate limit' in error_str or 'too many concurrent' in error_str or
                               'timeout' in error_str)
                
                if attempt < max_retries - 1:
                    delay = min(60, 10 * (2 ** attempt))
                    if is_rate_limit:
                        delay = max(delay, 30)  # At least 30s for rate limits
                    if month_idx % 10 == 0:  # Only print every 10th month to avoid spam
                        print(f"\n    Month {month_idx}/{total_months} retry {attempt + 1}/{max_retries} in {delay}s...", end=" ", flush=True)
                    time.sleep(delay)
                else:
                    # Failed after retries, add None record and continue
                    if month_idx % 10 == 0:
                        print(f"\n    ✗ Month {month_idx}/{total_months} failed after {max_retries} attempts: {e}")
                    all_records.append({
                        "date": month_dt.strftime("%Y-%m"),
                        "hotspot_count": None,
                        "frp_mean": None,
                    })
                    break
        
        # Small sleep between months to avoid rate limits
        if month_idx < total_months:
            time.sleep(1)
        
        # Progress indicator every 12 months
        if month_idx % 12 == 0:
            print(f"\n    Progress: {month_idx}/{total_months} months...", end=" ", flush=True)
    
    print(f"✓ ({len([r for r in all_records if r.get('hotspot_count') is not None])} months with data)")

    return pd.DataFrame(all_records).sort_values("date").reset_index(drop=True) if all_records else pd.DataFrame(columns=["date", "hotspot_count", "frp_mean"])

def fetch_viirs_daily(geom: ee.Geometry, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
    """
    Get daily VIIRS fire data for a specific geometry and date range.
    Iterates one day at a time to avoid "too many concurrent aggregations".
    Pass region_id in kwargs (e.g. from process_with_per_subregion_save) to prefix progress with region.
    """
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # Treat end_date as inclusive at the function boundary.
    end_dt = end_dt + timedelta(days=1)
    all_recs = []
    total_days = (end_dt - start_dt).days
    day_idx = 0
    current = start_dt
    sleep_s = 0.2

    DAY_RETRIES = 3
    DAY_RETRY_DELAYS = (5, 15, 45)  # seconds

    while current < end_dt:
        day_str = current.strftime("%Y-%m-%d")
        day_idx += 1
        day_ok = False
        for day_attempt in range(DAY_RETRIES):
            try:
                d = ee.Date(day_str)
                ic = (
                    ee.ImageCollection(VIIRS_COLLECTION_ID)
                    .filterDate(d.advance(-1, "day"), d.advance(1, "day"))
                    .select(["FireMask", "MaxFRP"])
                )
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
                            raise
                if n == 0:
                    all_recs.append({"date": day_str, "hotspot_count": 0.0, "frp_mean": 0.0})
                else:
                    img = ic.sort("system:time_start", False).first()
                    mask = img.select("FireMask").gt(7)
                    cnt_reduce = mask.reduceRegion(
                        reducer=ee.Reducer.sum(),
                        geometry=geom,
                        scale=375,
                        bestEffort=True,
                        maxPixels=1_000_000_000,
                        tileScale=8,
                    )
                    cnt_info = None
                    for timeout_attempt in range(3):
                        try:
                            cnt_info = get_info_with_timeout(cnt_reduce)
                            break
                        except concurrent.futures.TimeoutError:
                            if timeout_attempt < 2:
                                delay = 30 * (2 ** timeout_attempt)
                                time.sleep(delay)
                            else:
                                raise
                    frp_reduce = (
                        img.select("MaxFRP")
                        .updateMask(mask)
                        .reduceRegion(
                            reducer=ee.Reducer.mean(),
                            geometry=geom,
                            scale=375,
                            bestEffort=True,
                            maxPixels=1_000_000_000,
                            tileScale=8,
                        )
                    )
                    frp_info = None
                    for timeout_attempt in range(3):
                        try:
                            frp_info = get_info_with_timeout(frp_reduce)
                            break
                        except concurrent.futures.TimeoutError:
                            if timeout_attempt < 2:
                                delay = 30 * (2 ** timeout_attempt)
                                time.sleep(delay)
                            else:
                                raise
                    h = cnt_info.get("FireMask")
                    fm = frp_info.get("MaxFRP")
                    if h is None or h == 0 or h > 10000:
                        h, fm = 0.0, 0.0
                    else:
                        h = float(h)
                        fm = float(fm) if fm is not None else 0.0
                    all_recs.append({"date": day_str, "hotspot_count": h, "frp_mean": fm})
                day_ok = True
                break
            except Exception as e:
                if day_attempt < DAY_RETRIES - 1:
                    delay = DAY_RETRY_DELAYS[min(day_attempt, len(DAY_RETRY_DELAYS) - 1)]
                    time.sleep(delay)
                else:
                    all_recs.append({"date": day_str, "hotspot_count": None, "frp_mean": None})

        current += timedelta(days=1)
        if current < end_dt:
            time.sleep(sleep_s)

    if not all_recs:
        return pd.DataFrame(columns=["date", "hotspot_count", "frp_mean"])
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_viirs_data(region_name: str, start_date: str, end_date: str, daily: bool = True) -> pd.DataFrame:
    """Fetch VIIRS data for a region."""
    geom = regions_ee()[region_name]
    
    if not daily:
        df = viirs_monthly_df(geom, start_date, end_date)
        if not df.empty:
            df["region"] = region_name
        return df

    df = fetch_viirs_daily(geom, start_date, end_date)
    
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "hotspot_count", "frp_mean", "region"])
    
    df["region"] = region_name
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)

def fetch_all_regions_viirs(start_date: str, end_date: str, daily: bool = True) -> pd.DataFrame:
    """Fetch VIIRS data for all regions with retry logic for rate limits."""
    frames = []
    region_names = list(regions_ee().keys())
    
    for i, region_name in enumerate(region_names):
        print(f"Processing region: {region_name}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = fetch_viirs_data(region_name, start_date, end_date, daily=daily)
                if df is not None and not df.empty:
                    frames.append(df)
                    break
            except Exception as e:
                error_msg = str(e).lower()
                if 'too many concurrent aggregations' in error_msg or '429' in error_msg:
                    if attempt < max_retries - 1:
                        wait_time = min(30, 5 * (2 ** attempt))
                        print(f"  Rate limit hit, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"  Failed after {max_retries} attempts: {e}")
                else:
                    print(f"  Error: {e}")
                    break
        
        if i < len(region_names) - 1:
            time.sleep(2)
    
    if not frames:
        return pd.DataFrame(columns=["date", "hotspot_count", "frp_mean", "region"])
    return pd.concat(frames, ignore_index=True).sort_values(["region", "date"])
