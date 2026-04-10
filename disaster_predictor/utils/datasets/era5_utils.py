import os
import time
import pandas as pd
import numpy as np
import ee
import concurrent.futures
from typing import Dict, Any, Optional
from scipy.stats import gamma, norm
from ..earth_engine_utils import (
    month_starts,
    _reduce_ic_to_df,
    regions_ee,
    standard_execution_flow,
    get_info_with_timeout,
)
from ..bq_utils import load_from_bigquery, save_to_bigquery

def era5_temp_precip_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 25000) -> pd.DataFrame:
    """Get ERA5 temperature and precipitation data aggregated by month."""
    months = month_starts(start, end)

    def per_m(m):
        m = ee.Date(m)
        ic = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterDate(m, m.advance(1, "month"))
            .select(["temperature_2m", "total_precipitation"])
        )
        temp = ic.select("temperature_2m").mean()
        precip = ic.select("total_precipitation").sum()
        img = temp.addBands(precip).rename(["temperature_2m", "total_precipitation"])
        return img.set({"system:time_start": m.millis(), "month": m.get("month")})

    ic = ee.ImageCollection.fromImages(months.map(per_m))

    df = _reduce_ic_to_df(
        ic,
        geom,
        scale_m,
        band_map={
            "temperature_2m": "temp_2m_mean_C",
            "total_precipitation": "precipitation_sum_mm",
        },
    )
    if not df.empty:
        df["temp_2m_mean_C"] = df["temp_2m_mean_C"] - 273.15
    return df

def soil_moisture_mean_df(geom: ee.Geometry, start: str, end: str, scale_m: int = 9000) -> pd.DataFrame:
    """Get soil moisture data aggregated by month."""
    months = month_starts(start, end)

    def per_m(m):
        m = ee.Date(m)
        ic = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterDate(m, m.advance(1, "month"))
            .select(["volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2"])
        )
        mean_img = ic.mean()
        return mean_img.set({"system:time_start": m.millis(), "month": m.get("month")})

    mcoll = ee.ImageCollection.fromImages(months.map(per_m))

    df = _reduce_ic_to_df(
        mcoll,
        geom,
        scale_m,
        band_map={
            "volumetric_soil_water_layer_1": "sm1_mean",
            "volumetric_soil_water_layer_2": "sm2_mean",
        },
    )
    return df


def fetch_era5_monthly_temp_precip(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 25000) -> pd.DataFrame:
    """
    Get monthly ERA5 temperature and precipitation.
    Much faster than daily - processes ~528 months instead of 16,000 days.
    Processes in 5-year chunks to avoid memory limits.
    """
    import pandas as pd
    from datetime import datetime, timedelta
    from ..earth_engine_utils import month_starts
    import time
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_recs = []
    
    # Process in 5-year chunks to avoid memory limits
    current_start = start_dt
    chunk_years = 5
    total_chunks = ((end_dt.year - start_dt.year) // chunk_years) + 1
    current_chunk = 0
    
    while current_start < end_dt:
        current_end = min(
            datetime(current_start.year + chunk_years, 1, 1),
            end_dt
        )
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_chunk += 1
        
        
        # Retry logic for chunk processing
        chunk_success = False
        max_chunk_retries = 3
        for chunk_attempt in range(max_chunk_retries):
            try:
                months = month_starts(chunk_start, chunk_end)
                
                def per_month(m):
                    m = ee.Date(m)
                    ic = (
                        ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                        .filterDate(m, m.advance(1, "month"))
                        .select(["temperature_2m", "total_precipitation"])
                    )
                    temp = ic.select("temperature_2m").mean()
                    precip = ic.select("total_precipitation").sum()
                    img = temp.addBands(precip).rename(["temperature_2m", "total_precipitation"])
                    return img.set({"system:time_start": m.millis(), "date": m.format("YYYY-MM-01")})
                
                ic = ee.ImageCollection.fromImages(months.map(per_month))
                
                def mapper(img):
                    stats = img.reduceRegion(
                        reducer=ee.Reducer.mean(),
                        geometry=geom,
                        scale=scale_m,
                        bestEffort=True,
                        maxPixels=1_000_000_000,
                        tileScale=8
                    )
                    return ee.Feature(None, {
                        "date": img.get("date"),
                        "temperature_2m": stats.get("temperature_2m"),
                        "total_precipitation": stats.get("total_precipitation")
                    })
                
                fc = ee.FeatureCollection(ic.map(mapper))
                rows = fc.getInfo()["features"]
                
                chunk_count = 0
                for f in rows:
                    p = f["properties"]
                    rec = {"date": p["date"]}
                    rec["temp_2m_mean_C"] = float(p["temperature_2m"]) - 273.15 if p["temperature_2m"] is not None else None
                    rec["precipitation_sum_mm"] = float(p["total_precipitation"]) if p["total_precipitation"] is not None else None
                    all_recs.append(rec)
                    chunk_count += 1
                
                chunk_success = True
                break
            except Exception as e:
                if chunk_attempt < max_chunk_retries - 1:
                    delay = min(10 * (2 ** chunk_attempt), 60)  # 10s, 20s, 40s max
                    time.sleep(delay)
                else:
                    print(f"    ✗ Error after {max_chunk_retries} attempts: {e}", flush=True)
        
        # Small sleep between chunks
        if current_start < end_dt:
            time.sleep(2)
        
        current_start = current_end
    
    if not all_recs:
        return pd.DataFrame(columns=["date", "temp_2m_mean_C", "precipitation_sum_mm"])
    
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_daily_temp_precip(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 25000) -> pd.DataFrame:
    """Get daily ERA5 temperature and precipitation. Processes in yearly chunks to avoid memory limits."""
    import pandas as pd
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # Treat end_date as inclusive at the function boundary.
    # Internally we use half-open [start, end) ranges, so add one day.
    end_dt = end_dt + timedelta(days=1)

    all_recs = []
    total_years = max(1, (end_dt.year - start_dt.year) + 1)
    current_year = 0
    current_start = start_dt

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=365), end_dt)
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_year += 1
        year_label = current_start.strftime("%Y")

        start = ee.Date(chunk_start)
        end = ee.Date(chunk_end)
        days = end.difference(start, "day").int()
        day_list = ee.List.sequence(0, days.subtract(1)).map(lambda d: start.advance(d, "day"))

        def per_day(day):
            day = ee.Date(day)
            ic = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(day, day.advance(1, "day")).select(["temperature_2m", "total_precipitation"])
            temp = ic.select("temperature_2m").mean()
            precip = ic.select("total_precipitation").sum()
            img = temp.addBands(precip).rename(["temperature_2m", "total_precipitation"])
            return img.set({"system:time_start": day.millis(), "date": day.format("YYYY-MM-dd")})

        ic = ee.ImageCollection.fromImages(day_list.map(per_day))

        def mapper(img):
            stats = img.reduceRegion(reducer=ee.Reducer.mean(), geometry=geom, scale=scale_m, bestEffort=True, maxPixels=1_000_000_000, tileScale=8)
            return ee.Feature(None, stats).set("date", img.get("date"))

        fc = ee.FeatureCollection(ic.map(mapper))
        rows = None
        for timeout_attempt in range(3):
            try:
                rows = get_info_with_timeout(fc)["features"]
                break
            except concurrent.futures.TimeoutError:
                if timeout_attempt < 2:
                    delay = 30 * (2 ** timeout_attempt)
                    time.sleep(delay)
                else:
                    pass

        if rows is not None:
            try:
                for f in rows:
                    p = f["properties"]
                    rec = {"date": p.get("date")}
                    t2m = p.get("temperature_2m")
                    precip = p.get("total_precipitation")
                    rec["temp_2m_mean_C"] = float(t2m) - 273.15 if t2m is not None else None
                    rec["precipitation_sum_mm"] = float(precip) if precip is not None else None
                    all_recs.append(rec)
                pass
            except Exception as e:
                print(f"    ✗ Error processing year {year_label}: {e}", flush=True)

        if current_start < end_dt:
            time.sleep(2)
        current_start = current_end

    if not all_recs:
        return pd.DataFrame(columns=["date", "temp_2m_mean_C", "precipitation_sum_mm"])
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_monthly_soil_moisture(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 9000) -> pd.DataFrame:
    """
    Get monthly ERA5 soil moisture.
    Much faster than daily - processes ~528 months instead of 16,000 days.
    Processes in 5-year chunks to avoid memory limits.
    """
    import pandas as pd
    from datetime import datetime, timedelta
    from ..earth_engine_utils import month_starts
    import time
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_recs = []
    
    # Process in 5-year chunks to avoid memory limits
    current_start = start_dt
    chunk_years = 5
    total_chunks = ((end_dt.year - start_dt.year) // chunk_years) + 1
    current_chunk = 0
    
    while current_start < end_dt:
        current_end = min(
            datetime(current_start.year + chunk_years, 1, 1),
            end_dt
        )
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_chunk += 1
        
        
        try:
            months = month_starts(chunk_start, chunk_end)
            
            def per_month(m):
                m = ee.Date(m)
                ic = (
                    ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                    .filterDate(m, m.advance(1, "month"))
                    .select(["volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2"])
                )
                mean_img = ic.mean()
                return mean_img.set({"system:time_start": m.millis(), "date": m.format("YYYY-MM-01")})
            
            ic = ee.ImageCollection.fromImages(months.map(per_month))
            
            def mapper(img):
                stats = img.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=scale_m,
                    bestEffort=True,
                    maxPixels=1_000_000_000,
                    tileScale=8
                )
                return ee.Feature(None, {
                    "date": img.get("date"),
                    "volumetric_soil_water_layer_1": stats.get("volumetric_soil_water_layer_1"),
                    "volumetric_soil_water_layer_2": stats.get("volumetric_soil_water_layer_2")
                })
            
            fc = ee.FeatureCollection(ic.map(mapper))
            rows = fc.getInfo()["features"]
            
            chunk_count = 0
            for f in rows:
                p = f["properties"]
                rec = {"date": p["date"]}
                rec["sm1_mean"] = float(p["volumetric_soil_water_layer_1"]) if p["volumetric_soil_water_layer_1"] is not None else None
                rec["sm2_mean"] = float(p["volumetric_soil_water_layer_2"]) if p["volumetric_soil_water_layer_2"] is not None else None
                all_recs.append(rec)
                chunk_count += 1
            
        except Exception as e:
            print(f"    ✗ Error processing chunk: {e}", flush=True)
        
        # Small sleep between chunks
        if current_start < end_dt:
            time.sleep(2)
        
        current_start = current_end
    
    if not all_recs:
        return pd.DataFrame(columns=["date", "sm1_mean", "sm2_mean"])
    
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_daily_soil_moisture(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 9000) -> pd.DataFrame:
    """Get daily ERA5 soil moisture. Processes in yearly chunks to avoid memory limits."""
    import pandas as pd
    from datetime import datetime, timedelta

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # Treat end_date as inclusive at the function boundary.
    end_dt = end_dt + timedelta(days=1)

    all_recs = []
    total_years = (end_dt.year - start_dt.year) + 1
    current_year = 0
    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=365), end_dt)
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_year += 1
        year_label = current_start.strftime("%Y")

        start = ee.Date(chunk_start)
        end = ee.Date(chunk_end)
        days = end.difference(start, "day").int()
        day_list = ee.List.sequence(0, days.subtract(1)).map(lambda d: start.advance(d, "day"))

        def per_day(day):
            day = ee.Date(day)
            ic = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY").filterDate(day, day.advance(1, "day")).select(["volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2"])
            mean_img = ic.mean()
            return mean_img.set({"system:time_start": day.millis(), "date": day.format("YYYY-MM-dd")})

        ic = ee.ImageCollection.fromImages(day_list.map(per_day))

        def mapper(img):
            stats = img.reduceRegion(reducer=ee.Reducer.mean(), geometry=geom, scale=scale_m, bestEffort=True, maxPixels=1_000_000_000, tileScale=8)
            return ee.Feature(None, stats).set("date", img.get("date"))

        fc = ee.FeatureCollection(ic.map(mapper))
        rows = None
        for timeout_attempt in range(3):
            try:
                rows = get_info_with_timeout(fc)["features"]
                break
            except concurrent.futures.TimeoutError:
                if timeout_attempt < 2:
                    delay = 30 * (2 ** timeout_attempt)
                    time.sleep(delay)
                else:
                    pass

        if rows is not None:
            try:
                for f in rows:
                    p = f["properties"]
                    rec = {"date": p.get("date")}
                    sm1 = p.get("volumetric_soil_water_layer_1")
                    sm2 = p.get("volumetric_soil_water_layer_2")
                    rec["sm1_mean"] = float(sm1) if sm1 is not None else None
                    rec["sm2_mean"] = float(sm2) if sm2 is not None else None
                    all_recs.append(rec)
            except Exception as e:
                print(f"✗ Error: {e}", flush=True)

        if current_start < end_dt:
            time.sleep(0.5)
        current_start = current_end

    if not all_recs:
        return pd.DataFrame(columns=["date", "sm1_mean", "sm2_mean"])
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_monthly_runoff(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 11000) -> pd.DataFrame:
    """
    Get monthly ERA5 runoff data from ERA5-Land Daily Aggregates.
    Much faster than daily - processes ~528 months instead of 16,000 days.
    Processes in 5-year chunks to avoid memory limits.
    
    Uses ERA5_LAND/DAILY_AGGR and sums daily values to monthly totals.
    """
    import pandas as pd
    from datetime import datetime, timedelta
    from ..earth_engine_utils import month_starts
    import time
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    all_recs = []
    
    # Process in 5-year chunks to avoid memory limits
    current_start = start_dt
    chunk_years = 5
    total_chunks = ((end_dt.year - start_dt.year) // chunk_years) + 1
    current_chunk = 0
    
    while current_start < end_dt:
        current_end = min(
            datetime(current_start.year + chunk_years, 1, 1),
            end_dt
        )
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_chunk += 1
        
        
        try:
            months = month_starts(chunk_start, chunk_end)
            
            def per_month(m):
                m = ee.Date(m)
                ic = (
                    ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
                    .filterDate(m, m.advance(1, "month"))
                    .select(["surface_runoff_sum", "sub_surface_runoff_sum"])
                )
                # Sum all daily values for the month
                surface_sum = ic.select("surface_runoff_sum").sum()
                subsurface_sum = ic.select("sub_surface_runoff_sum").sum()
                img = surface_sum.addBands(subsurface_sum).rename(["surface_runoff_sum", "sub_surface_runoff_sum"])
                return img.set({"system:time_start": m.millis(), "date": m.format("YYYY-MM-01")})
            
            ic = ee.ImageCollection.fromImages(months.map(per_month))
            
            def mapper(img):
                stats = img.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=scale_m,
                    bestEffort=True,
                    maxPixels=1_000_000_000,
                    tileScale=8
                )
                date = img.get("date")
                return ee.Feature(None, stats).set("date", date)
            
            fc = ee.FeatureCollection(ic.map(mapper))
            rows = fc.getInfo()["features"]
            
            chunk_count = 0
            for f in rows:
                p = f["properties"]
                surf = p.get("surface_runoff_sum")
                subsurf = p.get("sub_surface_runoff_sum")
                surf_mm = float(surf) * 1000 if surf is not None else None
                subsurf_mm = float(subsurf) * 1000 if subsurf is not None else None
                tot_mm = (surf_mm + subsurf_mm) if (surf_mm is not None and subsurf_mm is not None) else None
                
                all_recs.append({
                    "date": p.get("date"),
                    "surface_runoff_mm": surf_mm,
                    "subsurface_runoff_mm": subsurf_mm,
                    "total_runoff_mm": tot_mm
                })
                chunk_count += 1
            
        except Exception as e:
            print(f"    ✗ Error processing chunk: {e}", flush=True)
        
        # Small sleep between chunks
        if current_start < end_dt:
            time.sleep(2)
        
        current_start = current_end
    
    if not all_recs:
        return pd.DataFrame(columns=["date", "surface_runoff_mm", "subsurface_runoff_mm", "total_runoff_mm"])
    
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_daily_runoff(geom: ee.Geometry, start_date: str, end_date: str, scale_m: int = 11000) -> pd.DataFrame:
    """
    Get daily runoff data from ERA5-Land Daily Aggregates.
    Processes in yearly chunks to avoid memory limits.
    
    Useful for flood risk assessment. Uses ERA5_LAND/DAILY_AGGR which provides
    pre-aggregated daily sums for surface and subsurface runoff.
    """
    import pandas as pd
    from datetime import datetime, timedelta
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # Treat end_date as inclusive at the function boundary.
    end_dt = end_dt + timedelta(days=1)
    
    all_recs = []
    
    # Process in yearly chunks to avoid memory limits
    total_years = (end_dt.year - start_dt.year) + 1
    current_year = 0
    current_start = start_dt
    while current_start < end_dt:
        # Process one year at a time
        current_end = min(current_start + timedelta(days=365), end_dt)
        chunk_start = current_start.strftime("%Y-%m-%d")
        chunk_end = current_end.strftime("%Y-%m-%d")
        current_year += 1
        year_label = current_start.strftime("%Y")

        ic = (
            ee.ImageCollection("ECMWF/ERA5_LAND/DAILY_AGGR")
            .filterDate(chunk_start, chunk_end)
            .select(["surface_runoff_sum", "sub_surface_runoff_sum"])
        )

        def mapper(img):
            stats = img.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom,
                scale=scale_m,
                bestEffort=True,
                maxPixels=1_000_000_000,
                tileScale=8
            )
            date = ee.Date(img.get("system:time_start")).format("YYYY-MM-dd")
            return ee.Feature(None, stats).set("date", date)

        fc = ee.FeatureCollection(ic.map(mapper))
        rows = None
        for timeout_attempt in range(3):
            try:
                rows = get_info_with_timeout(fc)["features"]
                break
            except concurrent.futures.TimeoutError:
                if timeout_attempt < 2:
                    delay = 30 * (2 ** timeout_attempt)
                    time.sleep(delay)
                else:
                    pass

        if rows is not None:
            try:
                for f in rows:
                    p = f["properties"]
                    surf = p.get("surface_runoff_sum")
                    subsurf = p.get("sub_surface_runoff_sum")
                    surf_mm = float(surf) * 1000 if surf is not None else None
                    subsurf_mm = float(subsurf) * 1000 if subsurf is not None else None
                    tot_mm = (surf_mm + subsurf_mm) if (surf_mm is not None and subsurf_mm is not None) else None
                    
                    all_recs.append({
                        "date": p.get("date"),
                        "surface_runoff_mm": surf_mm,
                        "subsurface_runoff_mm": subsurf_mm,
                        "total_runoff_mm": tot_mm
                    })
            except Exception as e:
                print(f"✗ Error: {e}", flush=True)

        if current_start < end_dt:
            time.sleep(0.5)
        current_start = current_end

    if not all_recs:
        return pd.DataFrame(columns=["date", "surface_runoff_mm", "subsurface_runoff_mm", "total_runoff_mm"])
    return pd.DataFrame(all_recs).sort_values("date").reset_index(drop=True)

def fetch_era5_monthly(geom: ee.Geometry, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get monthly ERA5 data by combining temp/precip and soil moisture.
    
    Uses monthly aggregation for much faster processing (~528 months vs 16,000 days).
    Monthly data is sufficient for climatology baselines.
    
    Args:
        geom: Earth Engine geometry
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        DataFrame with monthly aggregated data (date format: YYYY-MM-01)
    """
    df_temp_precip = fetch_era5_monthly_temp_precip(geom, start_date, end_date, scale_m=25000)
    df_soil_moisture = fetch_era5_monthly_soil_moisture(geom, start_date, end_date, scale_m=9000)
    
    if df_temp_precip is None or df_temp_precip.empty:
        base = df_soil_moisture
    elif df_soil_moisture is None or df_soil_moisture.empty:
        base = df_temp_precip
    else:
        base = pd.merge(df_temp_precip, df_soil_moisture, on="date", how="outer")
    
    if base is None or base.empty:
        cols = ["date", "temp_2m_mean_C", "precipitation_sum_mm", "sm1_mean", "sm2_mean"]
        return pd.DataFrame(columns=cols)
    
    base["date"] = pd.to_datetime(base["date"])
    return base.sort_values("date").reset_index(drop=True)

def fetch_era5_daily(geom: ee.Geometry, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
    """
    Get daily ERA5 data by combining temp/precip and soil moisture.
    
    NOTE: This is slower. For climatology baselines, use fetch_era5_monthly() instead.
    
    Args:
        geom: Earth Engine geometry
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    """
    df_temp_precip = fetch_era5_daily_temp_precip(geom, start_date, end_date, scale_m=25000)
    df_soil_moisture = fetch_era5_daily_soil_moisture(geom, start_date, end_date, scale_m=9000)
    
    if df_temp_precip is None or df_temp_precip.empty:
        base = df_soil_moisture
    elif df_soil_moisture is None or df_soil_moisture.empty:
        base = df_temp_precip
    else:
        base = pd.merge(df_temp_precip, df_soil_moisture, on="date", how="outer")
    
    if base is None or base.empty:
        cols = ["date", "temp_2m_mean_C", "precipitation_sum_mm", "sm1_mean", "sm2_mean"]
        return pd.DataFrame(columns=cols)
    
    base["date"] = pd.to_datetime(base["date"])
    return base.sort_values("date").reset_index(drop=True)

def fetch_era5_data(region_name: str, start_date: str, end_date: str, daily: bool = True) -> pd.DataFrame:
    """Unified function to fetch ERA5 data (daily or monthly)."""
    geom = regions_ee()[region_name]
    if daily:
        df = fetch_era5_daily(geom, start_date, end_date)
    else:
        df_t = era5_temp_precip_df(geom, start_date, end_date)
        df_sm = soil_moisture_mean_df(geom, start_date, end_date)
        df = pd.merge(df_t, df_sm, on="date", how="outer") if not df_t.empty and not df_sm.empty else (df_t if not df_t.empty else df_sm)
        if df is not None and not df.empty:
            df["date"] = pd.to_datetime(df["date"])
    
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "temp_2m_mean_C", "precipitation_sum_mm", "sm1_mean", "sm2_mean", "region"])
    
    df["region"] = region_name
    return df[["date", "temp_2m_mean_C", "precipitation_sum_mm", "sm1_mean", "sm2_mean", "region"]]

def fetch_all_regions_era5(start_date: str, end_date: str, daily: bool = True) -> pd.DataFrame:
    """Fetch ERA5 data for all regions."""
    frames = []
    for region_name in regions_ee().keys():
        df = fetch_era5_data(region_name, start_date, end_date, daily=daily)
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "temp_2m_mean_C", "precipitation_sum_mm", "sm1_mean", "sm2_mean", "region"])
    return pd.concat(frames, ignore_index=True).sort_values(["region", "date"])

def compute_spi(precip_series: pd.Series, scale: int = 30) -> pd.Series:
    """Compute SPI at given scale (days) using gamma fit + normal quantile."""
    rolling = precip_series.rolling(window=scale, min_periods=scale//2).sum()
    valid = rolling.dropna()
    if valid.empty:
        return pd.Series(index=precip_series.index, data=np.nan)

    q0 = (valid == 0).mean()
    nonzero = valid[valid > 0]
    if nonzero.empty:
        return pd.Series(index=precip_series.index, data=np.nan)

    try:
        mean_val, var_val = nonzero.mean(), nonzero.var()
        if var_val <= 0 or mean_val <= 0:
            spi = pd.Series(index=precip_series.index, data=np.nan)
            spi.loc[valid.index] = (valid - valid.mean()) / (valid.std() + 1e-6)
            return spi
        
        shape, scale_param = mean_val**2 / var_val, var_val / mean_val
        cdf_nonzero = gamma.cdf(nonzero, shape, loc=0, scale=scale_param)
        p = pd.Series(index=valid.index, dtype=float)
        p.loc[valid == 0] = q0
        p.loc[valid > 0] = q0 + (1 - q0) * cdf_nonzero
        p = p.clip(1e-6, 1 - 1e-6)
        
        spi = pd.Series(index=precip_series.index, data=np.nan)
        spi.loc[valid.index] = norm.ppf(p)
        return spi
    except Exception as e:
        return pd.Series(index=precip_series.index, data=np.nan)

def update_era5_spi(project_id: str, dataset_id: str, table_id: str = "era5_spi"):
    """Fetch all ERA5 data from BQ, recompute SPI, and save back."""
    query = f"SELECT * FROM `{project_id}.{dataset_id}.era5` ORDER BY region, date"
    df = load_from_bigquery(query)
    if df is None or df.empty:
        return
    
    df['date'] = pd.to_datetime(df['date'])
    spi_records = []
    for region in df['region'].unique():
        region_df = df[df['region'] == region].sort_values('date')
        region_df['spi30'] = compute_spi(region_df['precipitation_sum_mm'])
        spi_records.append(region_df[['date', 'region', 'spi30']])
    
    spi_df = pd.concat(spi_records, ignore_index=True).dropna(subset=['spi30'])
    save_to_bigquery(spi_df, project_id, dataset_id, table_id, mode='WRITE_TRUNCATE')
