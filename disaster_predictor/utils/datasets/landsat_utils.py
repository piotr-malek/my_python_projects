import os
import pandas as pd
import ee
import datetime
from typing import Optional, Tuple
from ..earth_engine_utils import regions_ee, standard_execution_flow

LANDSAT_9_COLLECTION = "LANDSAT/LC09/C02/T1_L2"
LANDSAT_8_COLLECTION = "LANDSAT/LC08/C02/T1_L2"

def calculate_ndvi_landsat(image):
    """Calculate NDVI for Landsat images"""
    red = image.select('SR_B4').multiply(0.0000275).add(-0.2)
    nir = image.select('SR_B5').multiply(0.0000275).add(-0.2)
    ndvi = nir.subtract(red).divide(nir.add(red)).rename('NDVI')
    ndvi = ndvi.where(ndvi.lt(-1), -9999).where(ndvi.gt(1), -9999)
    return image.addBands(ndvi)

def get_latest_landsat_image(collection_id: str, geom: ee.Geometry, day: ee.Date, search_start: ee.Date, search_end: ee.Date):
    """Get the latest available Landsat image for a specific day."""
    ic = (
        ee.ImageCollection(collection_id)
        .filterDate(search_start, search_end)
        .filterBounds(geom)
        .map(calculate_ndvi_landsat)
        .select(["NDVI"])
    )
    
    tf = ic.filter(ee.Filter.lte("system:time_start", day.millis()))
    ic_size = ic.size()
    tf_size = tf.size()
    
    latest_img = ee.Image(ee.Algorithms.If(
        tf_size.gt(0),
        tf.sort("system:time_start", False).first(),
        ee.Algorithms.If(
            ic_size.gt(0),
            ic.sort("system:time_start", False).first(),
            ee.Image.constant(-9999).rename("NDVI")
        )
    ))
    return latest_img, ic_size

def fetch_landsat_daily_ndvi(region_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch hybrid Landsat 8/9 NDVI data for a region."""
    geom = regions_ee()[region_name]
    start = ee.Date(start_date)
    end = ee.Date(end_date)
    days = end.difference(start, "day").int()
    day_list = ee.List.sequence(0, days).map(lambda d: start.advance(d, "day"))
    
    def per_day(day):
        day = ee.Date(day)
        search_start = day.advance(-16, "day")
        search_end = day.advance(1, "day")
        
        l9_img, l9_size = get_latest_landsat_image(LANDSAT_9_COLLECTION, geom, day, search_start, search_end)
        l8_img, l8_size = get_latest_landsat_image(LANDSAT_8_COLLECTION, geom, day, search_start, search_end)
        
        def reduce_ndvi(img):
            return img.select("NDVI").reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom,
                scale=30,
                bestEffort=True,
                maxPixels=1_000_000_000,
                tileScale=4
            ).get("NDVI")

        return ee.Feature(None, {
            "date": day.format("YYYY-MM-dd"),
            "l9_ndvi": reduce_ndvi(l9_img),
            "l8_ndvi": reduce_ndvi(l8_img)
        })
    
    fc = ee.FeatureCollection(day_list.map(per_day))
    features = fc.getInfo()["features"]
    recs = []
    for f in features:
        p = f["properties"]
        # Preference: L9 -> L8
        l9, l8 = p.get("l9_ndvi"), p.get("l8_ndvi")
        val = l9 if (l9 is not None and -1 <= l9 <= 1 and l9 != -9999) else l8
        if val is not None and (val < -1 or val > 1 or val == -9999):
            val = None
        recs.append({"date": p["date"], "ndvi_mean": val})
    
    df = pd.DataFrame(recs)
    df["region"] = region_name
    return df

def aggregate_landsat_to_16day(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily Landsat NDVI to 16-day periods (MODIS-like)."""
    if df.empty: return df
    df['date'] = pd.to_datetime(df['date'])
    # Using fixed 16-day periods from start of year or start of data
    start_date = df['date'].min().replace(month=1, day=1) if not df.empty else None
    if not start_date: return df
    
    df['period_start'] = ((df['date'] - start_date).dt.days // 16) * 16
    df['period_start'] = start_date + pd.to_timedelta(df['period_start'], unit='D')
    
    agg = df.groupby(['region', 'period_start'])['ndvi_mean'].mean().reset_index()
    agg = agg.rename(columns={'period_start': 'date'})
    agg['date'] = agg['date'].dt.strftime('%Y-%m-%d')
    return agg

def check_new_landsat_available(max_date: datetime.date) -> Tuple[bool, Optional[str]]:
    """Check if any new Landsat data is available past max_date."""
    test_date = datetime.date.today().strftime("%Y-%m-%d")
    first_region = next(iter(regions_ee()))
    df = fetch_landsat_daily_ndvi(first_region, test_date, test_date)
    if not df.empty and df['ndvi_mean'].notna().any():
        latest_date_str = df['date'].iloc[0]
        latest_dt = datetime.datetime.strptime(latest_date_str, "%Y-%m-%d").date()
        return latest_dt > max_date, latest_date_str
    return False, None
