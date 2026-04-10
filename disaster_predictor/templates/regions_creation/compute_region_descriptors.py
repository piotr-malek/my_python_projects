#!/usr/bin/env python3
"""
Compute region descriptors for subregions in a BigQuery table.

Requires climatology data (terrain_static, era5, modis) to exist first.
Run add_regions_from_json.py and fetch climatology before running this script.

This script computes all region descriptors defined in the v2.0 specification:
- Geographic: lat/lon, elevation, slope
- Climate: precip, temp, aridity, frost days
- Hydrology: distance to coast/water, basin type, drainage
- Landcover: vegetation, urban, crop fractions
- Soil: texture, depth, organic carbon, water capacity
- Historical: event frequencies (flood, fire, landslide)

Data sources:
- Existing tables: regions_info, climatology.terrain_static, climatology.era5, climatology.modis
- GEE: landcover, soil, water bodies, hydrology, coastlines
"""

import os
import sys
import time
import math
from pathlib import Path
from typing import Dict, Any, Optional
from collections import Counter

import ee
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)
sys.path.insert(0, str(ROOT))

from utils.earth_engine_utils import init_ee, KEY_PATH, get_subregions_from_bq
from utils.bq_utils import load_from_bigquery, save_to_bigquery, execute_sql
from utils.incremental_save_utils import SLEEP_BETWEEN_SUBREGIONS

# GEE Dataset IDs
SRTM_DEM_ID = "USGS/SRTMGL1_003"
ESA_WORLDCOVER_ID = "ESA/WorldCover/v200"  # v200 is latest as of 2024
SOILGRIDS_ID = "projects/soilgrids-isric/clip"  # Deprecated; use ISRIC_SOILGRIDS
ISRIC_SOILGRIDS_ID = "ISRIC/SoilGrids250m/v20"  # Water content, texture
OPENLANDMAP_TEXTURE_ID = "OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02"
HYDROSHEDS_ID = "WWF/HydroSHEDS/v1"
JRC_WATER_ID = "JRC/GSW1_4/GlobalSurfaceWater"  # Global Surface Water dataset
USGS_WATER_ID = "USGS/GFSAD30"  # Global Food Security Analysis Data
COASTLINES_ID = "USDOS/LSIB_SIMPLE/2017"  # Large Scale International Boundaries

# USDA texture class codes (OpenLandMap) -> human-readable
USDA_TEXTURE_NAMES = {
    1: "clay", 2: "silty_clay", 3: "sandy_clay", 4: "clay_loam",
    5: "silty_clay_loam", 6: "sandy_clay_loam", 7: "loam",
    8: "silt_loam", 9: "sandy_loam", 10: "silt",
    11: "loamy_sand", 12: "sand",
}

# Climatology period for computing region descriptors
# 
# For region descriptors (mean_annual_precip, mean_annual_temp, etc.):
#   - Using 1991-2024 (34 years) to capture recent climate trends
#   - Longer period = more stable statistics for characterizing regions
#
# Note: For ML model anomaly computation (used in training/inference):
#   - Should use standard 1991-2020 (WMO 30-year climatology period)
#   - This ensures consistent baseline across all regions
#   - See ml_training/features/compute_climatology.py for model climatology
#
CLIMATOLOGY_START = "1991-01-01"
CLIMATOLOGY_END = "2024-12-31"  # Extended to use all available data (34 years)

# Descriptor schema definition - used for both schema updates and counting
DESCRIPTOR_SCHEMA = {
    # Geographic
    "mean_latitude_deg": "FLOAT64",
    "mean_longitude_deg": "FLOAT64",
    "elevation_mean_m": "FLOAT64",
    "elevation_std_m": "FLOAT64",
    "slope_mean_deg": "FLOAT64",
    "slope_gt15_pct": "FLOAT64",
    # Climate
    "mean_annual_precip_mm": "FLOAT64",
    "precip_seasonality_index": "FLOAT64",
    "mean_annual_temp_c": "FLOAT64",
    "temperature_seasonality_c": "FLOAT64",
    "aridity_index": "FLOAT64",
    "frost_days_per_year": "FLOAT64",
    # Hydrology
    "distance_to_coast_km": "FLOAT64",
    "distance_to_major_waterbody_km": "FLOAT64",
    "basin_type": "STRING",
    "drainage_area_km2": "FLOAT64",
    "stream_order_max": "INT64",
    "permanent_water_fraction_pct": "FLOAT64",
    "wetland_fraction_pct": "FLOAT64",
    "seasonal_water_range_pct": "FLOAT64",
    # Landcover
    "landcover_diversity_index": "FLOAT64",
    "urban_fraction_pct": "FLOAT64",
    "crop_fraction_pct": "FLOAT64",
    "natural_vegetation_fraction_pct": "FLOAT64",
    "ndvi_mean": "FLOAT64",
    "ndvi_std": "FLOAT64",
    "tree_cover_pct": "FLOAT64",
    "grass_cover_pct": "FLOAT64",
    # Soil
    "soil_texture_class": "STRING",
    "soil_depth_cm": "FLOAT64",
    "soil_organic_carbon_pct": "FLOAT64",
    "available_water_capacity_mm": "FLOAT64",
    "effective_rooting_depth_cm": "FLOAT64",
    # Historical
    "flood_events_per_decade": "FLOAT64",
    "fire_return_interval_years": "FLOAT64",
    "landslide_events_per_decade": "FLOAT64",
}


def compute_walsh_lawler_seasonality(precip_monthly: list) -> float:
    """
    Compute Walsh-Lawler precipitation seasonality index.
    
    Formula: (1/R) * sum(|Xi - R/12|) where R is annual total, Xi is monthly precip
    Range: 0 (uniform) to 1.83 (highly seasonal)
    """
    if not precip_monthly or len(precip_monthly) != 12:
        return None
    
    annual_total = sum(precip_monthly)
    if annual_total == 0:
        return 0.0
    
    mean_monthly = annual_total / 12.0
    seasonality = sum(abs(x - mean_monthly) for x in precip_monthly) / annual_total
    return seasonality


def compute_aridity_index(precip_mm: float, pet_mm: float) -> Optional[float]:
    """
    Compute aridity index as PET/P ratio.
    
    Args:
        precip_mm: Annual precipitation (mm)
        pet_mm: Annual potential evapotranspiration (mm)
    
    Returns:
        Aridity index (None if inputs invalid)
    """
    if precip_mm is None or pet_mm is None or precip_mm <= 0:
        return None
    return pet_mm / precip_mm


def compute_thornthwaite_pet_annual(
    monthly_temp_c: list, lat_deg: float
) -> Optional[float]:
    """
    Compute annual PET (mm) using Thornthwaite method from monthly mean temps.
    Uses existing climatology data - no GEE fetch.
    
    Args:
        monthly_temp_c: 12 values, mean temp for months 1-12 (°C)
        lat_deg: Latitude (degrees) for daylength
    """
    if not monthly_temp_c or len(monthly_temp_c) != 12:
        return None
    # Heat index I = sum of (T/5)^1.514 for T>0
    heat_index = sum((max(0, t) / 5.0) ** 1.514 for t in monthly_temp_c)
    if heat_index <= 0:
        return 0.0
    a = 6.75e-7 * heat_index**3 - 7.71e-5 * heat_index**2 + 0.01792 * heat_index + 0.49239
    lat_rad = math.radians(lat_deg)
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    pet_annual = 0.0
    for i, (t, n) in enumerate(zip(monthly_temp_c, days_in_month)):
        if t <= 0:
            continue
        # Approximate daylength (hours): 12 + 4*sin(lat)*cos(2pi*(month-6)/12)
        month_angle = 2 * math.pi * (i + 0.5 - 6) / 12
        L = 12 + 4 * math.sin(lat_rad) * math.cos(month_angle)
        L = max(1, min(24, L))
        pet_m = 1.6 * (L / 12) * (n / 30) * (10 * t / heat_index) ** a
        pet_annual += max(0, pet_m)
    return pet_annual if pet_annual > 0 else None


def compute_shannon_entropy(proportions: Dict[str, float]) -> float:
    """Compute Shannon entropy for landcover diversity."""
    if not proportions:
        return 0.0
    
    total = sum(proportions.values())
    if total == 0:
        return 0.0
    
    entropy = 0.0
    for prop in proportions.values():
        if prop > 0:
            p = prop / total
            entropy -= p * math.log(p)
    
    return entropy


def get_descriptors_from_existing_tables(
    region: str, project_id: str, dataset_id: str = "google_earth", table_id: str = "regions_info"
) -> Dict[str, Any]:
    """
    Extract descriptors from existing BigQuery tables.
    
    Raises ValueError if region doesn't exist in any required table.
    """
    descriptors = {}
    
    # From regions table: lat/lon
    query = f"""
    SELECT centroid_lat, centroid_lon
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE region = '{region}'
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        raise ValueError(f"Region '{region}' not found in {dataset_id}.{table_id}")
    descriptors["mean_latitude_deg"] = float(df["centroid_lat"].iloc[0])
    descriptors["mean_longitude_deg"] = float(df["centroid_lon"].iloc[0])
    
    # From terrain_static: elevation, slope
    query = f"""
    SELECT 
        elevation_mean as elevation_mean_m,
        elevation_stdDev as elevation_std_m,
        slope_mean as slope_mean_deg
    FROM `{project_id}.climatology.terrain_static`
    WHERE region = '{region}'
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        raise ValueError(f"Region '{region}' not found in climatology.terrain_static table")
    descriptors["elevation_mean_m"] = float(df["elevation_mean_m"].iloc[0]) if pd.notna(df["elevation_mean_m"].iloc[0]) else None
    descriptors["elevation_std_m"] = float(df["elevation_std_m"].iloc[0]) if pd.notna(df["elevation_std_m"].iloc[0]) else None
    descriptors["slope_mean_deg"] = float(df["slope_mean_deg"].iloc[0]) if pd.notna(df["slope_mean_deg"].iloc[0]) else None
    
    # Compute slope_gt15_pct estimate
    slope_mean = descriptors.get("slope_mean_deg")
    if slope_mean is not None:
        descriptors["slope_gt15_pct"] = min(100, slope_mean * 3) if slope_mean > 15 else max(0, (slope_mean / 15) * 50)
    
    # From ERA5 climatology: precip, temp, frost days
    query = f"""
    SELECT 
        date,
        precipitation_sum_mm,
        temp_2m_mean_C
    FROM `{project_id}.climatology.era5`
    WHERE region = '{region}'
        AND date >= '{CLIMATOLOGY_START}'
        AND date <= '{CLIMATOLOGY_END}'
    ORDER BY date
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        raise ValueError(f"Region '{region}' not found in climatology.era5 table")
    
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    
    annual_precip = df.groupby("year")["precipitation_sum_mm"].sum()
    descriptors["mean_annual_precip_mm"] = float(annual_precip.mean())
    
    monthly_precip = df.groupby("month")["precipitation_sum_mm"].sum().tolist()
    if len(monthly_precip) == 12:
        descriptors["precip_seasonality_index"] = compute_walsh_lawler_seasonality(monthly_precip)
    
    annual_temp = df.groupby("year")["temp_2m_mean_C"].mean()
    descriptors["mean_annual_temp_c"] = float(annual_temp.mean())
    
    monthly_temp = df.groupby("month")["temp_2m_mean_C"].mean()
    descriptors["temperature_seasonality_c"] = float(monthly_temp.std())
    
    # Aridity index: PET/P from Thornthwaite PET (uses monthly temp, no new fetch)
    monthly_temp_list = [monthly_temp.get(m, 0) for m in range(1, 13)]
    lat = descriptors.get("mean_latitude_deg")
    if lat is not None:
        pet_annual = compute_thornthwaite_pet_annual(monthly_temp_list, lat)
        descriptors["aridity_index"] = compute_aridity_index(
            descriptors.get("mean_annual_precip_mm"), pet_annual
        )
    
    frost_days = df[df["temp_2m_mean_C"] < 0].groupby("year").size()
    descriptors["frost_days_per_year"] = float(frost_days.mean()) if len(frost_days) > 0 else 0.0
    
    # From MODIS: NDVI stats
    query = f"""
    SELECT ndvi_mean
    FROM `{project_id}.climatology.modis`
    WHERE region = '{region}'
        AND date >= '{CLIMATOLOGY_START}'
        AND date <= '{CLIMATOLOGY_END}'
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        raise ValueError(f"Region '{region}' not found in climatology.modis table")
    descriptors["ndvi_mean"] = float(df["ndvi_mean"].mean())
    descriptors["ndvi_std"] = float(df["ndvi_mean"].std())
    
    return descriptors


def compute_gee_descriptors(geom: ee.Geometry, region: str) -> Dict[str, Any]:
    """
    Compute descriptors requiring GEE data sources.
    
    Includes:
    - Distance to coast/water
    - Basin type, drainage
    - Landcover fractions
    - Soil properties
    - Water fractions
    """
    descriptors = {}
    
    try:
        # 1. Distance to coast
        # FeatureCollection.distance() returns an Image; sample at centroid
        coastlines = ee.FeatureCollection(COASTLINES_ID)
        centroid = geom.centroid(maxError=1000)
        dist_image = coastlines.distance(searchRadius=100000, maxError=100)  # 100km search
        dist_stats = dist_image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=centroid.buffer(1000),
            scale=1000,
            maxPixels=1e6,
            bestEffort=True
        ).getInfo()
        if dist_stats:
            # Band name from distance() may be 'distance' or the image's default
            dist_val = dist_stats.get("distance") or (list(dist_stats.values())[0] if dist_stats else None)
            if dist_val is not None:
                descriptors["distance_to_coast_km"] = float(dist_val) / 1000.0  # m to km
        
        # 2. Distance to major waterbody (JRC GSW + fastDistanceTransform)
        try:
            water_img = ee.Image(JRC_WATER_ID)
            water_mask = water_img.select("occurrence").gte(50)  # Occurrence > 50% = water
            # fastDistanceTransform: distance to nearest non-zero pixel (water)
            # Returns squared_euclidean in pixels; sqrt and * scale for meters
            jrc_scale_m = 30  # JRC GSW native resolution
            dist_px = water_mask.fastDistanceTransform(
                neighborhood=1024, units="pixels", metric="squared_euclidean"
            ).sqrt()
            dist_m = dist_px.multiply(jrc_scale_m)
            dist_stats = dist_m.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom,
                scale=jrc_scale_m,
                maxPixels=1e9,
                bestEffort=True
            ).getInfo()
            if dist_stats:
                vals = [v for v in dist_stats.values() if v is not None]
                if vals:
                    descriptors["distance_to_major_waterbody_km"] = float(vals[0]) / 1000.0
                else:
                    descriptors["distance_to_major_waterbody_km"] = None
            else:
                descriptors["distance_to_major_waterbody_km"] = None
        except Exception:
            descriptors["distance_to_major_waterbody_km"] = None
        
        # 3. Basin type and drainage (from HydroSHEDS)
        # HydroSHEDS Basin asset may be deprecated/unavailable in some EE catalogs
        try:
            basins = ee.FeatureCollection(f"{HYDROSHEDS_ID}/Basin")
            intersecting = basins.filterBounds(geom)
            basin_count = intersecting.size().getInfo()
            if basin_count > 0:
                basin = intersecting.first()
                basin_info = basin.getInfo()
                descriptors["basin_type"] = "exorheic"  # Default, needs refinement
                descriptors["drainage_area_km2"] = None
                descriptors["stream_order_max"] = None
        except Exception:
            descriptors["basin_type"] = None
            descriptors["drainage_area_km2"] = None
            descriptors["stream_order_max"] = None
        
        # 4. Landcover from ESA WorldCover (ImageCollection; use first() to get single Image)
        worldcover_ic = ee.ImageCollection(ESA_WORLDCOVER_ID)
        worldcover = worldcover_ic.first().select("Map")
        # Get landcover distribution
        hist = worldcover.reduceRegion(
            reducer=ee.Reducer.frequencyHistogram(),
            geometry=geom,
            scale=100,  # 100m scale
            maxPixels=1e9,
            bestEffort=True
        ).getInfo()
        
        if hist and "Map" in hist:
            lc_hist = hist["Map"]
            total_pixels = sum(lc_hist.values())
            
            # ESA WorldCover classes (simplified mapping)
            # 10: Tree cover, 20: Shrubland, 30: Grassland, 40: Cropland,
            # 50: Built-up, 60: Bare/sparse, 70: Snow/ice, 80: Water, 90: Wetlands
            tree_pixels = lc_hist.get("10", 0)
            grass_pixels = lc_hist.get("30", 0)
            crop_pixels = lc_hist.get("40", 0)
            urban_pixels = lc_hist.get("50", 0)
            water_pixels = lc_hist.get("80", 0)
            wetland_pixels = lc_hist.get("90", 0)
            
            descriptors["tree_cover_pct"] = (tree_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            descriptors["grass_cover_pct"] = (grass_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            descriptors["crop_fraction_pct"] = (crop_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            descriptors["urban_fraction_pct"] = (urban_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            descriptors["permanent_water_fraction_pct"] = (water_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            descriptors["wetland_fraction_pct"] = (wetland_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            
            # Natural vegetation = tree + shrub + grass
            shrub_pixels = lc_hist.get("20", 0)
            natural_pixels = tree_pixels + shrub_pixels + grass_pixels
            descriptors["natural_vegetation_fraction_pct"] = (natural_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
            
            # Landcover diversity (Shannon entropy)
            descriptors["landcover_diversity_index"] = compute_shannon_entropy(lc_hist)
        
        # 5. Soil properties: AWC + effective rooting depth from ISRIC SoilGrids (global)
        # AWC = sum over layers of (field_capacity - wilting_point) * thickness * 10 [mm]
        # Effective rooting depth = depth (cm) where 95% of AWC is contained (global proxy)
        try:
            soil_ic = ee.ImageCollection(ISRIC_SOILGRIDS_ID)
            soil_img = soil_ic.first()
            depths = ["0-5cm", "5-15cm", "15-30cm", "30-60cm", "60-100cm", "100-200cm"]
            thick_cm = [5, 10, 15, 30, 40, 100]
            depth_cumulative = [5, 15, 30, 60, 100, 200]  # cumulative depth to layer bottom (cm)
            scale = 0.001  # SoilGrids wv: 10^-3 cm³/cm³
            layer_imgs = []
            for i, d in enumerate(depths):
                fc_band = f"wv0033_{d}"
                wp_band = f"wv1500_{d}"
                try:
                    fc = soil_img.select(fc_band)
                    wp = soil_img.select(wp_band)
                    layer_awc = fc.subtract(wp).multiply(scale * thick_cm[i] * 10)
                    layer_imgs.append((f"awc_{i}", layer_awc))
                except Exception:
                    continue
            if layer_imgs:
                band_names = [n for n, _ in layer_imgs]
                awc_bands = ee.Image.cat([img for _, img in layer_imgs]).rename(band_names)
                # Single reduceRegion: mean per band gives layer AWC (mm)
                layer_stats = awc_bands.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=250,
                    maxPixels=1e9,
                    bestEffort=True
                ).getInfo()
                if layer_stats:
                    layer_vals = [float(layer_stats.get(n, 0) or 0) for n in band_names]
                    total_val = sum(layer_vals)
                    descriptors["available_water_capacity_mm"] = total_val if total_val else None
                else:
                    total_val = 0
                    descriptors["available_water_capacity_mm"] = None
                    layer_vals = []
                if layer_vals and total_val and total_val > 0:
                    cumul = 0
                    for i, v in enumerate(layer_vals):
                        cumul += v
                        if cumul >= 0.95 * total_val:
                            descriptors["effective_rooting_depth_cm"] = round(depth_cumulative[i], 1)
                            break
                    else:
                        descriptors["effective_rooting_depth_cm"] = 200.0  # full profile
                else:
                    descriptors["effective_rooting_depth_cm"] = None
            else:
                descriptors["available_water_capacity_mm"] = None
                descriptors["effective_rooting_depth_cm"] = None
        except Exception as e:
            print(f"    Warning: Soil AWC computation failed: {e}")
            descriptors["available_water_capacity_mm"] = None
            descriptors["effective_rooting_depth_cm"] = None

        # Soil texture class from OpenLandMap (USDA)
        try:
            texture_img = ee.Image(OPENLANDMAP_TEXTURE_ID)
            texture_bands = texture_img.bandNames().getInfo()
            top_band = "b0" if texture_bands and "b0" in texture_bands else texture_bands[0] if texture_bands else None
            if top_band:
                tex_stats = texture_img.select(top_band).reduceRegion(
                    reducer=ee.Reducer.mode(),
                    geometry=geom,
                    scale=250,
                    maxPixels=1e9,
                    bestEffort=True
                ).getInfo()
                if tex_stats and top_band in tex_stats and tex_stats[top_band] is not None:
                    code = int(float(tex_stats[top_band]))
                    descriptors["soil_texture_class"] = USDA_TEXTURE_NAMES.get(code, f"class_{code}")
                else:
                    descriptors["soil_texture_class"] = None
            else:
                descriptors["soil_texture_class"] = None
        except Exception as e:
            print(f"    Warning: Soil texture computation failed: {e}")
            descriptors["soil_texture_class"] = None

        # Legacy placeholders (would need different datasets)
        descriptors["soil_depth_cm"] = None
        descriptors["soil_organic_carbon_pct"] = None
        
        # 6. Seasonal water range (from JRC GSW - seasonality = months with water, 1-12)
        try:
            water_img = ee.Image(JRC_WATER_ID)
            # Mean seasonality in region: avg months with surface water (0-12 scale)
            seasonality = water_img.select("seasonality")
            stats = seasonality.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom,
                scale=100,
                maxPixels=1e9,
                bestEffort=True
            ).getInfo()
            if stats and "seasonality" in stats and stats["seasonality"] is not None:
                # Scale to 0-100: mean months/12 * 100
                descriptors["seasonal_water_range_pct"] = float(stats["seasonality"]) * (100.0 / 12.0)
            else:
                descriptors["seasonal_water_range_pct"] = None
        except Exception:
            descriptors["seasonal_water_range_pct"] = None
        
        # 7. Aridity index - computed from ERA5 in get_descriptors_from_existing_tables
        
        # 8. Effective rooting depth - set in soil block (AWC-derived, global); ensure key exists
        if "effective_rooting_depth_cm" not in descriptors:
            descriptors["effective_rooting_depth_cm"] = None
        
    except Exception as e:
        print(f"    Error computing GEE descriptors: {e}")
    
    return descriptors


def compute_historical_event_frequencies(
    region: str,
    project_id: str,
    dataset_id: str = "climatology",
) -> Dict[str, Any]:
    """
    Compute historical event frequencies from climatology tables.
    
    - fire_return_interval_years: from MODIS burned_area + VIIRS hotspots (BQ only)
    - flood_events_per_decade, landslide_events_per_decade: still placeholders
    """
    descriptors = {"flood_events_per_decade": None, "landslide_events_per_decade": None}

    # Fire return interval from existing MODIS + VIIRS data (no new fetch)
    try:
        # MODIS: years with burned_area_pct > 0.5% (significant fire)
        q_modis = f"""
        SELECT EXTRACT(YEAR FROM date) as year, MAX(burned_area_pct) as max_burn
        FROM `{project_id}.{dataset_id}.modis`
        WHERE region = '{region}' AND burned_area_pct IS NOT NULL
        GROUP BY year
        HAVING MAX(burned_area_pct) > 0.5
        """
        df_modis = load_from_bigquery(q_modis, project_id=project_id)
        fire_years_modis = set(df_modis["year"].astype(int).tolist()) if df_modis is not None and not df_modis.empty else set()

        # VIIRS: years with any hotspots
        q_viirs = f"""
        SELECT DISTINCT EXTRACT(YEAR FROM date) as year
        FROM `{project_id}.{dataset_id}.viirs`
        WHERE region = '{region}' AND hotspot_count > 0
        """
        df_viirs = load_from_bigquery(q_viirs, project_id=project_id)
        fire_years_viirs = set(df_viirs["year"].astype(int).tolist()) if df_viirs is not None and not df_viirs.empty else set()

        fire_years = fire_years_modis | fire_years_viirs
        # Use MODIS date range (2000+) or VIIRS (2012+) for denominator
        q_years = f"""
        SELECT MIN(EXTRACT(YEAR FROM date)) as min_y, MAX(EXTRACT(YEAR FROM date)) as max_y
        FROM `{project_id}.{dataset_id}.modis`
        WHERE region = '{region}'
        """
        df_y = load_from_bigquery(q_years, project_id=project_id)
        if df_y is not None and not df_y.empty and pd.notna(df_y["min_y"].iloc[0]):
            span = int(df_y["max_y"].iloc[0]) - int(df_y["min_y"].iloc[0]) + 1
            n_fire = len(fire_years)
            if n_fire > 0 and span > 0:
                descriptors["fire_return_interval_years"] = round(span / n_fire, 1)
            else:
                descriptors["fire_return_interval_years"] = None
        else:
            descriptors["fire_return_interval_years"] = None
    except Exception:
        descriptors["fire_return_interval_years"] = None

    return descriptors


def compute_all_descriptors_for_region(
    region: str,
    geom: ee.Geometry,
    project_id: str,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> Dict[str, Any]:
    """Compute all descriptors for a single region."""
    all_descriptors = {"region": region}
    all_descriptors.update(
        get_descriptors_from_existing_tables(
            region, project_id, dataset_id=dataset_id, table_id=table_id
        )
    )
    all_descriptors.update(compute_gee_descriptors(geom, region))
    all_descriptors.update(compute_historical_event_frequencies(region, project_id))
    return all_descriptors


def update_regions_info_schema(
    project_id: str,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
):
    """
    Add descriptor columns to the regions table if they don't exist.
    
    Uses ALTER TABLE ADD COLUMN IF NOT EXISTS (BigQuery doesn't support this directly,
    so we check and add columns one by one).
    """
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Get existing columns
    query = f"""
    SELECT column_name, data_type
    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_id}'
    """
    existing_cols = load_from_bigquery(query, project_id=project_id)
    existing_col_names = set(existing_cols["column_name"].tolist()) if existing_cols is not None and not existing_cols.empty else set()
    
    # Add missing columns
    for col_name, col_type in DESCRIPTOR_SCHEMA.items():
        if col_name not in existing_col_names:
            print(f"  Adding column: {col_name} ({col_type})")
            sql = f"ALTER TABLE `{table_ref}` ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            try:
                execute_sql(sql, project_id=project_id)
            except Exception as e:
                print(f"    Warning: Could not add {col_name}: {e}")
                # BigQuery doesn't support IF NOT EXISTS, so we'll handle errors gracefully


def _get_descriptors_bq_schema() -> list:
    """Build BigQuery schema for descriptor temp table (region + all descriptors)."""
    fields = [SchemaField("region", "STRING", mode="REQUIRED")]
    for col, bq_type in DESCRIPTOR_SCHEMA.items():
        mode = "NULLABLE"
        fields.append(SchemaField(col, bq_type, mode=mode))
    return fields


def _coerce_descriptors_df_for_bq(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce DataFrame columns to match BigQuery schema.
    Prevents Pyarrow from inferring wrong types (e.g. all-None STRING cols as INT64).
    """
    df = df.copy()
    for col, bq_type in DESCRIPTOR_SCHEMA.items():
        if col not in df.columns:
            continue
        if bq_type == "STRING":
            # Ensure string columns stay string/object; all-None cols get inferred wrong otherwise
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x))
        elif bq_type == "INT64":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def update_region_descriptors_in_bq(
    descriptors_df: pd.DataFrame,
    project_id: str,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
):
    """
    Update regions table with computed descriptors.
    
    Uses MERGE statement for efficient updates.
    """
    if descriptors_df.empty:
        return

    # Coerce dtypes and ensure all schema columns exist (fill missing with None)
    descriptors_df = _coerce_descriptors_df_for_bq(descriptors_df)
    schema_cols = ["region"] + list(DESCRIPTOR_SCHEMA.keys())
    for c in schema_cols:
        if c not in descriptors_df.columns:
            descriptors_df[c] = None
    descriptors_df = descriptors_df[schema_cols]

    # Create temporary table with new descriptors
    temp_table_id = f"_temp_descriptors_{int(time.time())}"
    
    try:
        # Save to temporary table with explicit schema (avoids Pyarrow inferring all-None STRING as INT64)
        bq_schema = _get_descriptors_bq_schema()
        save_to_bigquery(
            descriptors_df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=temp_table_id,
            mode="WRITE_TRUNCATE",
            schema=bq_schema,
        )

        # Build MERGE statement; use SAFE_CAST for STRING cols in case temp table schema differs
        descriptor_cols = [c for c in descriptors_df.columns if c != "region"]
        set_clauses = []
        for col in descriptor_cols:
            if DESCRIPTOR_SCHEMA.get(col) == "STRING":
                set_clauses.append(f"t.{col} = SAFE_CAST(s.{col} AS STRING)")
            else:
                set_clauses.append(f"t.{col} = s.{col}")
        
        merge_sql = f"""
        MERGE `{project_id}.{dataset_id}.{table_id}` t
        USING `{project_id}.{dataset_id}.{temp_table_id}` s
        ON t.region = s.region
        WHEN MATCHED THEN
          UPDATE SET {', '.join(set_clauses)}
        """
        
        execute_sql(merge_sql, project_id=project_id)

    except Exception as e:
        print(f"  ✗ Error updating descriptors: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Clean up temporary table
        try:
            drop_sql = f"DROP TABLE IF EXISTS `{project_id}.{dataset_id}.{temp_table_id}`"
            execute_sql(drop_sql, project_id=project_id)
        except:
            pass


def get_subregions_with_null_descriptors(
    project_id: str,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> Dict[str, ee.Geometry]:
    """Load region geometries from BQ for rows where mean_latitude_deg IS NULL (new regions)."""
    query = f"""
    SELECT region, lon_min, lat_min, lon_max, lat_max
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE mean_latitude_deg IS NULL
    ORDER BY region
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        return {}
    subregions = {}
    for _, row in df.iterrows():
        subregions[row["region"]] = ee.Geometry.Rectangle([
            row["lon_min"], row["lat_min"], row["lon_max"], row["lat_max"]
        ])
    return subregions


def main(
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
):
    """Compute descriptors for regions where mean_latitude_deg IS NULL; append to BQ after each."""
    if project_id is None:
        project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise RuntimeError("PROJECT_ID not set")
    
    init_ee(KEY_PATH)
    subregions = get_subregions_with_null_descriptors(
        project_id=project_id, dataset_id=dataset_id, table_id=table_id
    )
    if not subregions:
        print("No regions with NULL descriptors. Nothing to do.")
        return
    
    update_regions_info_schema(
        project_id, dataset_id=dataset_id, table_id=table_id
    )
    total = len(subregions)
    print(f"{total} regions to process (mean_latitude_deg IS NULL), appending to BQ after each.", flush=True)
    
    for i, (region, geom) in enumerate(subregions.items(), 1):
        t0 = time.time()
        descriptors = compute_all_descriptors_for_region(
            region, geom, project_id,
            dataset_id=dataset_id, table_id=table_id,
        )
        elapsed = time.time() - t0
        update_region_descriptors_in_bq(
            pd.DataFrame([descriptors]),
            project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        print(f"[{i}/{total}] processed {region}, took {elapsed:.0f} secs, appended to BQ", flush=True)
        if i < total:
            time.sleep(SLEEP_BETWEEN_SUBREGIONS)
    
    print(f"Done. {total} regions processed.", flush=True)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Compute region descriptors for subregions. Requires climatology data."
    )
    ap.add_argument(
        "--dataset-id",
        type=str,
        default="google_earth",
        help="BigQuery dataset ID (default: google_earth)",
    )
    ap.add_argument(
        "--table-id",
        type=str,
        default="regions_info",
        help="BigQuery table ID (default: regions_info)",
    )
    ap.add_argument(
        "--test",
        type=int,
        metavar="N",
        default=0,
        help="Test mode: only process N regions (e.g. --test 5). Skips BQ update.",
    )
    ap.add_argument(
        "--test-save",
        type=int,
        metavar="N",
        default=0,
        help="Test BQ save: process N regions, save to temp table and MERGE. Validates full save flow.",
    )
    args = ap.parse_args()
    if args.test_save > 0:
        # Test save flow on small sample
        project_id = os.getenv("PROJECT_ID") or "disaster-predictor-470812"
        print("Initializing Earth Engine...")
        init_ee(KEY_PATH)
        subregions = get_subregions_from_bq(
            project_id=project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id,
        )
        regions_list = list(subregions.keys())
        n = min(args.test_save, len(regions_list))
        test_regions = regions_list[:n]
        subregions_subset = {r: subregions[r] for r in test_regions}
        print(f"TEST-SAVE: Processing {n} regions, then saving to BigQuery")
        all_descriptors = []
        for i, (region, geom) in enumerate(subregions_subset.items(), 1):
            print(f"[{i}/{n}] {region}")
            d = compute_all_descriptors_for_region(
                region, geom, project_id,
                dataset_id=args.dataset_id, table_id=args.table_id,
            )
            all_descriptors.append(d)
        df = pd.DataFrame(all_descriptors)
        print("Updating BigQuery (temp table + MERGE)...")
        update_region_descriptors_in_bq(
            df, project_id,
            dataset_id=args.dataset_id, table_id=args.table_id,
        )
        print("✓ Test-save succeeded")
    elif args.test > 0:
        # Run test: pick N regions, compute descriptors, print results (no BQ update)
        project_id = os.getenv("PROJECT_ID") or "disaster-predictor-470812"
        print("Initializing Earth Engine...")
        init_ee(KEY_PATH)
        subregions = get_subregions_from_bq(
            project_id=project_id,
            dataset_id=args.dataset_id,
            table_id=args.table_id,
        )
        regions_list = list(subregions.keys())
        test_regions = regions_list[: args.test]
        subregions_subset = {r: subregions[r] for r in test_regions}
        print(f"TEST MODE: Processing {len(test_regions)} regions (no BQ update)")
        all_descriptors = []
        for i, (region, geom) in enumerate(subregions_subset.items(), 1):
            print(f"[{i}/{len(test_regions)}] {region}")
            d = compute_all_descriptors_for_region(
                region, geom, project_id,
                dataset_id=args.dataset_id, table_id=args.table_id,
            )
            all_descriptors.append(d)
            non_null = sum(1 for k, v in d.items() if k != "region" and v is not None)
            print(f"  {non_null} non-null descriptors")
        df = pd.DataFrame(all_descriptors)
        print(df.to_string())
    else:
        main(dataset_id=args.dataset_id, table_id=args.table_id)
