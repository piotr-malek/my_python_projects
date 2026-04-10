import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import warnings
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from utils.bq_utils import load_from_bigquery, execute_sql
from ml_training.config import (
    PROJECT_ID,
    CLIMATOLOGY_DATASET,
    REGIONS_DATASET,
    REGIONS_TABLE,
    ERA5_TABLE,
    MODIS_TABLE,
    VIIRS_TABLE,
    TERRAIN_TABLE,
    GLC_TABLE,
    GLOBAL_FLOOD_DB_TABLE,
    WORLDFLOODS_TABLE,
    ERA5_START,
    ERA5_END,
    MODIS_START,
    MODIS_END,
    VIIRS_START,
    VIIRS_END,
    MODIS_FORWARD_FILL_WINDOW,
    MISSING_DATA_THRESHOLD,
    REQUIRED_FEATURES,
    DISCHARGE_CLIMATOLOGY_TABLE,
    DISCHARGE_DAILY_DATASET,
    DISCHARGE_DAILY_TABLE,
)


def _build_year_filter(
    dataset_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> str:
    """Return year partition filter for climatology tables that have `year`."""
    if dataset_id != CLIMATOLOGY_DATASET or not start_date or not end_date:
        return ""
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    return f"AND year BETWEEN {start_year} AND {end_year}"


def load_all_regions() -> List[str]:
    query = f"""
    SELECT DISTINCT region
    FROM `{PROJECT_ID}.{REGIONS_DATASET}.{REGIONS_TABLE}`
    ORDER BY region
    """
    
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        raise ValueError(f"No regions found in {REGIONS_DATASET}.{REGIONS_TABLE}")
    
    return df['region'].tolist()


def load_era5_data(
    regions: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dataset_id: Optional[str] = None
) -> pd.DataFrame:
    """Load ERA5 data for specified regions or all regions if None.
    
    Args:
        regions: List of region names, or None for all regions
        start_date: Start date (YYYY-MM-DD), or None for dataset default
        end_date: End date (YYYY-MM-DD), or None for dataset default
        dataset_id: Dataset to load from ('climatology' or 'daily_ingestion'), 
                   or None to auto-detect based on date range
    """
    # Auto-detect dataset if not specified
    if dataset_id is None:
        # Use daily_ingestion for recent dates (last 60 days from today)
        from datetime import datetime, timedelta
        today = datetime.now().date()
        cutoff_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
        if end_date and end_date >= cutoff_date:
            dataset_id = "daily_ingestion"
        else:
            dataset_id = CLIMATOLOGY_DATASET
    elif dataset_id == "climatology":
        dataset_id = CLIMATOLOGY_DATASET
    
    # Set defaults based on dataset
    if dataset_id == CLIMATOLOGY_DATASET:
        start_date = start_date or ERA5_START
        end_date = end_date or ERA5_END
    else:  # daily_ingestion
        # No defaults for daily_ingestion - must specify dates
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date must be specified for daily_ingestion dataset")
    
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    year_filter = _build_year_filter(dataset_id, start_date, end_date)

    def _drop_duplicate_rows(df: pd.DataFrame, source: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        dupes = df.duplicated(subset=['date', 'region'])
        if dupes.any():
            df = df.sort_values(['region', 'date']).drop_duplicates(
                subset=['date', 'region'],
                keep='last'
            )
        return df
    
    # Load ERA5 data
    era5_query = f"""
    SELECT 
        date,
        region,
        temp_2m_mean_C,
        precipitation_sum_mm,
        sm1_mean,
        sm2_mean
    FROM `{PROJECT_ID}.{dataset_id}.{ERA5_TABLE}`
    WHERE {where_clause}
        {year_filter}
        AND date >= '{start_date}'
        AND date <= '{end_date}'
    ORDER BY region, date
    """
    
    era5_df = pd.DataFrame()
    try:
        era5_df = load_from_bigquery(era5_query, project_id=PROJECT_ID)
        if era5_df is not None and not era5_df.empty:
            era5_df['date'] = pd.to_datetime(era5_df['date'])
            era5_df = _drop_duplicate_rows(era5_df, f"{dataset_id}.{ERA5_TABLE}")
    except Exception as e:
        print(f"Warning: Could not load ERA5 data from {dataset_id}: {e}")
    
    # For daily_ingestion, merge with OpenMeteo backup
    if dataset_id == "daily_ingestion" and not era5_df.empty:
        # Load OpenMeteo data as backup
        if regions:
            regions_str = "', '".join(regions)
            openmeteo_where = f"region_name IN ('{regions_str}')"
        else:
            openmeteo_where = "1=1"
        
        openmeteo_query = f"""
        SELECT 
            date,
            region_name as region,
            temperature_2m_mean,
            precipitation_sum,
            soil_moisture_0_to_7cm_mean,
            soil_moisture_7_to_28cm_mean
        FROM `{PROJECT_ID}.{dataset_id}.openmeteo_weather`
        WHERE {openmeteo_where}
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY region, date
        """
        
        openmeteo_df = pd.DataFrame()
        try:
            openmeteo_df = load_from_bigquery(openmeteo_query, project_id=PROJECT_ID)
            if openmeteo_df is not None and not openmeteo_df.empty:
                openmeteo_df['date'] = pd.to_datetime(openmeteo_df['date'])
                # Map OpenMeteo fields to ERA5 format
                openmeteo_df = openmeteo_df.rename(columns={
                    'temperature_2m_mean': 'temp_2m_mean_C',
                    'precipitation_sum': 'precipitation_sum_mm',
                    'soil_moisture_0_to_7cm_mean': 'sm1_mean',
                    'soil_moisture_7_to_28cm_mean': 'sm2_mean'
                })
                openmeteo_df = _drop_duplicate_rows(openmeteo_df, f"{dataset_id}.openmeteo_weather")
        except Exception as e:
            print(f"Warning: Could not load OpenMeteo backup data: {e}")
        
        # Merge ERA5 + OpenMeteo (prefer ERA5, fill gaps with OpenMeteo)
        if not openmeteo_df.empty:
            era5_indexed = era5_df.set_index(['date', 'region'])
            openmeteo_indexed = openmeteo_df.set_index(['date', 'region'])
            
            # Start with ERA5
            merged_df = era5_indexed.copy()
            
            # Fill missing dates/values from OpenMeteo
            fillable_fields = ['temp_2m_mean_C', 'precipitation_sum_mm', 'sm1_mean', 'sm2_mean']
            
            for (date_idx, region_idx) in openmeteo_indexed.index:
                if (date_idx, region_idx) not in merged_df.index:
                    # Missing date - add row from OpenMeteo
                    new_row = openmeteo_indexed.loc[(date_idx, region_idx)].copy()
                    merged_df.loc[(date_idx, region_idx)] = new_row
                else:
                    # Date exists - fill missing fields from OpenMeteo
                    for field in fillable_fields:
                        if field in openmeteo_indexed.columns:
                            current_value = merged_df.loc[(date_idx, region_idx), field]
                            if isinstance(current_value, pd.Series):
                                needs_fill = current_value.isna().all()
                            else:
                                needs_fill = pd.isna(current_value)
                            if needs_fill:
                                merged_df.loc[(date_idx, region_idx), field] = openmeteo_indexed.loc[(date_idx, region_idx), field]
            
            era5_df = merged_df.reset_index()
    
    if era5_df.empty:
        return pd.DataFrame()
    
    return era5_df


def load_modis_data(
    regions: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dataset_id: Optional[str] = None
) -> pd.DataFrame:
    """Load MODIS/Landsat NDVI data for specified regions or all regions if None.
    
    - For historical dates (before 2024-08-31): loads MODIS from climatology
    - For recent dates (after 2024-08-31): loads Landsat from daily_ingestion
    - For daily_ingestion dataset: Only loads Landsat (no MODIS merge needed)
    - For training data spanning both periods: Merges MODIS + Landsat
    
    Args:
        regions: List of region names, or None for all regions
        start_date: Start date (YYYY-MM-DD), or None for dataset default
        end_date: End date (YYYY-MM-DD), or None for dataset default
        dataset_id: Dataset to load from ('climatology' or 'daily_ingestion'), 
                   or None to auto-detect based on date range
    """
    # MODIS end date (discontinued)
    modis_end_date = "2024-08-31"
    
    # Auto-detect dataset if not specified
    if dataset_id is None:
        # Use daily_ingestion for recent dates (last 60 days from today)
        from datetime import datetime, timedelta
        today = datetime.now().date()
        cutoff_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
        if end_date and end_date >= cutoff_date:
            dataset_id = "daily_ingestion"
        else:
            dataset_id = CLIMATOLOGY_DATASET
    elif dataset_id == "climatology":
        dataset_id = CLIMATOLOGY_DATASET
    
    # Set defaults based on dataset
    if dataset_id == CLIMATOLOGY_DATASET:
        start_date = start_date or MODIS_START
        end_date = end_date or MODIS_END
    else:  # daily_ingestion
        # No defaults for daily_ingestion - must specify dates
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date must be specified for daily_ingestion dataset")
    
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    modis_year_filter = _build_year_filter(CLIMATOLOGY_DATASET, start_date, end_date or modis_end_date)
    
    result_dfs = []
    
    # Load MODIS for historical dates (before 2024-08-31)
    if start_date and start_date < modis_end_date:
        modis_end = min(end_date or modis_end_date, modis_end_date) if end_date else modis_end_date
        modis_query = f"""
    SELECT 
        date,
        region,
        ndvi_mean,
        burned_area_pct
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.{MODIS_TABLE}`
    WHERE {where_clause}
        {modis_year_filter}
        AND date >= '{start_date}'
          AND date <= '{modis_end}'
    ORDER BY region, date
    """
    
        modis_df = pd.DataFrame()
        try:
            modis_df = load_from_bigquery(modis_query, project_id=PROJECT_ID)
            if modis_df is not None and not modis_df.empty:
                modis_df['date'] = pd.to_datetime(modis_df['date'])
                result_dfs.append(modis_df)
        except Exception as e:
            print(f"Warning: Could not load MODIS data: {e}")
    
    # Load Landsat for recent dates (after 2024-08-31 or from daily_ingestion)
    if (end_date and end_date > modis_end_date) or dataset_id == "daily_ingestion":
        landsat_start = max(start_date or modis_end_date, modis_end_date) if start_date else modis_end_date
        landsat_end = end_date or datetime.now().strftime('%Y-%m-%d')
        
        # Determine which dataset to use for Landsat
        if dataset_id == "daily_ingestion":
            landsat_dataset = "daily_ingestion"
            landsat_table = "landsat"
            # For daily_ingestion, we only load Landsat (no MODIS merge needed)
            # MODIS is only used for historical climatology data
        else:
            # For climatology, we might not have Landsat, so skip
            landsat_dataset = None
        
        if landsat_dataset:
            # burned_area_pct is optional and only exists in MODIS (historical)
            # Landsat doesn't have this column, so set to NULL
            # This is fine - it's only optional for fire models and has very low importance
            landsat_query = f"""
            SELECT 
                date,
                region,
                ndvi_mean,
                CAST(NULL AS FLOAT64) as burned_area_pct
            FROM `{PROJECT_ID}.{landsat_dataset}.{landsat_table}`
            WHERE {where_clause}
              AND date >= '{landsat_start}'
              AND date <= '{landsat_end}'
            ORDER BY region, date
            """
            
            landsat_df = pd.DataFrame()
            try:
                landsat_df = load_from_bigquery(landsat_query, project_id=PROJECT_ID)
                if landsat_df is not None and not landsat_df.empty:
                    landsat_df['date'] = pd.to_datetime(landsat_df['date'])
                    result_dfs.append(landsat_df)
            except Exception as e:
                print(f"Warning: Could not load Landsat data: {e}")
    
    # Combine MODIS and Landsat data
    if result_dfs:
        non_empty_dfs = [df for df in result_dfs if df is not None and not df.empty]
        if non_empty_dfs:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=FutureWarning,
                    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
                )
                combined_df = pd.concat(non_empty_dfs, ignore_index=True)
            combined_df = combined_df.sort_values(['region', 'date']).reset_index(drop=True)
            return combined_df
    
    return pd.DataFrame()


def load_viirs_data(
    regions: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dataset_id: Optional[str] = None
) -> pd.DataFrame:
    """Load VIIRS data for specified regions or all regions if None.
    
    Args:
        regions: List of region names, or None for all regions
        start_date: Start date (YYYY-MM-DD), or None for dataset default
        end_date: End date (YYYY-MM-DD), or None for dataset default
        dataset_id: Dataset to load from ('climatology' or 'daily_ingestion'), 
                   or None to auto-detect based on date range
    """
    # Auto-detect dataset if not specified
    if dataset_id is None:
        # Use daily_ingestion for recent dates (last 60 days from today)
        from datetime import datetime, timedelta
        today = datetime.now().date()
        cutoff_date = (today - timedelta(days=60)).strftime('%Y-%m-%d')
        if end_date and end_date >= cutoff_date:
            dataset_id = "daily_ingestion"
        else:
            dataset_id = CLIMATOLOGY_DATASET
    elif dataset_id == "climatology":
        dataset_id = CLIMATOLOGY_DATASET
    
    # Set defaults based on dataset
    if dataset_id == CLIMATOLOGY_DATASET:
        start_date = start_date or VIIRS_START
        end_date = end_date or VIIRS_END
    else:  # daily_ingestion
        # No defaults for daily_ingestion - must specify dates
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date must be specified for daily_ingestion dataset")
    
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    year_filter = _build_year_filter(dataset_id, start_date, end_date)
    
    query = f"""
    SELECT 
        date,
        region,
        hotspot_count,
        frp_mean
    FROM `{PROJECT_ID}.{dataset_id}.{VIIRS_TABLE}`
    WHERE {where_clause}
        {year_filter}
        AND date >= '{start_date}'
        AND date <= '{end_date}'
    ORDER BY region, date
    """
    
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        return pd.DataFrame()
    
    df['date'] = pd.to_datetime(df['date'])
    return df


def load_terrain_data(regions: Optional[List[str]] = None) -> pd.DataFrame:
    """Load terrain data for specified regions or all regions if None."""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.{TERRAIN_TABLE}`
    WHERE {where_clause}
    ORDER BY region
    """
    
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        return pd.DataFrame()
    
    return df


def load_region_descriptors(regions: Optional[List[str]] = None) -> pd.DataFrame:
    """Load region descriptors for specified regions or all regions if None."""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{REGIONS_DATASET}.{REGIONS_TABLE}`
    WHERE {where_clause}
    ORDER BY region
    """
    
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        raise ValueError(f"No region descriptors found in {REGIONS_DATASET}.{REGIONS_TABLE}")
    
    return df


def load_glc_events(regions: Optional[List[str]] = None) -> pd.DataFrame:
    """Load GLC events for specified regions or all regions if None."""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
    else:
        where_clause = "1=1"
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.{GLC_TABLE}`
    WHERE {where_clause}
    ORDER BY region, date
    """
    
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        return pd.DataFrame()
    
    df['date'] = pd.to_datetime(df['date'])
    return df


def load_global_flood_db(regions: Optional[List[str]] = None) -> pd.DataFrame:
    """Load Global Flood Database events for flood label creation.
    Returns rows with non-null region (event_id, date, region, ...)."""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IS NOT NULL AND region IN ('{regions_str}')"
    else:
        where_clause = "region IS NOT NULL"
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.{GLOBAL_FLOOD_DB_TABLE}`
    WHERE {where_clause}
    ORDER BY region, date
    """
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_worldfloods_events(regions: Optional[List[str]] = None) -> pd.DataFrame:
    """Load WorldFloods events for flood label creation (e.g. Cadiz_01, Malaga_01, Murcia_01).
    Returns rows with non-null region."""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IS NOT NULL AND region IN ('{regions_str}')"
    else:
        where_clause = "region IS NOT NULL"
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.{WORLDFLOODS_TABLE}`
    WHERE {where_clause}
    ORDER BY region, date
    """
    df = load_from_bigquery(query, project_id=PROJECT_ID)
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_river_discharge_data(
    regions: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    project_id: Optional[str] = None,
) -> pd.DataFrame:
    """Load river discharge for training: GloFAS reanalysis (climatology) + daily ingestion.

    Overlapping dates prefer ``daily_ingestion`` (last wins after dedupe).
    """
    pid = project_id or PROJECT_ID
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"region IN ('{regions_str}')"
        om_where_clause = f"region_name IN ('{regions_str}')"
    else:
        where_clause = "1=1"
        om_where_clause = "1=1"
    if not start_date or not end_date:
        return pd.DataFrame()

    parts: List[pd.DataFrame] = []
    year_filter = _build_year_filter(CLIMATOLOGY_DATASET, start_date, end_date)
    glofas_query = f"""
    SELECT date, region, river_discharge
    FROM `{pid}.{CLIMATOLOGY_DATASET}.{DISCHARGE_CLIMATOLOGY_TABLE}`
    WHERE {where_clause}
      {year_filter}
      AND date >= '{start_date}' AND date <= '{end_date}'
    ORDER BY region, date
    """
    try:
        g = load_from_bigquery(glofas_query, project_id=pid)
        if g is not None and not g.empty:
            g = g.copy()
            g["date"] = pd.to_datetime(g["date"]).dt.normalize()
            g["_discharge_src"] = 0
            parts.append(g)
    except Exception as e:
        print(f"Warning: Could not load GloFAS discharge ({DISCHARGE_CLIMATOLOGY_TABLE}): {e}")

    daily_query = f"""
    SELECT date, region_name as region, river_discharge
    FROM `{pid}.{DISCHARGE_DAILY_DATASET}.{DISCHARGE_DAILY_TABLE}`
    WHERE {om_where_clause}
      AND date >= '{start_date}' AND date <= '{end_date}'
    ORDER BY region, date
    """
    try:
        d = load_from_bigquery(daily_query, project_id=pid)
        if d is not None and not d.empty:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"]).dt.normalize()
            d["_discharge_src"] = 1
            parts.append(d)
    except Exception as e:
        print(f"Warning: Could not load daily river discharge from {DISCHARGE_DAILY_TABLE}: {e}")

    if not parts:
        return pd.DataFrame()

    combined = pd.concat(parts, ignore_index=True)
    combined = combined.sort_values(["region", "date", "_discharge_src"])
    combined = combined.drop_duplicates(subset=["date", "region"], keep="last")
    return combined.drop(columns=["_discharge_src"], errors="ignore")


def forward_fill_modis(modis_df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill MODIS from 16-day to daily resolution. Max gap: 16 days."""
    if modis_df.empty:
        return modis_df
    
    result_dfs = []
    
    for region in modis_df['region'].unique():
        region_modis = modis_df[modis_df['region'] == region].copy()
        
        min_date = region_modis['date'].min()
        max_date = region_modis['date'].max()
        
        daily_dates = pd.date_range(start=min_date, end=max_date, freq='D')
        daily_df = pd.DataFrame({'date': daily_dates, 'region': region})
        
        merged = daily_df.merge(region_modis, on=['date', 'region'], how='left')
        
        if 'ndvi_mean' not in merged.columns:
            merged['ndvi_mean'] = None
        if 'burned_area_pct' not in merged.columns:
            merged['burned_area_pct'] = None
        merged['ndvi_mean'] = merged['ndvi_mean'].ffill(limit=MODIS_FORWARD_FILL_WINDOW)
        merged['burned_area_pct'] = merged['burned_area_pct'].ffill(limit=MODIS_FORWARD_FILL_WINDOW)
        
        result_dfs.append(merged)
    
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=FutureWarning,
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
        )
        return pd.concat(result_dfs, ignore_index=True).sort_values(['region', 'date'])


def merge_datasets(
    era5_df: pd.DataFrame,
    modis_df: pd.DataFrame,
    viirs_df: pd.DataFrame,
    terrain_df: pd.DataFrame,
    descriptors_df: pd.DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    if era5_df.empty:
        raise ValueError("ERA5 data is required but empty")
    
    modis_daily = forward_fill_modis(modis_df)
    
    merged = era5_df.copy()
    
    if not modis_daily.empty:
        merged = merged.merge(
            modis_daily[['date', 'region', 'ndvi_mean', 'burned_area_pct']],
            on=['date', 'region'],
            how='left'
        )
    
    if not viirs_df.empty:
        merged = merged.merge(
            viirs_df[['date', 'region', 'hotspot_count', 'frp_mean']],
            on=['date', 'region'],
            how='left'
        )
    
    if not terrain_df.empty:
        terrain_cols = [col for col in terrain_df.columns if col != 'region']
        terrain_subset = terrain_df[['region'] + terrain_cols]
        merged = merged.merge(terrain_subset, on='region', how='left')
    
    if not descriptors_df.empty:
        # Numeric + static region traits from google_earth.regions_info (exclude terrain duplicates).
        # STRING columns (e.g. basin_type) are merged but dropped before RF via select_dtypes(number).
        desc_cols = [
            col for col in descriptors_df.columns
            if col not in ['region', 'elevation_mean_m', 'slope_mean_deg']
        ]
        descriptors_subset = descriptors_df[['region'] + desc_cols]
        merged = merged.merge(descriptors_subset, on='region', how='left')
    
    if start_date:
        merged = merged[merged['date'] >= pd.to_datetime(start_date)]
    if end_date:
        merged = merged[merged['date'] <= pd.to_datetime(end_date)]
    
    return merged.sort_values(['region', 'date']).reset_index(drop=True)


def handle_missing_data(df: pd.DataFrame, required_features: List[str]) -> pd.DataFrame:
    """Drop rows with >50% missing required features, impute rest with region-specific median."""
    if df.empty:
        return df
    
    existing_required = [f for f in required_features if f in df.columns]
    
    if not existing_required:
        return df
    
    missing_count = df[existing_required].isnull().sum(axis=1)
    total_required = len(existing_required)
    missing_ratio = missing_count / total_required
    
    df_clean = df[missing_ratio <= MISSING_DATA_THRESHOLD].copy()
    
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col not in ['date']]
    
    for region in df_clean['region'].unique():
        region_mask = df_clean['region'] == region
        
        for col in numeric_cols:
            if col in df_clean.columns:
                region_data = df_clean.loc[region_mask, col]
                if region_data.notna().sum() > 0:
                    region_median = region_data.median()
                    if pd.notna(region_median):
                        df_clean.loc[region_mask, col] = region_data.fillna(region_median)
    
    return df_clean.reset_index(drop=True)
