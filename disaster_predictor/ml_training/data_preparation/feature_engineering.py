import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from utils.datasets.era5_utils import compute_spi


def compute_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling window aggregations (7d, 14d, 30d)."""
    df = df.copy()
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    
    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_df = df[region_mask].copy()
        
        if 'precipitation_sum_mm' in region_df.columns:
            df.loc[region_mask, 'precip_3d_max'] = (
                region_df['precipitation_sum_mm'].rolling(window=3, min_periods=1).max()
            )
            df.loc[region_mask, 'precip_7d_sum'] = (
                region_df['precipitation_sum_mm'].rolling(window=7, min_periods=1).sum()
            )
            df.loc[region_mask, 'precip_14d_sum'] = (
                region_df['precipitation_sum_mm'].rolling(window=14, min_periods=1).sum()
            )
            df.loc[region_mask, 'precip_30d_sum'] = (
                region_df['precipitation_sum_mm'].rolling(window=30, min_periods=1).sum()
            )
        
        if 'temp_2m_mean_C' in region_df.columns:
            df.loc[region_mask, 'temp_7d_mean'] = (
                region_df['temp_2m_mean_C'].rolling(window=7, min_periods=1).mean()
            )
            df.loc[region_mask, 'temp_7d_max'] = (
                region_df['temp_2m_mean_C'].rolling(window=7, min_periods=1).max()
            )
        
        if 'sm1_mean' in region_df.columns:
            df.loc[region_mask, 'sm1_14d_mean'] = (
                region_df['sm1_mean'].rolling(window=14, min_periods=1).mean()
            )
        if 'sm2_mean' in region_df.columns:
            df.loc[region_mask, 'sm2_14d_mean'] = (
                region_df['sm2_mean'].rolling(window=14, min_periods=1).mean()
            )
        
        if 'river_discharge' in region_df.columns:
            df.loc[region_mask, 'discharge_3d_max'] = (
                region_df['river_discharge'].rolling(window=3, min_periods=1).max()
            )
            df.loc[region_mask, 'river_discharge_7d_mean'] = (
                region_df['river_discharge'].rolling(window=7, min_periods=1).mean()
            )
        
        if 'hotspot_count' in region_df.columns:
            df.loc[region_mask, 'hotspot_7d_sum'] = (
                region_df['hotspot_count'].rolling(window=7, min_periods=1).sum()
            )
        if 'frp_mean' in region_df.columns:
            df.loc[region_mask, 'frp_7d_mean'] = (
                region_df['frp_mean'].rolling(window=7, min_periods=1).mean()
            )
        
        # NDVI trend: 30-day linear fit slope
        if 'ndvi_mean' in region_df.columns:
            ndvi_30d_trend = []
            for i in range(len(region_df)):
                window_start = max(0, i - 29)
                window_data = region_df['ndvi_mean'].iloc[window_start:i+1]
                if len(window_data) >= 3 and window_data.notna().sum() >= 3:
                    x = np.arange(len(window_data))
                    y = window_data.values
                    valid = ~np.isnan(y)
                    if valid.sum() >= 3:
                        slope = np.polyfit(x[valid], y[valid], 1)[0]
                        ndvi_30d_trend.append(slope)
                    else:
                        ndvi_30d_trend.append(np.nan)
                else:
                    ndvi_30d_trend.append(np.nan)
            df.loc[region_mask, 'ndvi_30d_trend'] = ndvi_30d_trend
    
    return df


def compute_spi_feature(df: pd.DataFrame) -> pd.DataFrame:
    """Compute SPI30 (Standardized Precipitation Index, 30-day scale)."""
    df = df.copy()
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    
    if 'precipitation_sum_mm' not in df.columns:
        df['spi30'] = np.nan
        return df
    
    spi_values = []
    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_precip = df.loc[region_mask, 'precipitation_sum_mm']
        spi_series = compute_spi(region_precip, scale=30)
        spi_values.extend(spi_series.values)
    
    df['spi30'] = spi_values
    return df


def compute_climatology(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly percentiles from all historical data (not just training period)."""
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['month'] = df['date'].dt.month
    
    metrics = {
        'temp_2m_mean_C': ['p80', 'p95'],
        'precipitation_sum_mm': ['p20', 'p80'],
        'sm1_mean': ['p20', 'p80'],
        'sm2_mean': ['p20', 'p80'],
        'river_discharge': ['p20', 'p80', 'p95'],
        'ndvi_mean': ['p20'],
        'hotspot_count': ['p95'],
        'frp_mean': ['p95'],
    }
    
    climatology_records = []
    
    for region in df['region'].unique():
        region_df = df[df['region'] == region]
        
        for month in range(1, 13):
            month_df = region_df[region_df['month'] == month]
            
            if month_df.empty:
                continue
            
            record = {'region': region, 'month': month}
            
            for metric, percentiles in metrics.items():
                if metric not in month_df.columns:
                    continue
                
                values = month_df[metric].dropna()
                if len(values) == 0:
                    continue
                
                for pct_name in percentiles:
                    pct_value = float(pct_name[1:]) / 100.0
                    pct_val = values.quantile(pct_value)
                    record[f'{metric}_{pct_name}'] = pct_val
            
            for pct_name in ['p20', 'p80', 'p95']:
                key = 'river_discharge_' + pct_name
                if key not in record:
                    record[key] = np.nan
            
            climatology_records.append(record)
    
    return pd.DataFrame(climatology_records)


def compute_anomaly_indicators(df: pd.DataFrame, climatology: pd.DataFrame) -> pd.DataFrame:
    """Compute binary anomaly indicators by comparing to climatology percentiles."""
    df = df.copy()
    
    if climatology.empty:
        return df
    
    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month
    
    df = df.merge(climatology, on=['region', 'month'], how='left')
    
    if 'temp_2m_mean_C' in df.columns:
        if 'temp_2m_mean_C_p80' in df.columns:
            df['temp_above_p80'] = df['temp_2m_mean_C'] > df['temp_2m_mean_C_p80']
        if 'temp_2m_mean_C_p95' in df.columns:
            df['temp_above_p95'] = df['temp_2m_mean_C'] > df['temp_2m_mean_C_p95']
    
    if 'precipitation_sum_mm' in df.columns:
        if 'precipitation_sum_mm_p20' in df.columns:
            df['precip_below_p20'] = df['precipitation_sum_mm'] < df['precipitation_sum_mm_p20']
    
    # Soil moisture: either layer
    if 'sm1_mean' in df.columns or 'sm2_mean' in df.columns:
        # Initialize with correct index to avoid ambiguity errors
        sm_below_p20 = pd.Series([False] * len(df), index=df.index)
        sm_above_p80 = pd.Series([False] * len(df), index=df.index)
        
        if 'sm1_mean' in df.columns:
            if 'sm1_mean_p20' in df.columns:
                comparison = df['sm1_mean'] < df['sm1_mean_p20']
                sm_below_p20 = sm_below_p20 | comparison.fillna(False)
            if 'sm1_mean_p80' in df.columns:
                comparison = df['sm1_mean'] > df['sm1_mean_p80']
                sm_above_p80 = sm_above_p80 | comparison.fillna(False)
        
        if 'sm2_mean' in df.columns:
            if 'sm2_mean_p20' in df.columns:
                comparison = df['sm2_mean'] < df['sm2_mean_p20']
                sm_below_p20 = sm_below_p20 | comparison.fillna(False)
            if 'sm2_mean_p80' in df.columns:
                comparison = df['sm2_mean'] > df['sm2_mean_p80']
                sm_above_p80 = sm_above_p80 | comparison.fillna(False)
        
        df['sm_below_p20'] = sm_below_p20
        df['sm_above_p80'] = sm_above_p80
    
    if 'river_discharge' in df.columns:
        if 'river_discharge_p95' in df.columns:
            df['discharge_above_p95'] = df['river_discharge'] > df['river_discharge_p95']
    
    if 'ndvi_mean' in df.columns:
        if 'ndvi_mean_p20' in df.columns:
            df['ndvi_below_p20'] = df['ndvi_mean'] < df['ndvi_mean_p20']
    
    if 'hotspot_count' in df.columns:
        if 'hotspot_count_p95' in df.columns:
            df['high_hotspot'] = df['hotspot_count'] > df['hotspot_count_p95']
        df['has_hotspots'] = df['hotspot_count'] > 0
    
    if 'frp_mean' in df.columns:
        if 'frp_mean_p95' in df.columns:
            df['high_frp'] = df['frp_mean'] > df['frp_mean_p95']
    
    return df


def compute_temporal_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Compute month, day_of_year, days_since_last_precip."""
    df = df.copy()
    
    df['month'] = df['date'].dt.month
    df['day_of_year'] = df['date'].dt.dayofyear
    
    if 'precipitation_sum_mm' in df.columns:
        days_since_precip = []
        for region in df['region'].unique():
            region_mask = df['region'] == region
            region_df = df[region_mask].sort_values('date').copy()
            
            region_days = []
            last_precip_day = None
            
            for idx, row in region_df.iterrows():
                precip_value = row['precipitation_sum_mm']
                if isinstance(precip_value, pd.Series):
                    precip_value = precip_value.dropna().iloc[0] if not precip_value.dropna().empty else precip_value.iloc[0]
                if pd.notna(precip_value) and precip_value > 1.0:
                    last_precip_day = row['date']
                    region_days.append(0)
                elif last_precip_day is not None:
                    days = (row['date'] - last_precip_day).days
                    region_days.append(days)
                else:
                    region_days.append(np.nan)
            
            days_since_precip.extend(region_days)
        
        df['days_since_last_precip'] = days_since_precip
    
    return df


def engineer_features(
    df: pd.DataFrame,
    climatology: Optional[pd.DataFrame] = None,
    compute_climatology_from_data: bool = False
) -> tuple:
    """Main feature engineering pipeline. Returns (features_df, climatology_df).
    
    By default, loads climatology from BigQuery. Set compute_climatology_from_data=True
    to compute from df instead (slower, only needed if climatology table doesn't exist).
    """
    df = df.copy()
    
    df = compute_temporal_features(df)
    df = compute_spi_feature(df)
    df = compute_temporal_metadata(df)
    
    if climatology is None:
        if compute_climatology_from_data:
            climatology = compute_climatology(df)
        else:
            from ml_training.data_preparation.climatology_utils import load_climatology_from_bq
            climatology = load_climatology_from_bq()
            if climatology.empty:
                print("Warning: Climatology table not found. Computing from data...")
                climatology = compute_climatology(df)
    
    if climatology is not None and not climatology.empty:
        df = compute_anomaly_indicators(df, climatology)
    
    return df, climatology
