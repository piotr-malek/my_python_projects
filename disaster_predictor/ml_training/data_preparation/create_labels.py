import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ml_training.data_preparation.load_training_data import load_glc_events
from ml_training.data_preparation.climatology_utils import load_climatology_from_bq
from ml_training.config import (
    FIRE_TRAIN_START,
    FIRE_TRAIN_END,
    DROUGHT_TRAIN_START,
    DROUGHT_TRAIN_END,
    FLOOD_TRAIN_START,
    FLOOD_TRAIN_END,
    LANDSLIDE_TRAIN_START_ORIGINAL,
    LANDSLIDE_TRAIN_END_ORIGINAL,
    LANDSLIDE_TRAIN_START_NEW,
    LANDSLIDE_TRAIN_END_NEW,
)


def _naive_normalized_day(ts) -> pd.Timestamp:
    """BQ / pandas may return tz-aware datetimes; flood logic compares to naive feature dates."""
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return pd.NaT
    t = pd.Timestamp(t)
    if t.tzinfo is not None:
        t = pd.Timestamp(t.tz_convert("UTC").to_pydatetime().replace(tzinfo=None))
    return t.normalize()


def create_fire_labels(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Create fire labels from VIIRS hotspot data.

    Level 3: hotspot_count > 0 on date
    Level 2: hotspot_count > 0 within 3 days before/after
    Level 1: hotspot_count > 0 within 5 days before/after
    Level 0: No hotspots within 5 days

    Returns ``(labels, confidence)`` with confidence 1.0 for all rows (v1.3).
    """
    labels = pd.Series(0, index=df.index)

    if 'hotspot_count' not in df.columns:
        return labels, pd.Series(1.0, index=labels.index)
    
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    labels = labels.reindex(df.index)
    
    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_df = df[region_mask].copy()
        region_indices = region_df.index
        
        for i, (idx, row) in enumerate(region_df.iterrows()):
            date = row['date']
            
            # Check for hotspots on date
            if pd.notna(row['hotspot_count']) and row['hotspot_count'] > 0:
                labels.loc[idx] = 3
                continue
            
            # Check within 3 days
            date_3d_before = date - pd.Timedelta(days=3)
            date_3d_after = date + pd.Timedelta(days=3)
            window_3d = region_df[
                (region_df['date'] >= date_3d_before) & 
                (region_df['date'] <= date_3d_after) &
                (region_df.index != idx)
            ]
            if len(window_3d) > 0 and (window_3d['hotspot_count'].fillna(0) > 0).any():
                labels.loc[idx] = 2
                continue
            
            # Check within 5 days
            date_5d_before = date - pd.Timedelta(days=5)
            date_5d_after = date + pd.Timedelta(days=5)
            window_5d = region_df[
                (region_df['date'] >= date_5d_before) & 
                (region_df['date'] <= date_5d_after) &
                (region_df.index != idx)
            ]
            if len(window_5d) > 0 and (window_5d['hotspot_count'].fillna(0) > 0).any():
                labels.loc[idx] = 1

    return labels, pd.Series(1.0, index=labels.index)


def create_drought_labels(
    df: pd.DataFrame, climatology: Optional[pd.DataFrame] = None
) -> Tuple[pd.Series, pd.Series]:
    """Create drought labels from anomaly-based proxies.
    
    Level 3: (SM < p20 AND SPI < -1.4) OR (NDVI < p20 AND SPI < -1.4) - Severe drought
    Level 2: (NDVI < p20 AND SM < p20) OR (SPI < -0.95 AND SM < p20) - Moderate drought
    Level 1: (SPI < -0.7 AND (NDVI < p20 OR SM < p20)) OR (SPI < -0.95 AND NDVI < p20 AND SM >= p20) - Mild drought
    Level 0: Otherwise

    Returns ``(labels, confidence)`` with confidence 1.0 for all rows (v1.3).

    Note: Level 1 uses SPI < -0.7 (mild drought) with vegetation/soil moisture stress,
    or moderate SPI with only NDVI stress (vegetation responds faster than soil).
    Based on SPI classification: -0.5 to -0.7 = mild, -0.7 to -1.0 = moderate, < -1.0 = severe.
    
    Note: df should already have climatology merged (from engineer_features).
    If not, pass climatology to merge it.
    """
    labels = pd.Series(0, index=df.index)
    
    df = df.copy()
    
    # Merge climatology if not already merged
    if 'ndvi_mean_p20' not in df.columns and climatology is not None and not climatology.empty:
        if 'month' not in df.columns:
            df['month'] = df['date'].dt.month
        df = df.merge(climatology, on=['region', 'month'], how='left')
    
    if 'ndvi_mean_p20' not in df.columns and 'sm1_mean_p20' not in df.columns:
        return labels, pd.Series(1.0, index=labels.index)

    for idx, row in df.iterrows():
        ndvi_below_p20 = False
        sm_below_p20 = False
        spi_severe = False
        spi_moderate = False
        
        # Find NDVI column (may be ndvi_mean_x or ndvi_mean_y after merge)
        ndvi_col = None
        for col in ['ndvi_mean', 'ndvi_mean_x', 'ndvi_mean_y']:
            if col in row and pd.notna(row[col]):
                ndvi_col = col
                break
        
        if ndvi_col:
            ndvi_p20_col = 'ndvi_mean_p20'
            if ndvi_p20_col in row and pd.notna(row[ndvi_p20_col]):
                ndvi_below_p20 = row[ndvi_col] < row[ndvi_p20_col]
        
        if 'sm1_mean' in row and pd.notna(row['sm1_mean']):
            sm1_p20_col = 'sm1_mean_p20'
            if sm1_p20_col in row and pd.notna(row[sm1_p20_col]):
                if row['sm1_mean'] < row[sm1_p20_col]:
                    sm_below_p20 = True
        
        if 'sm2_mean' in row and pd.notna(row['sm2_mean']):
            sm2_p20_col = 'sm2_mean_p20'
            if sm2_p20_col in row and pd.notna(row[sm2_p20_col]):
                if row['sm2_mean'] < row[sm2_p20_col]:
                    sm_below_p20 = True
        
        if 'spi30' in row and pd.notna(row['spi30']):
            if row['spi30'] < -1.4:
                spi_severe = True
            elif row['spi30'] < -0.95:
                spi_moderate = True
        
        # Check for mild SPI (Level 1 threshold)
        spi_mild = False
        if 'spi30' in row and pd.notna(row['spi30']):
            if row['spi30'] < -0.7:  # Mild drought threshold (between -0.5 and -0.95)
                spi_mild = True
        
        # Level 3: Severe SPI with either low SM OR low NDVI
        if (sm_below_p20 and spi_severe) or (ndvi_below_p20 and spi_severe):
            labels.loc[idx] = 3
        # Level 2: (NDVI + SM) OR (moderate SPI + SM)
        elif (ndvi_below_p20 and sm_below_p20) or (spi_moderate and sm_below_p20):
            labels.loc[idx] = 2
        # Level 1: Mild SPI (-0.7 to -0.95) with either NDVI OR SM below threshold
        # OR moderate SPI with NDVI (but not SM)
        elif (spi_mild and (ndvi_below_p20 or sm_below_p20)) or (spi_moderate and ndvi_below_p20 and not sm_below_p20):
            labels.loc[idx] = 1

    return labels, pd.Series(1.0, index=labels.index)


def create_flood_labels(
    df: pd.DataFrame,
    climatology: Optional[pd.DataFrame] = None,
    gfd_events: Optional[pd.DataFrame] = None,
    worldfloods_events: Optional[pd.DataFrame] = None,
    return_confidence: bool = False,
) -> Tuple[pd.Series, pd.Series] | pd.Series:
    """Create flood labels from GFD + WorldFloods event dates and river-discharge proxy.
    
    Event-based (GFD / WorldFloods): Level 3 = flood event on date, Level 2 = within 2 days.
    Discharge proxy for other dates: Level 3 = river_discharge > p95 on date, Level 2 = > p95 within 2d,
    Level 1 = river_discharge > p80 AND precipitation_sum_mm > p80.
    
    df should have climatology merged (from engineer_features) when using the discharge proxy.
    """
    df = df.copy()
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    labels = pd.Series(0, index=df.index)
    confidence = pd.Series(0.5, index=df.index)

    # Combine GFD and WorldFloods into (region, date) event set
    flood_event_dates = set()
    for events in (gfd_events, worldfloods_events):
        if events is not None and not events.empty and 'region' in events.columns and 'date' in events.columns:
            for _, row in events.iterrows():
                d = _naive_normalized_day(row["date"])
                if pd.isna(d):
                    continue
                r = row["region"]
                if pd.isna(r) or (isinstance(r, str) and not str(r).strip()):
                    continue
                flood_event_dates.add((str(r).strip(), d))

    # Apply event-based labels first (GFD + WorldFloods)
    if flood_event_dates:
        for region in df['region'].unique():
            region_key = str(region).strip() if region is not None and not pd.isna(region) else None
            if not region_key:
                continue
            region_mask = df['region'] == region
            region_df = df[region_mask].copy()
            region_events = {d for (r, d) in flood_event_dates if r == region_key}
            if not region_events:
                continue
            for idx, row in region_df.iterrows():
                date = _naive_normalized_day(row["date"])
                if pd.isna(date):
                    continue
                if (region_key, date) in flood_event_dates:
                    labels.loc[idx] = 3
                    confidence.loc[idx] = 1.0
                    continue
                date_2d_before = date - pd.Timedelta(days=2)
                date_2d_after = date + pd.Timedelta(days=2)
                if any(
                    date_2d_before <= d <= date_2d_after
                    for d in region_events
                    if pd.notna(d)
                ):
                    labels.loc[idx] = 2
                    confidence.loc[idx] = 1.0

    # River-discharge proxy for rows still at 0 (no event label)
    if 'river_discharge' not in df.columns:
        return (labels, confidence) if return_confidence else labels

    if 'river_discharge_p95' not in df.columns and climatology is not None and not climatology.empty:
        if 'month' not in df.columns:
            df['month'] = df['date'].dt.month
        df = df.merge(climatology, on=['region', 'month'], how='left')

    if 'river_discharge_p95' not in df.columns:
        return (labels, confidence) if return_confidence else labels

    discharge_p95_col = 'river_discharge_p95'
    discharge_p80_col = 'river_discharge_p80'
    precip_p80_col = 'precipitation_sum_mm_p80'

    if discharge_p80_col not in df.columns:
        df['month'] = df['date'].dt.month
        for region in df['region'].unique():
            region_mask = df['region'] == region
            region_df = df[region_mask].copy()
            for month in range(1, 13):
                month_mask = df['month'] == month
                month_df = region_df[region_df['month'] == month]
                if 'river_discharge' in month_df.columns:
                    discharge_values = month_df['river_discharge'].dropna()
                    if len(discharge_values) > 0:
                        discharge_p80 = discharge_values.quantile(0.80)
                        combined_mask = region_mask & month_mask
                        df.loc[combined_mask, discharge_p80_col] = discharge_p80

    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_df = df[region_mask].copy()
        proxy_mask = region_mask & (labels == 0)
        for idx in df.index[proxy_mask]:
            row = df.loc[idx]
            date = row['date']
            discharge = row['river_discharge']
            if pd.isna(discharge):
                continue
            discharge_p95 = row.get(discharge_p95_col)
            discharge_p80 = row.get(discharge_p80_col)
            precip_p80 = row.get(precip_p80_col)
            if pd.isna(discharge_p95) and pd.isna(discharge_p80):
                continue
            if pd.notna(discharge_p95) and discharge > discharge_p95:
                labels.loc[idx] = 3
                confidence.loc[idx] = 0.5
                continue
            date_2d_before = date - pd.Timedelta(days=2)
            date_2d_after = date + pd.Timedelta(days=2)
            window_2d = region_df[
                (region_df['date'] >= date_2d_before) & (region_df['date'] <= date_2d_after) & (region_df.index != idx)
            ]
            if pd.notna(discharge_p95) and len(window_2d) > 0 and (window_2d['river_discharge'] > discharge_p95).any():
                labels.loc[idx] = 2
                confidence.loc[idx] = 0.5
                continue
            if pd.notna(discharge_p80) and discharge > discharge_p80 and pd.notna(precip_p80) and 'precipitation_sum_mm' in row:
                precip = row['precipitation_sum_mm']
                if pd.notna(precip) and precip > precip_p80:
                    labels.loc[idx] = 1
                    confidence.loc[idx] = 0.5

    return (labels, confidence) if return_confidence else labels


def _compute_landslide_proxy_features(df: pd.DataFrame, climatology: pd.DataFrame) -> pd.DataFrame:
    """Compute proxy features for landslide labels (30-day windows, different from model features)."""
    df = df.copy()
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    
    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month
    
    # Merge climatology for discharge percentiles if not already on frame (engineer_features may have merged)
    if (
        climatology is not None
        and not climatology.empty
        and 'river_discharge_p95' not in df.columns
    ):
        df = df.merge(climatology, on=['region', 'month'], how='left')
    
    # 30-day cumulative precipitation and river discharge
    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_df = df[region_mask].copy()
        
        if 'precipitation_sum_mm' in region_df.columns:
            df.loc[region_mask, 'precip_30d_sum'] = (
                region_df['precipitation_sum_mm'].rolling(window=30, min_periods=1).sum()
            )
        
        if 'river_discharge' in region_df.columns:
            df.loc[region_mask, 'discharge_30d_sum'] = (
                region_df['river_discharge'].rolling(window=30, min_periods=1).sum()
            )
            
            discharge_p95_col = 'river_discharge_p95'
            if discharge_p95_col in region_df.columns:
                region_df['discharge_above_p95'] = (
                    region_df['river_discharge'] > region_df[discharge_p95_col]
                )
                
                consecutive = []
                current_streak = 0
                for val in region_df['discharge_above_p95']:
                    if val:
                        current_streak += 1
                    else:
                        current_streak = 0
                    consecutive.append(current_streak)
                
                df.loc[region_mask, 'consecutive_days_discharge_above_p95'] = consecutive
        
        # Soil saturation index (21-day mean)
        if 'sm1_mean' in region_df.columns and 'sm2_mean' in region_df.columns:
            soil_sat = (region_df['sm1_mean'] + region_df['sm2_mean']) / 2
            df.loc[region_mask, 'soil_saturation_21d_mean'] = (
                soil_sat.rolling(window=21, min_periods=1).mean()
            )
    
    return df


def _create_landslide_proxy_labels(df: pd.DataFrame, climatology: pd.DataFrame) -> pd.Series:
    """Create proxy labels for landslides using temporal-separated features."""
    labels = pd.Series(0, index=df.index)
    
    df = _compute_landslide_proxy_features(df, climatology)
    
    # Compute percentiles for 30-day features from data itself (not in climatology)
    # Add month column to main df if needed
    if 'month' not in df.columns:
        df['month'] = df['date'].dt.month
    
    for region in df['region'].unique():
        region_mask = df['region'] == region
        region_df = df[region_mask].copy()
        
        for month in range(1, 13):
            # Create month_mask from original df (not region_df) to match region_mask index
            month_mask = df['month'] == month
            month_df = region_df[region_df['month'] == month]
            combined_mask = region_mask & month_mask
            
            if 'precip_30d_sum' in month_df.columns:
                precip_30d_values = month_df['precip_30d_sum'].dropna()
                if len(precip_30d_values) > 0:
                    precip_p99 = precip_30d_values.quantile(0.99)
                    df.loc[combined_mask, 'precip_30d_p99'] = precip_p99
            
            if 'discharge_30d_sum' in month_df.columns:
                discharge_30d_values = month_df['discharge_30d_sum'].dropna()
                if len(discharge_30d_values) > 0:
                    discharge_p99 = discharge_30d_values.quantile(0.99)
                    df.loc[combined_mask, 'discharge_30d_p99'] = discharge_p99
            
            if 'soil_saturation_21d_mean' in month_df.columns:
                sat_21d_values = month_df['soil_saturation_21d_mean'].dropna()
                if len(sat_21d_values) > 0:
                    sat_p90 = sat_21d_values.quantile(0.90)
                    df.loc[combined_mask, 'soil_sat_21d_p90'] = sat_p90
    
    for idx, row in df.iterrows():
        extreme_precip = False
        extreme_discharge = False
        extreme_sat = False
        consecutive_high_discharge = False
        extreme_slope = False
        
        # Check 30-day precipitation > p99
        if 'precip_30d_sum' in row and pd.notna(row['precip_30d_sum']):
            precip_p99 = row.get('precip_30d_p99', None)
            if pd.notna(precip_p99):
                extreme_precip = row['precip_30d_sum'] > precip_p99
        
        # Check 30-day discharge sum > p99
        if 'discharge_30d_sum' in row and pd.notna(row['discharge_30d_sum']):
            discharge_p99 = row.get('discharge_30d_p99', None)
            if pd.notna(discharge_p99):
                extreme_discharge = row['discharge_30d_sum'] > discharge_p99
        
        # Check soil saturation > p90
        if 'soil_saturation_21d_mean' in row and pd.notna(row['soil_saturation_21d_mean']):
            sat_p90 = row.get('soil_sat_21d_p90', None)
            if pd.notna(sat_p90):
                extreme_sat = row['soil_saturation_21d_mean'] > sat_p90
        
        if 'consecutive_days_discharge_above_p95' in row and pd.notna(row['consecutive_days_discharge_above_p95']):
            consecutive_high_discharge = row['consecutive_days_discharge_above_p95'] >= 5
        
        # Check extreme slope (use p95 of actual slopes, ~20°, since max is 21.9°)
        if 'slope_mean' in row and pd.notna(row['slope_mean']):
            extreme_slope = row['slope_mean'] > 20
        
        # Level assignment
        if extreme_precip and extreme_discharge and extreme_sat and consecutive_high_discharge and extreme_slope:
            labels.loc[idx] = 3
        elif (extreme_precip and extreme_discharge) or (extreme_discharge and extreme_sat):
            labels.loc[idx] = 2
        elif extreme_precip or extreme_discharge:
            labels.loc[idx] = 1
    
    return labels


def create_landslide_labels(
    df: pd.DataFrame,
    glc_events: Optional[pd.DataFrame] = None,
    climatology: Optional[pd.DataFrame] = None,
) -> Tuple[pd.Series, pd.Series]:
    """Create landslide labels from GLC events + proxy labels.
    
    Returns: (labels, confidence_weights)
    - GLC events: confidence = 1.0
    - Proxy labels: confidence = 0.7
    """
    labels = pd.Series(0, index=df.index)
    confidence = pd.Series(0.7, index=df.index)
    
    df = df.copy()
    df = df.sort_values(['region', 'date']).reset_index(drop=True)
    labels = labels.reindex(df.index)
    confidence = confidence.reindex(df.index)
    
    # Create GLC labels first (highest confidence)
    if glc_events is not None and not glc_events.empty:
        glc_events = glc_events.sort_values(['region', 'date'])
        
        for region in df['region'].unique():
            region_mask = df['region'] == region
            region_df = df[region_mask].copy()
            region_glc = glc_events[glc_events['region'] == region]
            
            if region_glc.empty:
                continue
            
            for idx, row in region_df.iterrows():
                date = row['date']
                
                # Check for GLC event on date
                glc_on_date = region_glc[region_glc['date'] == date]
                if not glc_on_date.empty:
                    labels.loc[idx] = 3
                    confidence.loc[idx] = 1.0
                    continue
                
                # Check within 7 days
                date_7d_before = date - pd.Timedelta(days=7)
                date_7d_after = date + pd.Timedelta(days=7)
                glc_7d = region_glc[
                    (region_glc['date'] >= date_7d_before) & 
                    (region_glc['date'] <= date_7d_after)
                ]
                if not glc_7d.empty:
                    labels.loc[idx] = 2
                    confidence.loc[idx] = 1.0
                    continue
                
                # Check within 14 days
                date_14d_before = date - pd.Timedelta(days=14)
                date_14d_after = date + pd.Timedelta(days=14)
                glc_14d = region_glc[
                    (region_glc['date'] >= date_14d_before) & 
                    (region_glc['date'] <= date_14d_after)
                ]
                if not glc_14d.empty:
                    labels.loc[idx] = 1
                    confidence.loc[idx] = 1.0
    
    # Create proxy labels for dates without GLC events
    if climatology is not None and not climatology.empty:
        proxy_labels = _create_landslide_proxy_labels(df, climatology)

        # Only use proxy where GLC didn't assign a label (confidence < 1.0)
        proxy_mask = confidence < 1.0
        labels.loc[proxy_mask] = proxy_labels.loc[proxy_mask]
        # v1.3: trust GLC at 1.0; proxy-derived positives at 0.7; clear negatives at 1.0
        confidence.loc[proxy_mask & (labels > 0)] = 0.7
        confidence.loc[proxy_mask & (labels == 0)] = 1.0

    # v1.3: three classes only — map any Level 3 down to Level 2 (severe)
    labels = labels.replace(3, 2).astype(int)
    return labels, confidence


def create_labels(
    df: pd.DataFrame,
    disaster_type: str,
    climatology: Optional[pd.DataFrame] = None,
    glc_events: Optional[pd.DataFrame] = None,
    gfd_events: Optional[pd.DataFrame] = None,
    worldfloods_events: Optional[pd.DataFrame] = None,
    return_confidence: bool = False,
) -> pd.Series | Tuple[pd.Series, pd.Series]:
    """Main function to create labels for specified disaster type.
    
    Returns labels Series (0-3) for training data.
    For landslides, also returns confidence weights (use create_landslide_labels directly).
    """
    if climatology is None:
        climatology = load_climatology_from_bq()
    
    if disaster_type == 'fire':
        labels, conf = create_fire_labels(df)
        return (labels, conf) if return_confidence else labels
    elif disaster_type == 'drought':
        labels, conf = create_drought_labels(df, climatology)
        return (labels, conf) if return_confidence else labels
    elif disaster_type == 'flood':
        result = create_flood_labels(
            df,
            climatology,
            gfd_events=gfd_events,
            worldfloods_events=worldfloods_events,
            return_confidence=return_confidence,
        )
        return result
    elif disaster_type == 'landslide':
        labels, confidence = create_landslide_labels(df, glc_events, climatology)
        return (labels, confidence) if return_confidence else labels
    else:
        raise ValueError(f"Unknown disaster type: {disaster_type}")
