"""
ML-based disaster detection: risk level is the model prediction (0–3).

Weather and forecast series are loaded to build human-readable outlooks when risk ≥ 1.
"""

import os
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.bq_utils import load_from_bigquery
from ml_training.models.model_predictor import ModelPredictor, load_predictor
from ml_training.data_preparation.load_training_data import (
    load_era5_data,
    load_modis_data,
    load_viirs_data,
    load_terrain_data,
    load_region_descriptors,
    load_river_discharge_data,
    merge_datasets,
    handle_missing_data,
)
from ml_training.data_preparation.feature_engineering import engineer_features
from ml_training.config import REQUIRED_FEATURES, PROJECT_ID, CLIMATOLOGY_DATASET
from utils.datasets.era5_utils import compute_spi
from config import get_region_name

# Smoothed class probabilities for alert stability (flood / landslide)
ML_ROLLING_WINDOW_DAYS = 5
ML_ROLLING_PROBA_HAZARDS = frozenset({"flood", "landslide"})


class MLDisasterDetection:
    """ML-based disaster detection; risk score equals the ML class prediction."""
    
    def __init__(self, project_id: str = None, region_name: str = None):
        """Initialize detection system."""
        self.project_id = project_id or PROJECT_ID
        self.region_name = region_name or get_region_name()
        self.dataset_id = "daily_ingestion"
        
        # Load climatology once (monthly percentiles)
        self.climatology = self._load_climatology()
        
        # Cache ML predictors (lazy loading)
        self._predictors = {}
    
    def _load_climatology(self) -> pd.DataFrame:
        """Load monthly climatology percentiles from BigQuery."""
        # Use a smaller query to check if table exists and get schema
        query = f"""
        SELECT *
        FROM `{self.project_id}.{CLIMATOLOGY_DATASET}.climatology_monthly`
        LIMIT 1
        """
        
        try:
            # We don't actually need to load the whole table into memory at init.
            # It's better to load it once when needed or keep it as a lazy property.
            return pd.DataFrame()
        except Exception as e:
            print(f"Warning: Could not load climatology: {e}")
        
        return pd.DataFrame()

    @property
    def climatology_data(self) -> pd.DataFrame:
        """Lazy load climatology data."""
        if not hasattr(self, '_climatology_cache') or self._climatology_cache is None or self._climatology_cache.empty:
            # Use a lock-like check to avoid multiple prints in parallel
            if not hasattr(self, '_loading_climatology'):
                self._loading_climatology = True
                print("Loading climatology data into memory...")
                query = f"""
                SELECT *
                FROM `{self.project_id}.{CLIMATOLOGY_DATASET}.climatology_monthly`
                ORDER BY region, month
                """
                try:
                    self._climatology_cache = load_from_bigquery(query, project_id=self.project_id)
                except Exception as e:
                    print(f"Warning: Could not load climatology: {e}")
                    self._climatology_cache = pd.DataFrame()
                finally:
                    delattr(self, '_loading_climatology')
            else:
                # If another thread is already loading, wait or return empty (shouldn't happen with preloading)
                return getattr(self, '_climatology_cache', pd.DataFrame())
        return self._climatology_cache
    
    def preload_climatology(self):
        """Force load climatology data in main thread."""
        _ = self.climatology_data
    
    def _get_predictor(self, disaster_type: str) -> ModelPredictor:
        """Get or load ML predictor for disaster type."""
        if disaster_type not in self._predictors:
            # Clear other predictors to save memory if we're switching types
            # (Only relevant if processing hazards sequentially in one process)
            if self._predictors:
                print(f"Clearing previous predictors to free memory...")
                self._predictors = {}
                import gc
                gc.collect()
            self._predictors[disaster_type] = load_predictor(disaster_type)
        return self._predictors[disaster_type]
    
    def _get_climatology_percentile(
        self, 
        region: str, 
        month: int, 
        metric: str, 
        percentile: str
    ) -> Optional[float]:
        """Get climatology percentile value for region/month/metric."""
        df = self.climatology_data
        if df.empty:
            return None
        
        col_name = f"{metric}_{percentile}"
        if col_name not in df.columns:
            return None
        
        mask = (df['region'] == region) & (df['month'] == month)
        result = df[mask]
        
        if result.empty or pd.isna(result[col_name].iloc[0]):
            return None
        
        return float(result[col_name].iloc[0])
    
    def _merge_river_discharge_recent(
        self,
        weather_df: pd.DataFrame,
        region: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Attach river_discharge from GloFAS + daily_ingestion (same as training)."""
        if weather_df is None or weather_df.empty:
            return weather_df
        dd = load_river_discharge_data(
            regions=[region],
            start_date=start_date,
            end_date=end_date,
            project_id=self.project_id,
        )
        if dd is None or dd.empty:
            return weather_df
        out = weather_df.copy()
        out["date"] = pd.to_datetime(out["date"]).dt.normalize()
        dd = dd.drop_duplicates(subset=["date", "region"], keep="last")
        return out.merge(
            dd[["date", "region", "river_discharge"]],
            on=["date", "region"],
            how="left",
        )
    
    def _load_recent_weather(
        self, 
        region: str, 
        date: pd.Timestamp, 
        days_back: int = 32
    ) -> pd.DataFrame:
        """Load recent weather data for climatology comparisons.
        
        Loads from daily_ingestion.era5 (preferred) and fills gaps with 
        daily_ingestion.openmeteo_weather (backup). Prefers ERA5 when both 
        sources available for same date.
        """
        start_date = (date - pd.Timedelta(days=days_back)).strftime('%Y-%m-%d')
        end_date = date.strftime('%Y-%m-%d')
        
        # Load ERA5 data from daily_ingestion dataset (preferred source)
        era5_query = f"""
        SELECT 
            date,
            region,
            temp_2m_mean_C,
            precipitation_sum_mm,
            sm1_mean,
            sm2_mean
        FROM `{self.project_id}.{self.dataset_id}.era5`
        WHERE region = '{region}'
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY date
        """
        
        era5_df = pd.DataFrame()
        try:
            era5_df = load_from_bigquery(era5_query, project_id=self.project_id)
            if era5_df is not None and not era5_df.empty:
                era5_df['date'] = pd.to_datetime(era5_df['date'])
        except Exception as e:
            print(f"Warning: Could not load ERA5 data: {e}")
        
        # Load OpenMeteo data from daily_ingestion dataset (backup source)
        openmeteo_query = f"""
        SELECT 
            date,
            region_name as region,
            temperature_2m_mean,
            precipitation_sum,
            soil_moisture_0_to_7cm_mean,
            soil_moisture_7_to_28cm_mean
        FROM `{self.project_id}.{self.dataset_id}.openmeteo_weather`
        WHERE region_name = '{region}'
          AND date >= '{start_date}'
          AND date <= '{end_date}'
        ORDER BY date
        """
        
        openmeteo_df = pd.DataFrame()
        try:
            openmeteo_df = load_from_bigquery(openmeteo_query, project_id=self.project_id)
            if openmeteo_df is not None and not openmeteo_df.empty:
                openmeteo_df['date'] = pd.to_datetime(openmeteo_df['date'])
                # Map OpenMeteo fields to ERA5 format
                openmeteo_df = openmeteo_df.rename(columns={
                    'temperature_2m_mean': 'temp_2m_mean_C',
                    'precipitation_sum': 'precipitation_sum_mm',
                    'soil_moisture_0_to_7cm_mean': 'sm1_mean',
                    'soil_moisture_7_to_28cm_mean': 'sm2_mean'
                })
        except Exception as e:
            print(f"Warning: Could not load OpenMeteo data: {e}")
        
        # Merge data: prefer ERA5 when both available, fill gaps with OpenMeteo
        if era5_df.empty and openmeteo_df.empty:
            return pd.DataFrame()
        
        if era5_df.empty:
            return self._merge_river_discharge_recent(
                openmeteo_df, region, start_date, end_date
            )
        
        if openmeteo_df.empty:
            return self._merge_river_discharge_recent(era5_df, region, start_date, end_date)
        
        # Both available - merge, preferring ERA5
        # Filter to the specific region we're looking for
        def _dedupe_by_date(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return df
            return df.sort_values('date').drop_duplicates(subset=['date'], keep='last')

        era5_region = _dedupe_by_date(era5_df[era5_df['region'] == region].copy())
        openmeteo_region = _dedupe_by_date(openmeteo_df[openmeteo_df['region'] == region].copy())
        
        if era5_region.empty and openmeteo_region.empty:
            return pd.DataFrame()
        
        if era5_region.empty:
            return self._merge_river_discharge_recent(
                openmeteo_region, region, start_date, end_date
            )
        
        if openmeteo_region.empty:
            return self._merge_river_discharge_recent(era5_region, region, start_date, end_date)
        
        # Both available - merge, preferring ERA5
        # Create date index for easy merging
        era5_indexed = era5_region.set_index('date')
        openmeteo_indexed = openmeteo_region.set_index('date')
        
        # Start with ERA5 (preferred source)
        merged_df = era5_indexed.copy()
        
        # Fill missing dates/values from OpenMeteo
        # Only fill fields that OpenMeteo can provide (temp, precip, soil moisture)
        fillable_fields = ['temp_2m_mean_C', 'precipitation_sum_mm', 'sm1_mean', 'sm2_mean']
        
        for date_idx in openmeteo_indexed.index:
            if date_idx not in merged_df.index:
                # Missing date - add row from OpenMeteo
                new_row = pd.Series(index=merged_df.columns.tolist(), dtype=object)
                # Fill fillable fields from OpenMeteo
                for field in fillable_fields:
                    if field in openmeteo_indexed.columns:
                        new_row[field] = openmeteo_indexed.loc[date_idx, field]
                new_row['region'] = region
                merged_df.loc[date_idx] = new_row
            else:
                # Date exists - fill missing fields from OpenMeteo
                for field in fillable_fields:
                    if field in openmeteo_indexed.columns:
                        era5_value = merged_df.loc[date_idx, field]
                        # Handle both scalar and Series cases
                        if isinstance(era5_value, pd.Series):
                            if era5_value.isna().all() or len(era5_value) == 0:
                                merged_df.loc[date_idx, field] = openmeteo_indexed.loc[date_idx, field]
                            elif len(era5_value) == 1:
                                # Single value Series - check if it's NA
                                if pd.isna(era5_value.iloc[0]):
                                    merged_df.loc[date_idx, field] = openmeteo_indexed.loc[date_idx, field]
                        elif pd.isna(era5_value):
                            # Scalar value is NA
                            merged_df.loc[date_idx, field] = openmeteo_indexed.loc[date_idx, field]
        
        # Reset index and sort
        merged_df = merged_df.reset_index()
        merged_df = merged_df.sort_values('date')
        
        # Ensure region column is consistent
        merged_df['region'] = region
        
        return self._merge_river_discharge_recent(
            merged_df, region, start_date, end_date
        )
    
    def _load_forecast(
        self, 
        region: str, 
        date: pd.Timestamp, 
        days_ahead: int = 7,
        include_flood: bool = True
    ) -> pd.DataFrame:
        """Load forecast data from OpenMeteo in daily_ingestion dataset.
        
        Prefers columns merged at ingestion (soil moisture, GloFAS river_discharge).
        Falls back to temperature + precipitation only if extended schema is unavailable.
        """
        end_date = (date + pd.Timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        d0 = date.strftime('%Y-%m-%d')
        base_where = f"""
        FROM `{self.project_id}.{self.dataset_id}.openmeteo_forecast`
        WHERE region_name = '{region}'
          AND date > '{d0}'
          AND date <= '{end_date}'
        ORDER BY date
        """
        extended_sql = (
            f"SELECT date, region_name as region, temperature_2m_max, precipitation_sum, "
            f"sm1_mean, sm2_mean, river_discharge {base_where}"
        )
        minimal_sql = (
            f"SELECT date, region_name as region, temperature_2m_max, precipitation_sum {base_where}"
        )
        last_err: Optional[Exception] = None
        for forecast_query in (extended_sql, minimal_sql):
            try:
                df = load_from_bigquery(forecast_query, project_id=self.project_id)
                if df is not None and not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    return df
            except Exception as e:
                last_err = e
                continue
        if last_err is not None:
            print(f"Warning: Could not load forecast for {region}: {last_err}")
        return pd.DataFrame()

    def _percentile_approx_from_anchors(
        self,
        value: float,
        region: str,
        month: int,
        metric: str,
        scale: float = 1.0,
    ) -> Optional[float]:
        """Map a value to ~0–100 using monthly climatology anchors for ``metric`` (scaled by ``scale``)."""
        anchor_order = [
            (5, "p05"),
            (10, "p10"),
            (20, "p20"),
            (50, "p50"),
            (80, "p80"),
            (95, "p95"),
        ]
        pairs: List[Tuple[float, float]] = []
        for pnum, pname in anchor_order:
            v = self._get_climatology_percentile(region, month, metric, pname)
            if v is not None and not (isinstance(v, float) and np.isnan(v)):
                pairs.append((float(v) * scale, float(pnum)))
        if len(pairs) < 2:
            return None
        pairs.sort(key=lambda x: x[0])
        vals = [p[0] for p in pairs]
        pcts = [p[1] for p in pairs]
        if value <= vals[0]:
            if vals[0] <= 0:
                return max(0.0, min(100.0, pcts[0]))
            est = pcts[0] * (value / vals[0])
            return max(0.0, min(100.0, est))
        if value >= vals[-1]:
            span = max(vals[-1] - vals[-2], 1e-9)
            extra = (value - vals[-1]) / span
            est = min(100.0, pcts[-1] + (100.0 - pcts[-1]) * min(1.0, extra * 0.5))
            return max(0.0, min(100.0, est))
        for i in range(len(vals) - 1):
            if vals[i] <= value <= vals[i + 1]:
                t = (value - vals[i]) / (vals[i + 1] - vals[i] + 1e-15)
                return max(0.0, min(100.0, pcts[i] + t * (pcts[i + 1] - pcts[i])))
        return None

    def _outlook_metric_entry(
        self,
        value: Optional[float],
        region: str,
        month: int,
        metric: str,
        *,
        scale: float = 1.0,
        unit: Optional[str] = None,
        omit_value: bool = False,
    ) -> Optional[Dict]:
        """Single metric for ``recent_outlook`` / ``forecast_outlook`` (see docs/risk_outlook_plan.md)."""
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        pa = self._percentile_approx_from_anchors(float(value), region, month, metric, scale=scale)
        if pa is None:
            return None
        out: Dict = {"percentile_approx": round(float(pa), 1)}
        if not omit_value and unit:
            out["value"] = round(float(value), 4)
            out["unit"] = unit
        return out

    def _outlook_put(
        self,
        dest: Dict,
        key: str,
        *,
        value: Optional[float],
        region: str,
        month: int,
        metric: str,
        scale: float = 1.0,
        unit: Optional[str] = None,
        omit_value: bool = False,
    ) -> None:
        entry = self._outlook_metric_entry(
            value, region, month, metric,
            scale=scale, unit=unit, omit_value=omit_value,
        )
        if entry:
            dest[key] = entry

    def _build_weather_outlooks(
        self,
        disaster_type: str,
        region: str,
        date: pd.Timestamp,
        weather_df: pd.DataFrame,
        forecast_df: Optional[pd.DataFrame],
    ) -> Tuple[Dict, Dict]:
        """Build recent_outlook and forecast_outlook dicts for LLM / daily_evaluation JSON columns."""
        month = int(date.month)
        recent: Dict = {}
        forecast: Dict = {}
        if weather_df.empty:
            return recent, forecast

        dfw = weather_df.copy()
        dfw["date"] = pd.to_datetime(dfw["date"]).dt.normalize()
        d0 = pd.Timestamp(date).normalize()

        recent_7d = dfw[dfw["date"] >= d0 - pd.Timedelta(days=7)]
        recent_14d = dfw[dfw["date"] >= d0 - pd.Timedelta(days=14)]
        recent_30d = dfw[dfw["date"] >= d0 - pd.Timedelta(days=30)]
        current = dfw[dfw["date"] == d0]

        precip_sum_7d = None
        if not recent_7d.empty and "precipitation_sum_mm" in recent_7d.columns:
            precip_sum_7d = float(recent_7d["precipitation_sum_mm"].sum())

        temp_max_7d = None
        if not recent_7d.empty and "temp_2m_mean_C" in recent_7d.columns:
            temp_max_7d = float(recent_7d["temp_2m_mean_C"].max())

        sm1_curr = None
        if not current.empty and "sm1_mean" in current.columns:
            v = current["sm1_mean"].iloc[0]
            if pd.notna(v):
                sm1_curr = float(v)

        sm1_mean_14d = None
        if not recent_14d.empty and "sm1_mean" in recent_14d.columns:
            s = recent_14d["sm1_mean"].dropna()
            if len(s) > 0:
                sm1_mean_14d = float(s.mean())

        precip_sum_30d = None
        if not recent_30d.empty and "precipitation_sum_mm" in recent_30d.columns:
            precip_sum_30d = float(recent_30d["precipitation_sum_mm"].sum())

        rd_7d_sum = None
        if not recent_7d.empty and "river_discharge" in recent_7d.columns:
            s = recent_7d["river_discharge"].dropna()
            if len(s) > 0:
                rd_7d_sum = float(s.sum())

        if disaster_type == "fire":
            self._outlook_put(
                recent, "precipitation_7d_sum_mm",
                value=precip_sum_7d, region=region, month=month,
                metric="precipitation_sum_mm", scale=7.0, unit="mm",
            )
            self._outlook_put(
                recent, "temperature_7d_max_C",
                value=temp_max_7d, region=region, month=month,
                metric="temp_2m_mean_C", scale=1.0, unit="°C",
            )
            self._outlook_put(
                recent, "sm1_mean",
                value=sm1_curr, region=region, month=month,
                metric="sm1_mean", scale=1.0, omit_value=True,
            )
        elif disaster_type == "drought":
            self._outlook_put(
                recent, "precipitation_30d_sum_mm",
                value=precip_sum_30d, region=region, month=month,
                metric="precipitation_sum_mm", scale=30.0, unit="mm",
            )
            self._outlook_put(
                recent, "sm1_mean_14d",
                value=sm1_mean_14d, region=region, month=month,
                metric="sm1_mean", scale=1.0, omit_value=True,
            )
        elif disaster_type in ("flood", "landslide"):
            self._outlook_put(
                recent, "precipitation_7d_sum_mm",
                value=precip_sum_7d, region=region, month=month,
                metric="precipitation_sum_mm", scale=7.0, unit="mm",
            )
            if rd_7d_sum is not None:
                self._outlook_put(
                    recent, "river_discharge_7d_sum_m3s",
                    value=rd_7d_sum, region=region, month=month,
                    metric="river_discharge", scale=7.0, omit_value=True,
                )

        if forecast_df is not None and not forecast_df.empty:
            ff = forecast_df.copy()
            f_precip7 = float(ff["precipitation_sum"].sum()) if "precipitation_sum" in ff.columns else None
            f_temp_max = float(ff["temperature_2m_max"].max()) if "temperature_2m_max" in ff.columns else None
            f_sm1_mean = None
            if "sm1_mean" in ff.columns and ff["sm1_mean"].notna().any():
                f_sm1_mean = float(ff["sm1_mean"].mean())
            f_rd_sum = None
            if "river_discharge" in ff.columns and ff["river_discharge"].notna().any():
                f_rd_sum = float(ff["river_discharge"].sum())

            if disaster_type == "fire":
                self._outlook_put(
                    forecast, "precipitation_7d_sum_mm",
                    value=f_precip7, region=region, month=month,
                    metric="precipitation_sum_mm", scale=7.0, unit="mm",
                )
                self._outlook_put(
                    forecast, "temperature_7d_max_C",
                    value=f_temp_max, region=region, month=month,
                    metric="temp_2m_mean_C", scale=1.0, unit="°C",
                )
                if f_sm1_mean is not None:
                    self._outlook_put(
                        forecast, "sm1_mean",
                        value=f_sm1_mean, region=region, month=month,
                        metric="sm1_mean", scale=1.0, omit_value=True,
                    )
            elif disaster_type == "drought":
                self._outlook_put(
                    forecast, "precipitation_7d_sum_mm",
                    value=f_precip7, region=region, month=month,
                    metric="precipitation_sum_mm", scale=7.0, unit="mm",
                )
                if f_sm1_mean is not None:
                    self._outlook_put(
                        forecast, "sm1_mean",
                        value=f_sm1_mean, region=region, month=month,
                        metric="sm1_mean", scale=1.0, omit_value=True,
                    )
            elif disaster_type in ("flood", "landslide"):
                self._outlook_put(
                    forecast, "precipitation_7d_sum_mm",
                    value=f_precip7, region=region, month=month,
                    metric="precipitation_sum_mm", scale=7.0, unit="mm",
                )
                if f_rd_sum is not None:
                    self._outlook_put(
                        forecast, "river_discharge_7d_sum_m3s",
                        value=f_rd_sum, region=region, month=month,
                        metric="river_discharge", scale=7.0, omit_value=True,
                    )

        return recent, forecast

    def _predict_ml_with_optional_rolling(
        self,
        predictor: ModelPredictor,
        disaster_type: str,
        region: str,
        date: pd.Timestamp,
        preloaded_weather: Optional[pd.DataFrame] = None,
        precalculated_features: Optional[pd.DataFrame] = None
    ) -> Tuple[int, Dict]:
        """Single-day predict, or rolling-mean proba + thresholds for flood/landslide."""
        if precalculated_features is not None and not precalculated_features.empty:
            features = precalculated_features
        else:
            start_date = (date - pd.Timedelta(days=32)).strftime("%Y-%m-%d")
            end_date = date.strftime("%Y-%m-%d")
            features = predictor.prepare_features(
                [region], start_date, end_date, dataset_id="daily_ingestion",
                preloaded_data=preloaded_weather
            )
        
        if features.empty:
            return 0, {"error": "No features available for date"}
            
        features["date"] = pd.to_datetime(features["date"])
        features = features.sort_values("date").reset_index(drop=True)

        use_rolling = (
            disaster_type in ML_ROLLING_PROBA_HAZARDS
            and bool(predictor.class_thresholds)
        )
        extra: Dict = {}

        if use_rolling:
            proba_df = predictor.predict(features, return_proba=True)
            t2 = float(predictor.class_thresholds.get("level_2", 0.5))
            t3 = float(predictor.class_thresholds.get("level_3", 0.5))
            w = ML_ROLLING_WINDOW_DAYS
            if disaster_type == "landslide" and getattr(predictor, "n_classes", 4) == 3:
                sm2 = proba_df["prob_level_2"].rolling(window=w, min_periods=1).mean()
                last2 = float(sm2.iloc[-1])
                extra["rolling_prob_severe_mean"] = last2
                t3_ui = float(getattr(predictor, "landslide_ui_l3_threshold", 0.70))
                if last2 >= t3_ui:
                    ml = 3
                elif last2 >= t2:
                    ml = 2
                else:
                    ml = int(np.argmax(proba_df.iloc[-1].values))
            else:
                sm3 = (
                    proba_df["prob_level_3"].rolling(window=w, min_periods=1).mean()
                    if "prob_level_3" in proba_df.columns
                    else None
                )
                sm2 = (
                    proba_df["prob_level_2"].rolling(window=w, min_periods=1).mean()
                    if "prob_level_2" in proba_df.columns
                    else None
                )
                last3 = float(sm3.iloc[-1]) if sm3 is not None else 0.0
                last2 = float(sm2.iloc[-1]) if sm2 is not None else 0.0
                extra["rolling_prob_level_3_mean"] = last3
                extra["rolling_prob_level_2_mean"] = last2
                if last3 >= t3:
                    ml = 3
                elif last2 >= t2:
                    ml = 2
                else:
                    ml = int(np.argmax(proba_df.iloc[-1].values))
            extra["ml_prediction_mode"] = "rolling_proba_threshold"
            return int(ml), extra

        date_features = features[features["date"] == date]
        if date_features.empty:
            date_features = features.tail(1)
        ml_prediction = int(predictor.predict(date_features).iloc[0])
        extra["ml_prediction_mode"] = "single_day"
        return ml_prediction, extra

    def _assess_risk_inner(
        self,
        disaster_type: str,
        region: str,
        date: pd.Timestamp,
        preloaded_weather: Optional[pd.DataFrame] = None,
        preloaded_forecast: Optional[pd.DataFrame] = None,
        precalculated_features: Optional[pd.DataFrame] = None
    ) -> Tuple[int, Dict]:
        """Inner implementation of assess_risk (called within warning suppression)."""
        # Step 1: Get ML prediction
        try:
            predictor = self._get_predictor(disaster_type)
            ml_prediction, ml_extra = self._predict_ml_with_optional_rolling(
                predictor, disaster_type, region, date,
                preloaded_weather=preloaded_weather,
                precalculated_features=precalculated_features
            )
            if "error" in ml_extra:
                return 0, ml_extra
        except Exception as e:
            return 0, {'error': str(e)}
        
        # Step 2: Load recent weather (32 days for ML features and outlooks)
        if preloaded_weather is not None and not preloaded_weather.empty:
            weather_df = preloaded_weather[preloaded_weather['region'] == region].copy()
        else:
            weather_df = self._load_recent_weather(region, date, days_back=32)
        
        # Step 3: Load forecast (optional)
        if preloaded_forecast is not None and not preloaded_forecast.empty:
            forecast_df = preloaded_forecast[preloaded_forecast['region'] == region].copy()
        else:
            include_flood = disaster_type in ['flood', 'landslide']
            forecast_df = self._load_forecast(region, date, days_ahead=7, include_flood=include_flood)
        
        # Step 4: Final risk equals ML prediction
        has_forecast = isinstance(forecast_df, pd.DataFrame) and not forecast_df.empty

        final_risk = ml_prediction

        recent_outlook: Dict = {}
        forecast_outlook: Dict = {}
        if final_risk >= 1:
            recent_outlook, forecast_outlook = self._build_weather_outlooks(
                disaster_type=disaster_type,
                region=region,
                date=date,
                weather_df=weather_df,
                forecast_df=forecast_df if has_forecast else None,
            )

        assessment_details = {
            'ml_prediction': ml_prediction,
            'final_risk': final_risk,
            'has_forecast': has_forecast,
            'recent_outlook': recent_outlook,
            'forecast_outlook': forecast_outlook,
        }
        if "ml_prediction_mode" in ml_extra:
            assessment_details["ml_prediction_mode"] = ml_extra["ml_prediction_mode"]
        for k in (
            "rolling_prob_level_2_mean",
            "rolling_prob_level_3_mean",
            "rolling_prob_severe_mean",
        ):
            if k in ml_extra:
                assessment_details[k] = ml_extra[k]
        
        return final_risk, assessment_details

    def assess_risk(
        self,
        disaster_type: str,
        region: str,
        date: pd.Timestamp,
        preloaded_weather: Optional[pd.DataFrame] = None,
        preloaded_forecast: Optional[pd.DataFrame] = None,
        precalculated_features: Optional[pd.DataFrame] = None
    ) -> Tuple[int, Dict]:
        """
        Assess risk for a disaster type, region, and date.
        
        This is the main entry point that:
        1. Gets ML prediction
        2. Loads recent weather and forecasts (for outlook text when risk >= 1)
        3. Returns final risk level (same as ML prediction)
        
        Returns:
            Tuple of (final_risk_level, assessment_details_dict)
        """
        # Suppress noisy warnings from pandas concat/merge and sklearn model loading
        # that occur during feature preparation and prediction
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
            )
            warnings.filterwarnings(
                "ignore",
                message="Trying to unpickle estimator .* from version .* when using version .*",
            )
            return self._assess_risk_inner(
                disaster_type, region, date, 
                preloaded_weather=preloaded_weather, 
                preloaded_forecast=preloaded_forecast,
                precalculated_features=precalculated_features
            )
