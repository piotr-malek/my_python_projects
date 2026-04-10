"""
ML-based daily risk assessment for all disaster types.

This module replaces the old rule-based system with ML model predictions.
Integrates with the existing Airflow DAG structure.
"""

import os
import sys
import json
import datetime
import uuid
import concurrent.futures
from pathlib import Path
from typing import Optional, Dict, Tuple, List
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
import pandas as pd
import numpy as np

# Suppress noisy pandas warnings
pd.options.mode.chained_assignment = None
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', message='.*SettingWithCopyWarning.*')

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.bq_utils import save_to_bigquery, load_from_bigquery
from utils.earth_engine_utils import REGION_NAMES
from risk_assessment.ml_detection import MLDisasterDetection
from config import get_region_name

PROJECT_ID = os.getenv("PROJECT_ID")
# Risk data is stored in risk_assessment dataset (not region-specific)
RISK_DATASET = "risk_assessment"

# Risk level mapping
RISK_LEVEL_MAP = {0: "no_risk", 1: "low", 2: "medium", 3: "high"}

ROLLING_DIAGNOSTIC_KEYS = (
    "rolling_prob_level_2_mean",
    "rolling_prob_level_3_mean",
    "rolling_prob_severe_mean",
    "ml_prediction_mode",
)

# Whitelist for contributing_metrics / outlook snapshots (bounded JSON)
OUTLOOK_METRIC_KEYS = frozenset(
    {
        "precipitation_7d_sum_mm",
        "temperature_7d_max_C",
        "sm1_mean",
        "precipitation_30d_sum_mm",
        "sm1_mean_14d",
        "river_discharge_7d_sum_m3s",
    }
)

CONTRIBUTING_METRICS_MAX_JSON = 12_000


def convert_numpy_types(obj):
    """Recursively convert NumPy types to native Python types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_numpy_types(item) for item in obj]
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, datetime.datetime)):
        return obj.isoformat()
    return obj


def get_previous_assessment_data(
    project_id: str, 
    dataset_id: str, 
    region: str, 
    disaster_type: str, 
    date: pd.Timestamp
):
    """Get the most recent assessment data for comparison."""
    try:
        query = f"""
        SELECT risk_score, risk_level, date as prev_date
        FROM `{project_id}.{dataset_id}.daily_evaluation`
        WHERE region = '{region}'
          AND disaster_type = '{disaster_type}'
        ORDER BY created_at DESC
        LIMIT 1
        """
        df = load_from_bigquery(query)
        
        if df is not None and not df.empty and len(df) > 0:
            row = df.iloc[0]
            return {
                'risk_score': int(row['risk_score']) if pd.notna(row['risk_score']) else None,
                'risk_level': row['risk_level'] if pd.notna(row['risk_level']) else None,
                'prev_date': row['prev_date']
            }
    except NotFound:
        return None
    except Exception:
        return None
    return None


def has_assessment_for_date(
    project_id: str,
    dataset_id: str,
    region: str,
    disaster_type: str,
    date: datetime.date,
):
    """
    Check whether an assessment already exists for the given
    (date, region, disaster_type) combination.
    """
    try:
        date_str = date.isoformat()
        query = f"""
        SELECT 1
        FROM `{project_id}.{dataset_id}.daily_evaluation`
        WHERE region = '{region}'
          AND disaster_type = '{disaster_type}'
          AND date = DATE('{date_str}')
        LIMIT 1
        """
        df = load_from_bigquery(query)
        return df is not None and not df.empty
    except NotFound:
        return False
    except Exception:
        return False


def get_regions_already_assessed_for_date(
    project_id: str,
    dataset_id: str,
    assessment_date: datetime.date,
    disaster_type: str,
    regions_subset: Optional[List[str]] = None,
) -> set:
    """
    Regions that already have a row in daily_evaluation for this calendar date
    and hazard. Used to skip re-runs (same UUID would not be duplicated, but
    WRITE_APPEND would still insert duplicate region+date+hazard rows).

    Uniqueness is (date, region, disaster_type); one row per hazard per region per day.
    """
    date_str = assessment_date.isoformat()
    try:
        in_clause = ""
        if regions_subset is not None:
            esc = "', '".join(str(r).replace("'", "''") for r in regions_subset)
            in_clause = f" AND region IN ('{esc}')"
        query = f"""
        SELECT DISTINCT region
        FROM `{project_id}.{dataset_id}.daily_evaluation`
        WHERE date = DATE('{date_str}')
          AND disaster_type = '{disaster_type}'
          {in_clause}
        """
        df = load_from_bigquery(query, project_id=project_id)
        if df is None or df.empty:
            return set()
        return set(df["region"].astype(str).tolist())
    except NotFound:
        return set()
    except Exception as e:
        print(f"Warning: could not load existing assessments for {assessment_date} / {disaster_type}: {e}")
        return set()


def _extract_rolling_diagnostics(assessment_details: dict) -> Dict:
    out: Dict = {}
    for k in ROLLING_DIAGNOSTIC_KEYS:
        if k in assessment_details and assessment_details[k] is not None:
            out[k] = assessment_details[k]
    return out


def _outlook_metric_compact(entry) -> Dict:
    if not isinstance(entry, dict):
        return {}
    d: Dict = {}
    if "percentile_approx" in entry:
        d["percentile_approx"] = entry["percentile_approx"]
    if "value" in entry:
        d["value"] = entry["value"]
    if "unit" in entry:
        d["unit"] = entry["unit"]
    return d


def _snapshot_outlook_metrics(outlook: dict, max_keys: int = 8) -> Dict:
    if not outlook:
        return {}
    snap: Dict = {}
    for key in outlook:
        if key not in OUTLOOK_METRIC_KEYS:
            continue
        snap[key] = _outlook_metric_compact(outlook.get(key))
        if len(snap) >= max_keys:
            break
    return snap


def _outlook_percentile_deltas(prev_outlook: dict, curr_outlook: dict) -> Dict:
    deltas: Dict = {}
    if not prev_outlook or not curr_outlook:
        return deltas
    for key in OUTLOOK_METRIC_KEYS:
        pe = prev_outlook.get(key)
        ce = curr_outlook.get(key)
        if isinstance(pe, dict) and isinstance(ce, dict):
            pp = pe.get("percentile_approx")
            cp = ce.get("percentile_approx")
            if pp is not None and cp is not None:
                deltas[key] = round(float(cp) - float(pp), 2)
    return deltas


def _build_contributing_metrics(
    prev_risk_score: int,
    risk_score: int,
    ml_prediction,
    recent_outlook: dict,
    forecast_outlook: dict,
    prev_recent_outlook_raw,
    rolling_diag: Dict,
) -> str:
    if risk_score > prev_risk_score:
        direction = "up"
    elif risk_score < prev_risk_score:
        direction = "down"
    else:
        direction = "stable"
    prev_parsed: Dict = {}
    if prev_recent_outlook_raw is not None and not isinstance(
        prev_recent_outlook_raw, (str, dict)
    ):
        try:
            if pd.isna(prev_recent_outlook_raw):
                prev_recent_outlook_raw = None
        except (ValueError, TypeError):
            pass
    if isinstance(prev_recent_outlook_raw, str) and prev_recent_outlook_raw.strip() not in ("", "{}"):
        try:
            prev_parsed = json.loads(prev_recent_outlook_raw)
        except json.JSONDecodeError:
            prev_parsed = {}
    elif isinstance(prev_recent_outlook_raw, dict):
        prev_parsed = prev_recent_outlook_raw
    metrics = {
        "ml_prediction": ml_prediction,
        "final_risk": risk_score,
        "previous_risk_score": int(prev_risk_score),
        "change_direction": direction,
        "recent_outlook_snapshot": _snapshot_outlook_metrics(recent_outlook or {}),
        "forecast_outlook_snapshot": _snapshot_outlook_metrics(forecast_outlook or {}),
        "outlook_percentile_deltas_vs_prior_eval": _outlook_percentile_deltas(
            prev_parsed, recent_outlook or {}
        ),
    }
    if rolling_diag:
        metrics["rolling_diagnostics"] = rolling_diag
    payload = convert_numpy_types(metrics)
    s = json.dumps(payload)
    if len(s) > CONTRIBUTING_METRICS_MAX_JSON:
        metrics.pop("outlook_percentile_deltas_vs_prior_eval", None)
        s = json.dumps(convert_numpy_types(metrics))
    return s


def get_previous_assessment_data_batched(project_id, dataset_id, disaster_type, assessment_date):
    """Fetch previous assessments for all regions in one query."""
    try:
        query = f"""
        WITH ranked_evals AS (
            SELECT 
                region, risk_score, risk_level, date, recent_outlook,
                ROW_NUMBER() OVER(PARTITION BY region ORDER BY created_at DESC) as rn
            FROM `{project_id}.{dataset_id}.daily_evaluation`
            WHERE disaster_type = '{disaster_type}'
              AND date < DATE('{assessment_date.isoformat()}')
        )
        SELECT region, risk_score, risk_level, recent_outlook
        FROM ranked_evals
        WHERE rn = 1
        """
        df = load_from_bigquery(query)
        if df is not None and not df.empty:
            return df.set_index('region').to_dict('index')
    except Exception as e:
        print(f"Warning: Could not fetch previous assessments in batch: {e}")
    return {}


def preload_all_weather(project_id, dataset_id, assessment_date, regions=None, days_back=32):
    """Fetch weather data for regions in one query."""
    start_date = (assessment_date - datetime.timedelta(days=days_back)).isoformat()
    end_date = assessment_date.isoformat()
    
    where_clause = ""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"AND region IN ('{regions_str}')"
        om_where_clause = f"AND region_name IN ('{regions_str}')"
    else:
        where_clause = ""
        om_where_clause = ""

    # 1. Load ERA5 weather
    era5_query = f"""
    SELECT 
        date, region, temp_2m_mean_C, precipitation_sum_mm, sm1_mean, sm2_mean
    FROM `{project_id}.{dataset_id}.era5`
    WHERE date >= '{start_date}' AND date <= '{end_date}' {where_clause}
    """
    
    # 2. Load River Discharge (from openmeteo_weather)
    discharge_query = f"""
    SELECT 
        date, region_name as region, river_discharge
    FROM `{project_id}.{dataset_id}.openmeteo_weather`
    WHERE date >= '{start_date}' AND date <= '{end_date}' {om_where_clause}
    """
    
    # 3. Load NDVI (Landsat)
    landsat_query = f"""
    SELECT 
        date, region, ndvi_mean
    FROM `{project_id}.{dataset_id}.landsat`
    WHERE date >= '{start_date}' AND date <= '{end_date}' {where_clause}
    """
    
    # 4. Load VIIRS Fire data
    viirs_query = f"""
    SELECT 
        date, region, hotspot_count, frp_mean
    FROM `{project_id}.{dataset_id}.viirs`
    WHERE date >= '{start_date}' AND date <= '{end_date}' {where_clause}
    """
    
    try:
        era5_df = load_from_bigquery(era5_query)
        if era5_df is None or era5_df.empty:
            return pd.DataFrame()
            
        era5_df['date'] = pd.to_datetime(era5_df['date'])
        
        # Merge River Discharge
        try:
            discharge_df = load_from_bigquery(discharge_query)
            if discharge_df is not None and not discharge_df.empty:
                discharge_df['date'] = pd.to_datetime(discharge_df['date'])
                # Deduplicate discharge data
                discharge_df = discharge_df.sort_values(['region', 'date']).drop_duplicates(subset=['region', 'date'], keep='last')
                era5_df = era5_df.merge(discharge_df, on=['date', 'region'], how='left')
        except Exception as e:
            print(f"Warning: Could not preload river discharge: {e}")
            era5_df['river_discharge'] = np.nan
            
        # Merge NDVI
        try:
            landsat_df = load_from_bigquery(landsat_query)
            if landsat_df is not None and not landsat_df.empty:
                landsat_df['date'] = pd.to_datetime(landsat_df['date'])
                # Deduplicate landsat data
                landsat_df = landsat_df.sort_values(['region', 'date']).drop_duplicates(subset=['region', 'date'], keep='last')
                era5_df = era5_df.merge(landsat_df, on=['date', 'region'], how='left')
                # burned_area_pct is optional and only exists in MODIS (historical)
                # Landsat doesn't have this column, so set to NULL for consistency with model expectations
                era5_df['burned_area_pct'] = np.nan
        except Exception as e:
            print(f"Warning: Could not preload NDVI: {e}")
            era5_df['ndvi_mean'] = np.nan
            era5_df['burned_area_pct'] = np.nan
            
        # Merge VIIRS
        try:
            viirs_df = load_from_bigquery(viirs_query)
            if viirs_df is not None and not viirs_df.empty:
                viirs_df['date'] = pd.to_datetime(viirs_df['date'])
                # Deduplicate viirs data
                viirs_df = viirs_df.sort_values(['region', 'date']).drop_duplicates(subset=['region', 'date'], keep='last')
                era5_df = era5_df.merge(viirs_df, on=['date', 'region'], how='left')
        except Exception as e:
            print(f"Warning: Could not preload VIIRS: {e}")
            era5_df['hotspot_count'] = 0
            era5_df['frp_mean'] = 0.0
            
        return era5_df
    except Exception as e:
        print(f"Warning: Could not preload weather data: {e}")
    return pd.DataFrame()


def preload_all_forecasts(project_id, dataset_id, assessment_date, regions=None, days_ahead=7):
    """Fetch forecast data for regions in one query."""
    d0 = assessment_date.isoformat()
    end_date = (assessment_date + datetime.timedelta(days=days_ahead)).isoformat()
    
    where_clause = ""
    if regions:
        regions_str = "', '".join(regions)
        where_clause = f"AND region_name IN ('{regions_str}')"

    base_query = f"""
    FROM `{project_id}.{dataset_id}.openmeteo_forecast`
    WHERE date > '{d0}' AND date <= '{end_date}' {where_clause}
    """
    
    extended_sql = f"SELECT date, region_name as region, temperature_2m_max, precipitation_sum, sm1_mean, sm2_mean, river_discharge {base_query}"
    minimal_sql = f"SELECT date, region_name as region, temperature_2m_max, precipitation_sum {base_query}"
    
    for query in [extended_sql, minimal_sql]:
        try:
            df = load_from_bigquery(query)
            if df is not None and not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                return df
        except Exception:
            continue
            
    return pd.DataFrame()


def _assess_single_region_hazard(args):
    (project_id, region, disaster_type, assessment_date, assessment_date_pd, 
     detection, prev_data, weather_df, forecast_df, precalculated_features) = args
    try:
        # Get ML-based risk assessment using preloaded data and precalculated features
        risk_score, assessment_details = detection.assess_risk(
            disaster_type=disaster_type,
            region=region,
            date=assessment_date_pd,
            preloaded_weather=weather_df,
            preloaded_forecast=forecast_df,
            precalculated_features=precalculated_features
        )
        
        if 'error' in assessment_details:
            return None, f"{region}/{disaster_type}: {assessment_details['error']}"
        
        risk_level = RISK_LEVEL_MAP[risk_score]
        
        # Use preloaded previous data
        prev_row = prev_data.get(region, {})
        prev_risk_level = prev_row.get('risk_level')
        prev_risk_score = prev_row.get('risk_score')
        
        risk_changed = (risk_level != prev_risk_level) if prev_risk_level else False

        ml_prediction = assessment_details.get('ml_prediction')

        recent_outlook = assessment_details.get('recent_outlook')
        forecast_outlook = assessment_details.get('forecast_outlook')
        rolling_diag = _extract_rolling_diagnostics(assessment_details)
        rolling_json = (
            json.dumps(convert_numpy_types(rolling_diag)) if rolling_diag else "{}"
        )

        assessment = {
            'assessment_id': str(uuid.uuid4()),
            'date': assessment_date,
            'region': region,
            'disaster_type': disaster_type,
            'ml_prediction': int(ml_prediction) if ml_prediction is not None else None,
            'risk_score': risk_score,
            'risk_level': risk_level,
            'risk_changed': risk_changed,
            'recent_outlook': json.dumps(convert_numpy_types(recent_outlook)) if recent_outlook else "{}",
            'forecast_outlook': json.dumps(convert_numpy_types(forecast_outlook)) if forecast_outlook else "{}",
            'rolling_diagnostics': rolling_json,
            'created_at': pd.Timestamp.now()
        }
        
        risk_change = None
        if risk_changed and prev_risk_score is not None:
            prev_recent_raw = prev_row.get("recent_outlook")
            metrics_str = _build_contributing_metrics(
                int(prev_risk_score),
                risk_score,
                ml_prediction,
                recent_outlook if isinstance(recent_outlook, dict) else {},
                forecast_outlook if isinstance(forecast_outlook, dict) else {},
                prev_recent_raw,
                rolling_diag,
            )
            risk_change = {
                'date': assessment_date,
                'region': region,
                'disaster_type': disaster_type,
                'risk_score': risk_score,
                'previous_risk_score': int(prev_risk_score),
                'contributing_metrics': metrics_str
            }
            
        return (assessment, risk_change), None
    except Exception as e:
        return None, f"{region}/{disaster_type}: {str(e)}"


def preload_all_static_data(project_id, regions):
    """Fetch terrain and descriptor data for all regions once."""
    from ml_training.data_preparation.load_training_data import load_terrain_data, load_region_descriptors
    print("Preloading static terrain and descriptor data...")
    terrain = load_terrain_data(regions=regions)
    desc = load_region_descriptors(regions=regions)
    return {'terrain': terrain, 'desc': desc}


def assess_daily_risks(
    project_id: str = None, 
    region_name: str = None,
    disaster_types: list = None,
    max_workers: int = 5,
    chunk_size: int = 50
):
    """
    Assess risks for all regions and disaster types using ML models.
    Parallelized and batched version with hazard-first chunking to save memory.
    """
    if project_id is None:
        project_id = PROJECT_ID
    
    if disaster_types is None:
        disaster_types = ['fire', 'drought', 'flood', 'landslide']
    
    # Risk data is stored in risk_assessment dataset (not region-specific)
    dataset_id = RISK_DATASET
    # Always assess yesterday (previous complete day)
    assessment_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    assessment_date_pd = pd.to_datetime(assessment_date)
    
    if region_name is None:
        region_name = get_region_name()

    detection = MLDisasterDetection(project_id=project_id, region_name=region_name)
    
    # Preload climatology data in main thread to avoid parallel loading logs
    detection.preload_climatology()
    
    all_regions = REGION_NAMES()
    
    # Preload static data once for all regions
    static_data = preload_all_static_data(project_id, all_regions)
    
    # Process each disaster type sequentially
    for d_type in disaster_types:
        print(f"\n=== Processing {d_type} risks ===")
        
        # Preload previous assessments for this hazard in ONE query
        prev_data_map = get_previous_assessment_data_batched(project_id, RISK_DATASET, d_type, assessment_date)
        
        # Process regions in chunks for this hazard
        for i in range(0, len(all_regions), chunk_size):
            regions_chunk = all_regions[i:i + chunk_size]
            already = get_regions_already_assessed_for_date(
                project_id, dataset_id, assessment_date, d_type, regions_chunk
            )
            regions_to_run = [r for r in regions_chunk if r not in already]
            if not regions_to_run:
                print(
                    f"--- {d_type} chunk {i // chunk_size + 1}: all {len(regions_chunk)} regions "
                    f"already have daily_evaluation for {assessment_date}; skip ---"
                )
                continue
            if already:
                print(
                    f"--- {d_type} chunk {i // chunk_size + 1}: skipping {len(already)} region(s) "
                    f"already assessed for {assessment_date}; running {len(regions_to_run)} ---"
                )
            else:
                print(f"--- {d_type} chunk {i // chunk_size + 1} ({len(regions_to_run)} regions) ---")

            # 1. Preload weather and forecast only for regions we will assess
            chunk_weather = preload_all_weather(project_id, "daily_ingestion", assessment_date, regions=regions_to_run)
            chunk_forecasts = preload_all_forecasts(project_id, "daily_ingestion", assessment_date, regions=regions_to_run)

            # 2. Batch feature engineering for the entire chunk
            # This is much more memory efficient than doing it in parallel threads
            predictor = detection._get_predictor(d_type)
            start_date_str = (assessment_date_pd - pd.Timedelta(days=32)).strftime("%Y-%m-%d")
            end_date_str = assessment_date_pd.strftime("%Y-%m-%d")

            print(f"Engineering features for chunk...")
            chunk_features = predictor.prepare_features(
                regions_to_run, start_date_str, end_date_str,
                dataset_id="daily_ingestion",
                preloaded_data=chunk_weather,
                preloaded_static=static_data
            )

            import gc
            gc.collect()

            tasks = []
            for region in regions_to_run:
                # Filter preloaded data for this region
                region_weather = chunk_weather[chunk_weather['region'] == region] if not chunk_weather.empty else None
                region_forecast = chunk_forecasts[chunk_forecasts['region'] == region] if not chunk_forecasts.empty else None
                region_features = chunk_features[chunk_features['region'] == region] if not chunk_features.empty else None
                
                tasks.append((
                    project_id, region, d_type, assessment_date, assessment_date_pd, 
                    detection, prev_data_map, region_weather, region_forecast, region_features
                ))
            
            print(f"Starting parallel prediction for {len(tasks)} regions using {max_workers} workers...")
            all_assessments = []
            risk_changes_list = []
            errors = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(_assess_single_region_hazard, tasks))
                
            for res, err in results:
                if err: errors.append(err)
                if res:
                    all_assessments.append(res[0])
                    if res[1]: risk_changes_list.append(res[1])
            
            # Save results for this chunk immediately
            if all_assessments:
                df = pd.DataFrame(all_assessments)
                save_to_bigquery(df, project_id, dataset_id, "daily_evaluation", mode="WRITE_APPEND")
                if risk_changes_list:
                    risk_changes_df = pd.DataFrame(risk_changes_list)
                    save_to_bigquery(risk_changes_df, project_id, dataset_id, "risk_changes", mode="WRITE_APPEND")
                print(f"✓ Saved {len(df)} assessments for chunk")

            # Explicitly clear chunk data
            del chunk_weather
            del chunk_forecasts
            del chunk_features
            del all_assessments
            del risk_changes_list
            gc.collect()
            
        # Clear hazard-specific data
        del prev_data_map
        gc.collect()

    print(f"\n=== Risk Assessment Complete ===")


if __name__ == "__main__":
    assess_daily_risks()
