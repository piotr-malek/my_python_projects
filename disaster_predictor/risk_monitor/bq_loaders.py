"""Cached BigQuery loaders for the risk monitor."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

from risk_monitor.config import (
    CACHE_TTL_SEC,
    HAZARDS,
    INGESTION_DATASET,
    PROJECT_ID,
    REGIONS_DATASET,
    REGIONS_TABLE,
    RISK_DATASET,
)

_ROOT = Path(__file__).resolve().parent.parent
import sys

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.bq_utils import load_from_bigquery


def _project_id() -> str:
    if PROJECT_ID:
        return PROJECT_ID
    with open(_ROOT / "config" / "service_account.json") as f:
        return json.load(f)["project_id"]


def _sql_str(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_regions() -> pd.DataFrame:
    pid = _project_id()
    base = f"""
    SELECT
      region,
      parent_region,
      centroid_lat,
      centroid_lon,
      lon_min,
      lat_min,
      lon_max,
      lat_max
    FROM `{pid}.{REGIONS_DATASET}.{REGIONS_TABLE}`
    ORDER BY region
    """
    ext = f"""
    SELECT
      region,
      parent_region,
      country,
      centroid_lat,
      centroid_lon,
      lon_min,
      lat_min,
      lon_max,
      lat_max
    FROM `{pid}.{REGIONS_DATASET}.{REGIONS_TABLE}`
    ORDER BY region
    """
    try:
        df = load_from_bigquery(ext, project_id=pid)
    except Exception:
        df = load_from_bigquery(base, project_id=pid)
    if df is None or df.empty:
        return pd.DataFrame()
    if "country" in df.columns:
        df["country"] = df["country"].fillna(df["parent_region"])
    else:
        df["country"] = df["parent_region"]
    m = df["centroid_lat"].isna() | df["centroid_lon"].isna()
    if m.any():
        df.loc[m, "centroid_lat"] = (df.loc[m, "lat_min"] + df.loc[m, "lat_max"]) / 2.0
        df.loc[m, "centroid_lon"] = (df.loc[m, "lon_min"] + df.loc[m, "lon_max"]) / 2.0
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_latest_evaluation_date() -> Optional[pd.Timestamp]:
    pid = _project_id()
    q = f"SELECT MAX(date) AS d FROM `{pid}.{RISK_DATASET}.daily_evaluation`"
    df = load_from_bigquery(q, project_id=pid)
    if df is None or df.empty or pd.isna(df.iloc[0]["d"]):
        return None
    return pd.to_datetime(df.iloc[0]["d"])


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_evaluations_for_date(eval_date: str) -> pd.DataFrame:
    pid = _project_id()
    q = f"""
    SELECT
      assessment_id,
      date,
      region,
      disaster_type,
      ml_prediction,
      risk_score,
      risk_level,
      recent_outlook,
      forecast_outlook,
      rolling_diagnostics
    FROM `{pid}.{RISK_DATASET}.daily_evaluation`
    WHERE date = DATE('{eval_date}')
    """
    try:
        df = load_from_bigquery(q, project_id=pid)
    except Exception:
        q_fallback = q.replace(",\n      rolling_diagnostics", "")
        df = load_from_bigquery(q_fallback, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_evaluation_history(region: str, start: str, end: str) -> pd.DataFrame:
    pid = _project_id()
    r = _sql_str(region)
    q = f"""
    SELECT date, region, disaster_type, risk_score, risk_level,
           recent_outlook, forecast_outlook, assessment_id, rolling_diagnostics
    FROM `{pid}.{RISK_DATASET}.daily_evaluation`
    WHERE region = '{r}'
      AND date >= DATE('{start}')
      AND date <= DATE('{end}')
    ORDER BY date ASC, disaster_type ASC
    """
    try:
        df = load_from_bigquery(q, project_id=pid)
    except Exception:
        q2 = q.replace(", rolling_diagnostics", "")
        df = load_from_bigquery(q2, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_weather_outlook_for_assessments(assessment_ids: tuple) -> pd.DataFrame:
    if not assessment_ids:
        return pd.DataFrame()
    pid = _project_id()
    inner = ", ".join(f"'{_sql_str(aid)}'" for aid in assessment_ids)
    q = f"""
    SELECT assessment_id, recent_weather_interpretation, forecast_weather_interpretation,
           region, disaster_type, date, risk_level
    FROM `{pid}.{RISK_DATASET}.weather_outlook`
    WHERE assessment_id IN ({inner})
    """
    df = load_from_bigquery(q, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_change_log(region: str, ref_date: str, hazard: str = "all") -> pd.DataFrame:
    pid = _project_id()
    r = _sql_str(region)
    haz_clause = ""
    if hazard and hazard != "all":
        haz_clause = f" AND disaster_type = '{_sql_str(hazard)}'"
    q = f"""
    WITH ref AS (
      SELECT MAX(date) AS d FROM `{pid}.{RISK_DATASET}.daily_evaluation`
    ),
    ev AS (
      SELECT
        e.date,
        e.region,
        e.disaster_type,
        e.risk_score,
        LAG(e.risk_score) OVER (
          PARTITION BY e.region, e.disaster_type ORDER BY e.date
        ) AS prev_score
      FROM `{pid}.{RISK_DATASET}.daily_evaluation` e, ref
      WHERE e.region = '{r}'
        {haz_clause}
        AND e.date >= DATE_SUB((SELECT d FROM ref), INTERVAL 14 DAY)
    )
    SELECT date, region, disaster_type, risk_score, prev_score
    FROM ev
    WHERE prev_score IS NOT NULL AND risk_score != prev_score
    ORDER BY date DESC
    LIMIT 8
    """
    df = load_from_bigquery(q, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_era5_window(region: str, start: str, end: str) -> pd.DataFrame:
    pid = _project_id()
    r = _sql_str(region)
    q = f"""
    SELECT date, region,
           temp_2m_mean_C, precipitation_sum_mm, sm1_mean, sm2_mean
    FROM `{pid}.{INGESTION_DATASET}.era5`
    WHERE region = '{r}'
      AND date >= '{start}' AND date <= '{end}'
    ORDER BY date ASC
    """
    df = load_from_bigquery(q, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_openmeteo_window(region: str, start: str, end: str) -> pd.DataFrame:
    pid = _project_id()
    r = _sql_str(region)
    q = f"""
    SELECT date, region_name AS region, river_discharge
    FROM `{pid}.{INGESTION_DATASET}.openmeteo_weather`
    WHERE region_name = '{r}'
      AND date >= '{start}' AND date <= '{end}'
    ORDER BY date ASC
    """
    df = load_from_bigquery(q, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=CACHE_TTL_SEC)
def load_forecast_weather(region: str, anchor: str) -> pd.DataFrame:
    """Next 7 days after anchor from openmeteo_forecast (weather only, not ML risk)."""
    pid = _project_id()
    r = _sql_str(region)
    end = pd.to_datetime(anchor) + pd.Timedelta(days=8)
    q = f"""
    SELECT date, region_name AS region,
           temperature_2m_max, precipitation_sum, sm1_mean, sm2_mean, river_discharge
    FROM `{pid}.{INGESTION_DATASET}.openmeteo_forecast`
    WHERE region_name = '{r}'
      AND date > DATE('{anchor}')
      AND date <= DATE('{end.isoformat()[:10]}')
    ORDER BY date ASC
    """
    try:
        df = load_from_bigquery(q, project_id=pid)
    except Exception:
        q2 = q.replace("sm1_mean, sm2_mean, river_discharge", "precipitation_sum")
        df = load_from_bigquery(q2, project_id=pid)
    if df is None:
        return pd.DataFrame()
    return df


def clear_loader_cache() -> None:
    load_regions.clear()
    load_latest_evaluation_date.clear()
    load_evaluations_for_date.clear()
    load_evaluation_history.clear()
    load_weather_outlook_for_assessments.clear()
    load_change_log.clear()
    load_era5_window.clear()
    load_openmeteo_window.clear()
    load_forecast_weather.clear()
