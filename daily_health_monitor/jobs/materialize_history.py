"""Materialize wellness_daily_complete from backfilled raw_* tables (full-day values)."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from analytics.derived_blocks import add_sleep_quality_index, compute_sleep_quality_index
from analytics.digest_features import is_hard_day, safe_float
from analytics.stress_curve import analyze_stress_day
from config import settings


def _daily_tss_by_date(activities_df: pd.DataFrame) -> pd.DataFrame:
    if activities_df is None or activities_df.empty:
        return pd.DataFrame(columns=["date", "tss", "suffer_max"])
    act = activities_df.copy()
    if "local_date" in act.columns:
        act["date"] = pd.to_datetime(act["local_date"]).dt.date
    elif "start_date_local" in act.columns:
        act["date"] = pd.to_datetime(act["start_date_local"]).dt.date
    else:
        return pd.DataFrame(columns=["date", "tss", "suffer_max"])
    act["tss"] = pd.to_numeric(act.get("tss_proxy"), errors="coerce").fillna(0)
    act["suffer"] = pd.to_numeric(act.get("suffer_score"), errors="coerce")
    g = act.groupby("date").agg(tss=("tss", "sum"), suffer_max=("suffer", "max")).reset_index()
    return g


def materialize_day_row(
    d: date,
    hr_row,
    sleep_row,
    stress_row,
    act_row,
    bb_row,
    resp_row,
    hrv_row,
    tss_row,
    bands_config: dict,
    sleep_quality_baselines: dict,
) -> dict:
    sleep_min = safe_float(sleep_row.get("sleep_minutes")) if sleep_row is not None else None
    deep = safe_float(sleep_row.get("deep_minutes")) if sleep_row is not None else None
    rem = safe_float(sleep_row.get("rem_minutes")) if sleep_row is not None else None
    light = safe_float(sleep_row.get("light_minutes")) if sleep_row is not None else None
    awake = safe_float(sleep_row.get("awake_minutes")) if sleep_row is not None else None

    avg_stress = safe_float(stress_row.get("avg_stress")) if stress_row is not None else None
    curve = analyze_stress_day(
        stress_row.get("samples_json") if stress_row is not None else None,
        day_avg_stress=avg_stress,
        bands_config=bands_config,
    )

    bb_charged = safe_float(bb_row.get("charged")) if bb_row is not None else None
    bb_eff = None
    if bb_charged is not None and sleep_min and sleep_min > 0:
        bb_eff = bb_charged / (sleep_min / 60.0)

    sqi = compute_sleep_quality_index(deep, rem, awake, sleep_min, sleep_quality_baselines)

    tss = safe_float(tss_row.get("tss")) if tss_row is not None else 0.0
    suffer = safe_float(tss_row.get("suffer_max")) if tss_row is not None else None
    intensity = safe_float(act_row.get("intensity_minutes")) if act_row is not None else 0.0
    hard = is_hard_day(tss or 0, suffer, intensity or 0)

    bands = curve.get("bands") or {}
    return {
        "date": d.isoformat(),
        "rhr": safe_float(hr_row.get("rhr")) if hr_row is not None else None,
        "sleep_minutes": sleep_min,
        "deep_minutes": deep,
        "rem_minutes": rem,
        "light_minutes": light,
        "awake_minutes": awake,
        "sleep_start_local": sleep_row.get("sleep_start") if sleep_row is not None else None,
        "sleep_end_local": sleep_row.get("sleep_end") if sleep_row is not None else None,
        "waking_rr": safe_float(resp_row.get("waking_rr")) if resp_row is not None else None,
        "sleep_rr": safe_float(resp_row.get("sleep_rr")) if resp_row is not None else None,
        "avg_stress_fullday": avg_stress,
        "high_stress_pct_fullday": safe_float(stress_row.get("high_pct")) if stress_row is not None else None,
        "rest_pct_fullday": curve.get("rest_pct"),
        "stress_band_morning": bands.get("morning_06_09"),
        "stress_band_work": bands.get("work_09_18"),
        "stress_band_evening": bands.get("evening_18_22"),
        "stress_peak_window": curve.get("peak_window"),
        "stress_peak_value": curve.get("peak_value"),
        "stress_settled_after": curve.get("settled_after"),
        "high_stress_minutes": curve.get("high_stress_minutes"),
        "steps_fullday": int(act_row.get("steps")) if act_row is not None and pd.notna(act_row.get("steps")) else None,
        "intensity_minutes_prenoon": None,
        "bb_high": safe_float(bb_row.get("bb_high")) if bb_row is not None else None,
        "bb_low": safe_float(bb_row.get("bb_low")) if bb_row is not None else None,
        "bb_recharge_efficiency": bb_eff,
        "hrv_proxy_nocturnal": safe_float(hrv_row.get("nocturnal_proxy")) if hrv_row is not None else None,
        "sleep_quality_index": sqi,
        "is_hard_day": hard,
        "tss": tss,
    }


def build_wellness_daily_complete(
    wellness: dict,
    activities_df: pd.DataFrame,
    days: int,
    end: date | None = None,
    bands_config: dict | None = None,
) -> pd.DataFrame:
    end = end or date.today()
    start = end - timedelta(days=days - 1)
    bands_config = bands_config or settings.STRESS_TIME_BANDS

    def _index(df, col="date"):
        if df is None or df.empty:
            return {}
        x = df.copy()
        x["date"] = pd.to_datetime(x[col]).dt.date
        return {r["date"]: r for _, r in x.iterrows()}

    hr = _index(wellness.get("raw_heart_rate", pd.DataFrame()))
    sleep = _index(wellness.get("raw_sleep", pd.DataFrame()))
    stress = _index(wellness.get("raw_stress", pd.DataFrame()))
    act = _index(wellness.get("raw_activity_daily", pd.DataFrame()))
    bb = _index(wellness.get("raw_body_battery", pd.DataFrame()))
    resp = _index(wellness.get("raw_respiration", pd.DataFrame()))
    hrv = _index(wellness.get("raw_hrv", pd.DataFrame()))
    tss_by = _daily_tss_by_date(activities_df)
    tss = _index(tss_by)

    sleep_df = wellness.get("raw_sleep", pd.DataFrame())
    if not sleep_df.empty:
        sl = sleep_df.copy()
        if "deep_sleep_minutes" not in sl.columns and "deep_minutes" in sl.columns:
            sl["deep_sleep_minutes"] = sl["deep_minutes"]
        sl = add_sleep_quality_index(sl)
        deep = pd.to_numeric(sl.get("deep_sleep_minutes", sl.get("deep_minutes")), errors="coerce")
        rem = pd.to_numeric(sl.get("rem_minutes"), errors="coerce")
        awake = pd.to_numeric(sl.get("awake_minutes"), errors="coerce").fillna(0)
        sleep_m = pd.to_numeric(sl.get("sleep_minutes"), errors="coerce")
        eff = sleep_m / (sleep_m + awake).replace(0, pd.NA)
        baselines = {
            "deep_mean": float(deep.dropna().mean()) if deep.notna().any() else None,
            "deep_std": float(deep.dropna().std() or 1) if deep.notna().any() else None,
            "rem_mean": float(rem.dropna().mean()) if rem.notna().any() else None,
            "rem_std": float(rem.dropna().std() or 1) if rem.notna().any() else None,
            "eff_mean": float(eff.dropna().mean()) if eff.notna().any() else None,
            "eff_std": float(eff.dropna().std() or 1) if eff.notna().any() else None,
        }
    else:
        baselines = {}

    rows = []
    d = start
    while d <= end:
        rows.append(
            materialize_day_row(
                d,
                hr.get(d),
                sleep.get(d),
                stress.get(d),
                act.get(d),
                bb.get(d),
                resp.get(d),
                hrv.get(d),
                tss.get(d),
                bands_config,
                baselines,
            )
        )
        d += timedelta(days=1)
    return pd.DataFrame(rows)


def run_materialize_history(repo, target=None, window_days=None):
    target = target or date.today()
    window_days = window_days or settings.ANALYSIS_DAYS
    wellness = repo.load_wellness(window_days)
    activities = repo.load_activities_for_analysis(window_days)
    df = build_wellness_daily_complete(wellness, activities, window_days, end=target)
    if not df.empty:
        repo.save_wellness_daily_complete(df)
    return len(df)
