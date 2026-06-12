"""v5 derived payload blocks: sleep debt, circadian regularity, recovery response."""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from analytics.digest_features import is_hard_day, safe_float
from analytics.digest_v4 import circular_mean_hhmm, _time_to_seconds
from util.formatting import format_delta_minutes_pm, format_minutes_hm

_SLEEP_DEBT_WEIGHTS = (0.06, 0.09, 0.12, 0.15, 0.18, 0.20, 0.20)


def _signed_balance_hm(minutes: int) -> str:
    if minutes == 0:
        return "0m"
    sign = "+" if minutes > 0 else "-"
    return f"{sign}{format_minutes_hm(abs(minutes))}"


def _sleep_debt_status(balance_min: int) -> str:
    if balance_min >= 60:
        return "surplus"
    if balance_min > -60:
        return "on_track"
    if balance_min > -150:
        return "mild_deficit"
    return "notable_deficit"


def sleep_debt_should_surface(debt: Dict[str, Any]) -> bool:
    """Surface sleep debt only when the rolling balance warrants attention."""
    if not debt:
        return False
    status = debt.get("status")
    if status == "notable_deficit":
        return True
    if status == "mild_deficit":
        last_night = debt.get("last_night_vs_norm_min")
        return last_night is not None and last_night < -30
    return False


def compute_sleep_debt(sleep_df: pd.DataFrame, target: date, typical_duration_min: Optional[int]) -> Dict[str, Any]:
    """Net 7d sleep balance (surplus credits deficit). Status uses recency-weighted balance."""
    empty = {
        "balance_7d_min": 0,
        "balance_7d_hm": "0m",
        "status": "on_track",
        "last_night_vs_norm_min": 0,
        "nights_short_7d": 0,
    }
    if sleep_df is None or sleep_df.empty or not typical_duration_min:
        return empty
    df = sleep_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["sleep_minutes"] = pd.to_numeric(df["sleep_minutes"], errors="coerce")
    df = df[df["sleep_start"].notna() & (df["sleep_minutes"] > 0)]

    start = target - timedelta(days=7)
    win = df[(df["date"] >= start) & (df["date"] < target)].sort_values("date")
    if win.empty:
        return empty

    deltas = []
    for _, row in win.iterrows():
        actual = safe_float(row["sleep_minutes"])
        if actual is None:
            continue
        deltas.append(int(round(actual - typical_duration_min)))

    if not deltas:
        return empty

    balance_7d = int(sum(deltas))
    nights_short = int(sum(1 for d in deltas if d < 0))

    weighted_balance = balance_7d
    if len(deltas) == 7:
        weighted = sum(w * d for w, d in zip(_SLEEP_DEBT_WEIGHTS, deltas))
        weighted_balance = int(round(weighted / sum(_SLEEP_DEBT_WEIGHTS)))

    last_night_row = win[win["date"] == (target - timedelta(days=1))]
    last_night_vs_norm = 0
    if not last_night_row.empty:
        last_actual = safe_float(last_night_row.iloc[-1]["sleep_minutes"])
        if last_actual is not None:
            last_night_vs_norm = int(round(last_actual - typical_duration_min))

    return {
        "balance_7d_min": balance_7d,
        "balance_7d_hm": _signed_balance_hm(balance_7d),
        "status": _sleep_debt_status(weighted_balance),
        "last_night_vs_norm_min": last_night_vs_norm,
        "nights_short_7d": nights_short,
    }


def _circular_stdev_minutes(times: List[Any]) -> Optional[float]:
    secs = [_time_to_seconds(t) for t in times]
    secs = [s for s in secs if s is not None and not (isinstance(s, float) and math.isnan(s))]
    if len(secs) < 3:
        return None
    angles = np.array(secs) * (2 * math.pi / 86400.0)
    sin_m = np.mean(np.sin(angles))
    cos_m = np.mean(np.cos(angles))
    R = math.sqrt(sin_m ** 2 + cos_m ** 2)
    if R <= 0:
        return None
    circ_std_rad = math.sqrt(-2 * math.log(R))
    return circ_std_rad * 86400.0 / (2 * math.pi)


def compute_circadian(sleep_df: pd.DataFrame, target: date) -> Dict[str, Any]:
    empty = {
        "bedtime_stdev_min": None,
        "wake_stdev_min": None,
        "regularity_index": None,
        "best_bedtime": None,
    }
    if sleep_df is None or sleep_df.empty:
        return empty
    df = sleep_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    start = target - timedelta(days=14)
    win = df[(df["date"] >= start) & (df["date"] < target)]
    win = win[win["sleep_start"].notna() & (pd.to_numeric(win["sleep_minutes"], errors="coerce") > 0)]
    if len(win) < 3:
        return empty

    bt_stdev = _circular_stdev_minutes(win["sleep_start"].tolist())
    wake_stdev = _circular_stdev_minutes(win["sleep_end"].tolist())
    bt_min = int(round(bt_stdev / 60)) if bt_stdev is not None else None
    wake_min = int(round(wake_stdev / 60)) if wake_stdev is not None else None

    deep = pd.to_numeric(win["deep_minutes"], errors="coerce")
    if deep.notna().sum() >= 3:
        idx = deep.idxmax()
        best_row = win.loc[idx]
        best_bt = circular_mean_hhmm([best_row["sleep_start"]])
    else:
        best_bt = circular_mean_hhmm(win["sleep_start"].tolist())

    regularity = None
    if bt_min is not None:
        regularity = max(0, min(100, int(round(100 - bt_min * 1.2))))

    return {
        "bedtime_stdev_min": bt_min,
        "wake_stdev_min": wake_min,
        "regularity_index": regularity,
        "best_bedtime": best_bt,
    }


def compute_recovery_response(
    activities_df: pd.DataFrame,
    hr_df: pd.DataFrame,
    target: date,
    yesterday_trained: bool,
    yesterday_hard: bool,
) -> Dict[str, Any]:
    out = {
        "rhr_recovery_days": None,
        "hrv_recovery_days": None,
        "status_today": "no recent hard session",
    }
    if activities_df is None or activities_df.empty or hr_df is None or hr_df.empty:
        if yesterday_hard:
            out["status_today"] = "day +1 after a hard session"
        elif yesterday_trained:
            out["status_today"] = "day +1 after training"
        return out

    hr = hr_df.copy()
    hr["date"] = pd.to_datetime(hr["date"]).dt.date
    hr["rhr"] = pd.to_numeric(hr["rhr"], errors="coerce")
    base_rhr = float(hr["rhr"].dropna().tail(30).mean()) if not hr["rhr"].dropna().empty else None

    act = activities_df.copy()
    if "local_date" not in act.columns and "start_date_local" in act.columns:
        act["local_date"] = pd.to_datetime(act["start_date_local"]).dt.date
    elif "local_date" in act.columns:
        act["local_date"] = pd.to_datetime(act["local_date"]).dt.date

    act["tss"] = pd.to_numeric(act.get("tss_proxy", 0), errors="coerce").fillna(0)
    act["suffer"] = pd.to_numeric(act.get("suffer_score", 0), errors="coerce")
    hard_days = []
    for d, grp in act.groupby("local_date"):
        tss = float(grp["tss"].sum())
        suffer = float(grp["suffer"].max()) if grp["suffer"].notna().any() else 0
        if is_hard_day(tss, suffer, 0):
            hard_days.append(d)

    best_lag = None
    best_score = 0.0
    if base_rhr is not None and len(hard_days) >= 3:
        for lag in (1, 2):
            bumps = []
            for hd in hard_days:
                check = hd + timedelta(days=lag)
                row = hr[hr["date"] == check]
                if row.empty:
                    continue
                rhr = safe_float(row.iloc[0]["rhr"])
                if rhr is not None:
                    bumps.append(rhr - base_rhr)
            if len(bumps) >= 3:
                mean_bump = float(np.mean(bumps))
                if abs(mean_bump) > abs(best_score):
                    best_score = mean_bump
                    best_lag = lag
    if best_lag and best_score > 1.5:
        out["rhr_recovery_days"] = best_lag

    if yesterday_hard:
        lag = out.get("rhr_recovery_days") or 1
        if lag == 2:
            out["status_today"] = "day +1 after a hard ride — watch tomorrow morning"
        else:
            out["status_today"] = "day +1 after a hard session"
    elif yesterday_trained:
        out["status_today"] = "day +1 after training"
    return out


def _z_vs_distribution(value: Optional[float], series: pd.Series) -> Optional[float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if value is None or len(s) < 5:
        return None
    mu = float(s.mean())
    std = float(s.std())
    if std == 0 or pd.isna(std):
        return None
    return (float(value) - mu) / std


def compute_sleep_quality_index(
    deep_min: Optional[float],
    rem_min: Optional[float],
    awake_min: Optional[float],
    sleep_min: Optional[float],
    baselines: Dict[str, Optional[float]],
) -> Optional[float]:
    """Personal z-composite sleep quality (~0 = typical night)."""
    if sleep_min is None or sleep_min <= 0:
        return None
    awake = awake_min or 0.0
    total = sleep_min + awake
    if total <= 0:
        return None
    efficiency = sleep_min / total

    def _z(val, mean, std):
        if val is None or mean is None or std is None or std == 0:
            return None
        return (val - mean) / std

    z_deep = _z(deep_min, baselines.get("deep_mean"), baselines.get("deep_std"))
    z_rem = _z(rem_min, baselines.get("rem_mean"), baselines.get("rem_std"))
    z_eff = _z(efficiency, baselines.get("eff_mean"), baselines.get("eff_std"))
    parts = [(0.40, z_deep), (0.35, z_rem), (0.25, z_eff)]
    avail = [(w, z) for w, z in parts if z is not None]
    if not avail:
        return None
    wsum = sum(w for w, _ in avail)
    return round(sum(w * z for w, z in avail) / wsum, 3)


def add_sleep_quality_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    needed = {"deep_sleep_minutes", "rem_minutes", "awake_minutes", "sleep_minutes"}
    if not needed.issubset(out.columns):
        out["sleep_quality_index"] = None
        return out
    deep = pd.to_numeric(out["deep_sleep_minutes"], errors="coerce")
    rem = pd.to_numeric(out["rem_minutes"], errors="coerce")
    awake = pd.to_numeric(out["awake_minutes"], errors="coerce").fillna(0)
    sleep = pd.to_numeric(out["sleep_minutes"], errors="coerce")
    eff = sleep / (sleep + awake).replace(0, pd.NA)
    deep_s, rem_s, eff_s = deep.dropna(), rem.dropna(), eff.dropna()
    baselines = {
        "deep_mean": float(deep_s.mean()) if not deep_s.empty else None,
        "deep_std": float(deep_s.std() or 1) if not deep_s.empty else None,
        "rem_mean": float(rem_s.mean()) if not rem_s.empty else None,
        "rem_std": float(rem_s.std() or 1) if not rem_s.empty else None,
        "eff_mean": float(eff_s.mean()) if not eff_s.empty else None,
        "eff_std": float(eff_s.std() or 1) if not eff_s.empty else None,
    }
    indices = []
    for i in range(len(out)):
        indices.append(
            compute_sleep_quality_index(
                safe_float(deep.iloc[i]),
                safe_float(rem.iloc[i]),
                safe_float(awake.iloc[i]),
                safe_float(sleep.iloc[i]),
                baselines,
            )
        )
    out["sleep_quality_index"] = indices
    return out


def proxy_zscore_magnitude(z: Optional[float]) -> Optional[str]:
    if z is None:
        return None
    az = abs(z)
    if az < 0.5:
        return "noise"
    if az < 1.0:
        return "mild"
    if az < 2.0:
        return "significant"
    return "strong"


def enrich_hrv_proxy_block(block: Dict[str, Any], history: pd.Series) -> Dict[str, Any]:
    """Add percentile_30d and z-based magnitude; omit raw index from LLM-facing block."""
    if not isinstance(block, dict):
        return block
    raw_today = block.get("today")
    s = pd.to_numeric(history, errors="coerce").dropna()
    out = dict(block)
    if raw_today is not None and len(s) >= 5:
        z = _z_vs_distribution(raw_today, s)
        out["percentile_30d"] = round(float((s <= raw_today).mean() * 100), 1)
        out["direction"] = (
            "positive" if z is not None and z > 0.15
            else "negative" if z is not None and z < -0.15
            else "neutral"
        )
        out["magnitude"] = proxy_zscore_magnitude(z)
        out["confidence"] = "medium"
        out["source"] = "nocturnal_proxy"
    for k in (
        "today", "delta", "typical_range", "range_window",
        "is_max_7d", "is_min_7d", "unit", "today_hm", "baseline_hm", "delta_pm",
    ):
        out.pop(k, None)
    return out


def proxy_trend_z_7d(history: pd.Series) -> Optional[float]:
    s = pd.to_numeric(history, errors="coerce").dropna()
    if len(s) < 14:
        return None
    mu = float(s.mean())
    std = float(s.std())
    if std == 0:
        return None
    return (float(s.tail(7).mean()) - mu) / std
