"""v4 digest payload enrichment: multi-window stats, directions, score trajectories, sleep_context."""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from analytics.digest_features import (
    ELEVATED_MAGNITUDES,
    MAGNITUDE_CLASSIFIERS,
    WELLNESS_FIELDS,
    safe_float,
)
from util.formatting import (
    format_delta_minutes_pm,
    format_minutes_hm,
    format_minutes_pair_hm,
    format_recharge_delta,
    format_recharge_rate,
)

WINDOW_14D = frozenset(
    {
        "sleep_minutes",
        "deep_sleep_minutes",
        "rem_minutes",
        "light_minutes",
        "awake_minutes",
        "sleep_score",
        "sleep_stress",
    }
)
WINDOW_30D = frozenset(
    {
        "rhr_bpm",
        "waking_rr_brpm",
        "sleep_rr",
        "hrv_rmssd_ms",
        "hrv_proxy_nocturnal",
        "bb_recharge_efficiency",
        "avg_stress",
        "high_stress_pct",
        "body_battery_high",
        "body_battery_low",
        "steps",
    }
)
INTRADAY = frozenset(
    {"steps", "avg_stress", "high_stress_pct", "body_battery_high", "body_battery_low"}
)


def _percentiles(series: pd.Series, p_low: float, p_high: float) -> Tuple[Optional[float], Optional[float]]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 3:
        return None, None
    return float(s.quantile(p_low)), float(s.quantile(p_high))


def _window_slice(df: pd.DataFrame, target: date, days: int) -> pd.DataFrame:
    start = target - timedelta(days=days)
    return df[(df["date"] >= start) & (df["date"] < target)]


def _last7_slice(df: pd.DataFrame, target: date) -> pd.DataFrame:
    start = target - timedelta(days=6)
    return df[(df["date"] >= start) & (df["date"] <= target)]


def _magnitude_sleep_like_positive(delta: Optional[float]) -> Optional[str]:
    """Good sleep extension: symmetric bands to negative-side thresholds."""
    if delta is None or delta <= 0:
        return None
    if delta < 20:
        return "noise"
    if delta < 45:
        return "mild"
    if delta < 80:
        return "significant"
    return "strong"


def _magnitude_rhr_positive(delta: Optional[float]) -> Optional[str]:
    """Lower RHR than baseline — good. Use |delta| with same thresholds as elevated RHR."""
    if delta is None or delta >= -1:
        return None
    ad = abs(delta)
    if ad < 2:
        return "noise"
    if ad < 5:
        return "mild"
    if ad < 8:
        return "significant"
    return "strong"


def _magnitude_steps_positive(delta: Optional[float], baseline: Optional[float]) -> Optional[str]:
    if delta is None or baseline is None or baseline < 1000:
        return None
    ratio = delta / baseline
    if ratio <= 0.1:
        return None
    if ratio < 0.2:
        return "mild"
    if ratio < 0.35:
        return "significant"
    return "strong"


def _magnitude_body_battery_high_positive(delta: Optional[float]) -> Optional[str]:
    if delta is None or delta <= 0:
        return None
    if delta < 5:
        return "noise"
    if delta < 12:
        return "mild"
    if delta < 22:
        return "significant"
    return "strong"


def _magnitude_body_battery_low_positive(delta: Optional[float]) -> Optional[str]:
    if delta is None or delta <= 0:
        return None
    if delta < 5:
        return "noise"
    if delta < 10:
        return "mild"
    if delta < 18:
        return "significant"
    return "strong"


def _magnitude_sleep_score_positive(delta: Optional[float]) -> Optional[str]:
    if delta is None or delta <= 0:
        return None
    if delta < 5:
        return "noise"
    if delta < 12:
        return "mild"
    if delta < 22:
        return "significant"
    return "strong"


def _magnitude_deep_sleep_positive(delta: Optional[float]) -> Optional[str]:
    if delta is None or delta <= 0:
        return None
    if delta < 8:
        return "noise"
    if delta < 18:
        return "mild"
    if delta < 32:
        return "significant"
    return "strong"


def _combined_magnitude(field: str, block: Dict[str, Any]) -> Optional[str]:
    """Concern-side magnitude from v3 classifiers + good-side magnitude for positives."""
    d = block.get("delta")
    baseline = block.get("baseline")
    fn = MAGNITUDE_CLASSIFIERS.get(field)
    # v3 classifiers expect baseline_7d key for steps
    legacy_block = {**block, "baseline_7d": baseline}
    bad = fn(legacy_block) if fn else None
    if bad and bad != "noise":
        return bad
    good = None
    if field == "sleep_minutes":
        good = _magnitude_sleep_like_positive(d)
    elif field == "deep_sleep_minutes":
        good = _magnitude_deep_sleep_positive(d)
    elif field == "sleep_score":
        good = _magnitude_sleep_score_positive(d)
    elif field == "rhr_bpm":
        good = _magnitude_rhr_positive(d)
    elif field == "steps":
        good = _magnitude_steps_positive(d, baseline)
    elif field == "body_battery_high":
        good = _magnitude_body_battery_high_positive(d)
    elif field == "body_battery_low":
        good = _magnitude_body_battery_low_positive(d)
    return good if good and good != "noise" else bad


def classify_direction(field: str, delta: Optional[float], baseline: Optional[float]) -> Optional[str]:
    if delta is None:
        return None
    if field == "rhr_bpm":
        if delta < -1:
            return "positive"
        if delta > 1:
            return "negative"
        return "neutral"
    if field == "sleep_minutes":
        if delta > 20:
            return "positive"
        if delta < -20:
            return "negative"
        return "neutral"
    if field == "deep_sleep_minutes":
        if delta > 8:
            return "positive"
        if delta < -8:
            return "negative"
        return "neutral"
    if field == "awake_minutes":
        if delta < -8:
            return "positive"
        if delta > 8:
            return "negative"
        return "neutral"
    if field == "sleep_score":
        if delta > 5:
            return "positive"
        if delta < -5:
            return "negative"
        return "neutral"
    if field == "sleep_stress":
        if delta < -4:
            return "positive"
        if delta > 4:
            return "negative"
        return "neutral"
    if field == "waking_rr_brpm":
        if delta < -0.5:
            return "positive"
        if delta > 0.5:
            return "negative"
        return "neutral"
    if field in ("avg_stress", "high_stress_pct"):
        thr = 4 if field == "avg_stress" else 3
        if delta < -thr:
            return "positive"
        if delta > thr:
            return "negative"
        return "neutral"
    if field in ("body_battery_high", "body_battery_low"):
        if delta > 5:
            return "positive"
        if delta < -5:
            return "negative"
        return "neutral"
    if field == "rem_minutes":
        if delta > 10:
            return "positive"
        if delta < -10:
            return "negative"
        return "neutral"
    if field in ("hrv_rmssd_ms", "hrv_proxy_nocturnal"):
        if delta > 0:
            return "positive"
        if delta < 0:
            return "negative"
        return "neutral"
    if field == "bb_recharge_efficiency":
        if delta > 0.05:
            return "positive"
        if delta < -0.05:
            return "negative"
        return "neutral"
    if field == "sleep_rr":
        if delta < -0.5:
            return "positive"
        if delta > 0.5:
            return "negative"
        return "neutral"
    if field == "steps":
        if baseline is None or baseline < 1000:
            return "neutral"
        ratio = delta / baseline
        if ratio > 0.1:
            return "positive"
        if ratio < -0.1:
            return "negative"
        return "neutral"
    return "neutral"


def compute_window_stats(
    series: pd.Series,
) -> Tuple[Optional[float], Optional[List[float]], Optional[List[float]]]:
    """Return baseline mean, [p25,p75], [min,max] or Nones if insufficient data."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None, None, None
    baseline = round(float(s.mean()), 2)
    p25, p75 = _percentiles(s, 0.25, 0.75)
    typical = None if p25 is None or p75 is None else [round(p25, 2), round(p75, 2)]
    rng = [round(float(s.min()), 2), round(float(s.max()), 2)]
    return baseline, typical, rng


def _time_to_seconds(t: Any) -> Optional[float]:
    if t is None or pd.isna(t):
        return None
    if isinstance(t, pd.Timestamp):
        return t.hour * 3600 + t.minute * 60 + t.second
    if isinstance(t, datetime):
        return t.hour * 3600 + t.minute * 60 + t.second
    try:
        ts = pd.to_datetime(t)
        return ts.hour * 3600 + ts.minute * 60 + ts.second
    except (ValueError, TypeError):
        return None


def circular_mean_hhmm(timestamps: List[Any]) -> Optional[str]:
    """Circular mean of time-of-day; return HH:MM rounded to nearest 15 min."""
    secs = [_time_to_seconds(t) for t in timestamps]
    secs = [s for s in secs if s is not None and pd.notna(s)]
    if not secs:
        return None
    angles = np.array(secs, dtype=float) * (2 * math.pi / 86400.0)
    mean_sin = float(np.mean(np.sin(angles)))
    mean_cos = float(np.mean(np.cos(angles)))
    if pd.isna(mean_sin) or pd.isna(mean_cos):
        return None
    mean_angle = math.atan2(mean_sin, mean_cos)
    if mean_angle < 0:
        mean_angle += 2 * math.pi
    mean_sec = mean_angle / (2 * math.pi) * 86400.0
    mean_sec = mean_sec % 86400
    if pd.isna(mean_sec):
        return None
    # nearest 15 min
    rounded = int(round(mean_sec / 900.0)) * 900 % 86400
    h = rounded // 3600
    m = (rounded % 3600) // 60
    return f"{h:02d}:{m:02d}"


def _subtract_minutes_from_hhmm(hhmm: str, minutes: int) -> str:
    h, m = int(hhmm[:2]), int(hhmm[3:5])
    total = h * 60 + m - minutes
    total %= 24 * 60
    return f"{total // 60:02d}:{total % 60:02d}"


def _hhmm_to_minutes(hhmm: str) -> Optional[int]:
    if not isinstance(hhmm, str) or not re.match(r"^\d{2}:\d{2}$", hhmm):
        return None
    return int(hhmm[:2]) * 60 + int(hhmm[3:5])


def _earlier_hhmm(a: Optional[str], b: Optional[str]) -> Optional[str]:
    """Pick the chronologically earlier bedtime in the evening/overnight window."""
    if not a:
        return b
    if not b:
        return a
    am = _hhmm_to_minutes(a)
    bm = _hhmm_to_minutes(b)
    if am is None:
        return b
    if bm is None:
        return a
    # Normalize overnight clock into a bedtime axis where 18:00..23:59 come first,
    # then 00:00..05:59. This avoids treating 00:30 as "earlier" than 23:45.
    an = am if am >= 18 * 60 else am + 24 * 60
    bn = bm if bm >= 18 * 60 else bm + 24 * 60
    return a if an <= bn else b


def _bedtime_from_wake_goal(wake_hhmm: Optional[str], goal_sleep_min: Optional[int]) -> Optional[str]:
    wake_min = _hhmm_to_minutes(wake_hhmm) if wake_hhmm else None
    if wake_min is None or goal_sleep_min is None or goal_sleep_min <= 0:
        return None
    bedtime = (wake_min - int(goal_sleep_min)) % (24 * 60)
    return f"{bedtime // 60:02d}:{bedtime % 60:02d}"


def compute_sleep_context(
    sleep_df: pd.DataFrame,
    target: date,
    sleep_minutes_block: Dict[str, Any],
    deep_sleep_block: Dict[str, Any],
) -> Dict[str, Any]:
    """typical bed/wake from 14d circular means; target bedtime from deficits."""
    out = {
        "typical_bedtime": None,
        "typical_wake": None,
        "typical_duration_min": None,
        "typical_duration_hm": None,
        "target_bedtime_tonight": None,
    }
    if sleep_df is None or sleep_df.empty:
        return out
    df = sleep_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    win = _window_slice(df, target, 14)
    if win.empty:
        return out
    valid = pd.Series(True, index=win.index)
    if "sleep_start" in win.columns:
        valid &= win["sleep_start"].notna()
    if "sleep_minutes" in win.columns:
        valid &= pd.to_numeric(win["sleep_minutes"], errors="coerce").fillna(0) > 0
    win = win[valid]
    if win.empty:
        return out
    if "sleep_start" in win.columns:
        out["typical_bedtime"] = circular_mean_hhmm(win["sleep_start"].tolist())
    if "sleep_end" in win.columns:
        out["typical_wake"] = circular_mean_hhmm(win["sleep_end"].tolist())
    if "sleep_minutes" in win.columns:
        sm = pd.to_numeric(win["sleep_minutes"], errors="coerce").dropna()
        if not sm.empty:
            dur = int(round(float(sm.mean())))
            out["typical_duration_min"] = dur
            out["typical_duration_hm"] = format_minutes_hm(dur)

    typical_bt = out["typical_bedtime"]
    if not typical_bt:
        return out

    sm_mag = (sleep_minutes_block or {}).get("magnitude") or "noise"
    ds_mag = (deep_sleep_block or {}).get("magnitude") or "noise"
    pullback = {"noise": 0, "mild": 15, "significant": 30, "strong": 45}.get(sm_mag, 0)
    if ds_mag in ("significant", "strong"):
        pullback += 15
    pullback = min(pullback, 90)
    if sm_mag == "noise" and ds_mag not in ("significant", "strong"):
        target_bt = typical_bt
    else:
        target_bt = _subtract_minutes_from_hhmm(typical_bt, pullback)

    # Guardrail: when wake time is known, do not propose a bedtime that cannot deliver
    # at least 7h (or the user's typical duration when longer).
    typical_duration = out.get("typical_duration_min")
    goal_sleep = max(420, int(typical_duration)) if typical_duration else 420
    duration_based_bt = _bedtime_from_wake_goal(out.get("typical_wake"), goal_sleep)
    out["target_bedtime_tonight"] = _earlier_hhmm(target_bt, duration_based_bt)
    return out


def _linear_slope(y: np.ndarray) -> float:
    """Least squares slope per day index."""
    if len(y) < 2:
        return 0.0
    x = np.arange(len(y), dtype=float)
    x_mean = x.mean()
    y_mean = y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return 0.0
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def compute_score_trajectory(
    history_30d: List[Tuple[date, float]],
    target: date,
    higher_is_better: bool = True,
) -> Dict[str, Any]:
    """history_30d: list of (date, value) ascending by date."""
    empty = {
        "today": None,
        "yesterday": None,
        "two_days_ago": None,
        "range_7d": None,
        "typical_band": None,
        "direction_of_change": "stable",
        "higher_is_better": higher_is_better,
        "trajectory": "stable",
    }
    if not history_30d:
        return empty
    by_date = {d: v for d, v in history_30d}
    today_v = by_date.get(target)
    if today_v is None:
        return empty
    yday = by_date.get(target - timedelta(days=1))
    y2 = by_date.get(target - timedelta(days=2))

    last7_vals = []
    for i in range(7):
        d = target - timedelta(days=i)
        if d in by_date:
            last7_vals.append(by_date[d])
    range_7d = None
    if last7_vals:
        range_7d = [round(min(last7_vals), 1), round(max(last7_vals), 1)]

    vals30 = [v for _, v in history_30d if v is not None and not (isinstance(v, float) and math.isnan(v))]
    typical_band = None
    if len(vals30) >= 14:
        arr = np.array(vals30, dtype=float)
        p33, p66 = np.percentile(arr, [33, 66])
        if today_v <= p33:
            typical_band = "low"
        elif today_v <= p66:
            typical_band = "mid"
        else:
            typical_band = "high"

    y_arr = np.array(last7_vals[::-1], dtype=float) if last7_vals else np.array([], dtype=float)
    slope = _linear_slope(y_arr) if len(y_arr) >= 2 else 0.0
    d3 = (today_v - y2) if y2 is not None else 0.0

    # Raw numeric direction
    trajectory = "stable"
    if d3 >= 20 and slope > 0:
        trajectory = "improving_fast"
    elif d3 >= 8 or slope >= 2.0:
        trajectory = "improving"
    elif d3 <= -20 and slope < 0:
        trajectory = "declining_fast"
    elif d3 <= -8 or slope <= -2.0:
        trajectory = "declining"

    # Polarity-aware health direction
    direction_of_change = "stable"
    if not higher_is_better:
        d3_health = -d3
        slope_health = -slope
    else:
        d3_health = d3
        slope_health = slope
    if d3_health >= 20 and slope_health > 0:
        direction_of_change = "improving_fast"
    elif d3_health >= 8 or slope_health >= 2.0:
        direction_of_change = "improving"
    elif d3_health <= -20 and slope_health < 0:
        direction_of_change = "worsening_fast"
    elif d3_health <= -8 or slope_health <= -2.0:
        direction_of_change = "worsening"

    return {
        "today": round(float(today_v), 1),
        "yesterday": round(float(yday), 1) if yday is not None else None,
        "two_days_ago": round(float(y2), 1) if y2 is not None else None,
        "range_7d": range_7d,
        "typical_band": typical_band,
        "direction_of_change": direction_of_change,
        "higher_is_better": higher_is_better,
        "trajectory": trajectory,
    }


def enrich_today_wellness(
    df: pd.DataFrame,
    target: date,
    today_is_partial: bool,
) -> Dict[str, Any]:
    """Build v4 today_wellness dict with baseline_label, windows, direction, magnitude."""
    if df is None or df.empty:
        return {f: _empty_metric(unit) for f, unit in WELLNESS_FIELDS}

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    out: Dict[str, Any] = {}
    for field, unit in WELLNESS_FIELDS:
        if field not in df.columns:
            out[field] = _empty_metric(unit)
            continue
        window_days = 14 if field in WINDOW_14D else 30
        baseline_label = "14d" if field in WINDOW_14D else "30d"
        win = _window_slice(df, target, window_days)
        series = win[field] if not win.empty else pd.Series(dtype=float)
        baseline, typical_range, range_window = compute_window_stats(series)

        today_rows = df[df["date"] == target]
        if today_is_partial and field in INTRADAY:
            today_val = None
        else:
            today_val = (
                safe_float(today_rows.iloc[0][field]) if not today_rows.empty else None
            )

        delta = None
        if today_val is not None and baseline is not None:
            delta = round(today_val - baseline, 2)

        l7 = _last7_slice(df, target)
        l7s = pd.to_numeric(l7[field], errors="coerce") if field in l7.columns else pd.Series(dtype=float)
        is_max_7d = False
        is_min_7d = False
        if today_val is not None and not l7s.dropna().empty:
            is_max_7d = bool(today_val == l7s.max())
            is_min_7d = bool(today_val == l7s.min())

        block = {
            "today": today_val,
            "baseline": baseline,
            "baseline_label": baseline_label,
            "delta": delta,
            "typical_range": typical_range,
            "range_window": range_window,
            "is_max_7d": is_max_7d,
            "is_min_7d": is_min_7d,
            "unit": unit,
            "confidence": "high",
        }
        if unit == "minutes":
            block["today_hm"] = format_minutes_hm(today_val)
            block["baseline_hm"] = format_minutes_hm(baseline)
            block["delta_pm"] = format_delta_minutes_pm(delta)
            block["typical_range_hm"] = format_minutes_pair_hm(typical_range)
            block["range_window_hm"] = format_minutes_pair_hm(range_window)
        elif field == "bb_recharge_efficiency":
            if today_val is not None:
                block["today"] = round(today_val, 1)
            if baseline is not None:
                block["baseline"] = round(baseline, 1)
            if delta is not None:
                block["delta"] = round(delta, 1)
            block["today_fmt"] = format_recharge_rate(block["today"])
            block["baseline_fmt"] = format_recharge_rate(block["baseline"])
            block["delta_fmt"] = format_recharge_delta(block["delta"])
        block["direction"] = classify_direction(field, delta, baseline)
        block["magnitude"] = _combined_magnitude(field, block) if today_val is not None and baseline is not None else None
        out[field] = block
    return out


def _empty_metric(unit: str) -> Dict[str, Any]:
    out = {
        "today": None,
        "baseline": None,
        "baseline_label": "30d",
        "delta": None,
        "typical_range": None,
        "range_window": None,
        "is_max_7d": False,
        "is_min_7d": False,
        "magnitude": None,
        "direction": None,
        "confidence": "low",
        "unit": unit,
    }
    if unit == "minutes":
        out["today_hm"] = None
        out["baseline_hm"] = None
        out["delta_pm"] = None
        out["typical_range_hm"] = None
        out["range_window_hm"] = None
    return out

