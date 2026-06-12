"""Build stress_context block for v5.2 digest (retrospective stress reporting)."""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from analytics.digest_features import ELEVATED_MAGNITUDES, intensity_label, safe_float
from analytics.stress_curve import analyze_stress_day


def _magnitude_stress_positive(delta: Optional[float]) -> Optional[str]:
    if delta is None or delta <= 0:
        return None
    ad = abs(delta)
    if ad < 4:
        return "noise"
    if ad < 8:
        return "mild"
    if ad < 14:
        return "significant"
    return "strong"


def _parse_time_window(window: Optional[str]) -> Optional[Tuple[time, time]]:
    if not window or ("–" not in window and "-" not in window):
        return None
    sep = "–" if "–" in window else "-"
    start_s, end_s = window.split(sep, 1)
    parts_a = start_s.strip().split(":")
    parts_b = end_s.strip().split(":")
    if len(parts_a) < 2 or len(parts_b) < 2:
        return None
    return time(int(parts_a[0]), int(parts_a[1])), time(int(parts_b[0]), int(parts_b[1]))


def _time_to_minutes(t: time) -> int:
    return t.hour * 60 + t.minute


def _minutes_overlap(s1: time, e1: time, s2: time, e2: time) -> int:
    """Overlap length in minutes for same-day windows; end may be before start only for peak parsing."""
    a0, a1 = _time_to_minutes(s1), _time_to_minutes(e1)
    b0, b1 = _time_to_minutes(s2), _time_to_minutes(e2)
    start = max(a0, b0)
    end = min(a1, b1)
    return max(0, end - start)


def _windows_overlap(s1: time, e1: time, s2: time, e2: time) -> bool:
    return _minutes_overlap(s1, e1, s2, e2) > 0


def _fmt_time(t: time) -> str:
    return f"{t.hour:02d}:{t.minute:02d}"


def _intensity_minutes_on(intensity_minutes_df: Optional[pd.DataFrame], day: date) -> float:
    if intensity_minutes_df is None or intensity_minutes_df.empty:
        return 0.0
    row = intensity_minutes_df[intensity_minutes_df["date"] == day]
    if row.empty:
        return 0.0
    return safe_float(row.iloc[0].get("intensity_minutes")) or 0.0


def compute_activity_stress_overlap(
    day: date,
    intraday_shape: Dict[str, Any],
    activities_df: Optional[pd.DataFrame],
    intensity_minutes_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Detect when yesterday's stress peak coincided with logged exercise."""
    empty = {
        "peak_explained_by_exercise": False,
        "sessions": [],
    }
    peak_window = (intraday_shape or {}).get("peak_window")
    peak_bounds = _parse_time_window(peak_window)
    if peak_bounds is None or activities_df is None or activities_df.empty:
        return empty

    peak_start, peak_end = peak_bounds
    day_im = _intensity_minutes_on(intensity_minutes_df, day)
    sessions: List[Dict[str, Any]] = []

    df = activities_df.copy()
    if "local_date" not in df.columns and "start_date_local" in df.columns:
        df["local_date"] = pd.to_datetime(df["start_date_local"]).dt.date
    elif "local_date" in df.columns:
        df["local_date"] = pd.to_datetime(df["local_date"]).dt.date

    for _, row in df[df["local_date"] == day].iterrows():
        start_raw = row.get("start_date_local")
        if start_raw is None or (isinstance(start_raw, float) and pd.isna(start_raw)):
            continue
        start_dt = pd.Timestamp(start_raw).to_pydatetime()
        duration_sec = safe_float(row.get("moving_time")) or 0.0
        if duration_sec <= 0:
            continue
        end_dt = start_dt + timedelta(seconds=int(duration_sec))
        act_start = start_dt.time()
        act_end = end_dt.time()
        overlap_min = _minutes_overlap(act_start, act_end, peak_start, peak_end)
        overlaps_peak = overlap_min > 0
        # Hard efforts often elevate stress just before the peak window starts.
        pre_peak_carry = False
        if not overlaps_peak and end_dt.date() == start_dt.date():
            gap = _time_to_minutes(peak_start) - _time_to_minutes(act_end)
            label_probe = intensity_label(
                row.get("tss_proxy"), row.get("suffer_score"), day_im, True
            )
            pre_peak_carry = 0 <= gap <= 60 and label_probe in ("hard", "very_hard")

        if not overlaps_peak and not pre_peak_carry:
            continue

        label = intensity_label(row.get("tss_proxy"), row.get("suffer_score"), day_im, True)
        sessions.append(
            {
                "name": row.get("name") or row.get("sport_type") or "Activity",
                "sport": row.get("sport_type"),
                "start_local": _fmt_time(act_start),
                "end_local": _fmt_time(act_end),
                "duration_min": int(round(duration_sec / 60)),
                "intensity_label": label,
                "overlaps_peak": overlaps_peak,
                "overlap_minutes": overlap_min if overlaps_peak else None,
            }
        )

    exercise_labels = {"moderate", "hard", "very_hard"}
    peak_explained = any(
        s.get("overlaps_peak") and s.get("intensity_label") in exercise_labels
        for s in sessions
    ) or any(s.get("intensity_label") in ("hard", "very_hard") for s in sessions)

    return {
        "peak_explained_by_exercise": bool(peak_explained and sessions),
        "peak_window": peak_window,
        "sessions": sessions,
    }


def compute_likely_affected_last_night(
    yesterday_stress: Dict[str, Any],
    deep_sleep_block: Dict[str, Any],
    activity_overlap: Optional[Dict[str, Any]] = None,
) -> bool:
    """Yesterday high stress AND last-night deep sleep significantly below baseline."""
    if not yesterday_stress or not deep_sleep_block:
        return False
    overlap = activity_overlap or {}
    if overlap.get("peak_explained_by_exercise"):
        return False

    ys = yesterday_stress.get("avg_stress")
    mag = yesterday_stress.get("magnitude")
    work = (yesterday_stress.get("intraday_shape") or {}).get("bands", {}).get("work_09_18")
    stress_high = (
        mag in ELEVATED_MAGNITUDES
        or (ys is not None and yesterday_stress.get("vs_baseline", 0) >= 8)
        or (work is not None and yesterday_stress.get("vs_baseline") is not None
            and work >= (ys or 0) + 5)
    )
    deep_bad = (
        deep_sleep_block.get("magnitude") in ("significant", "strong")
        and deep_sleep_block.get("direction") == "negative"
    )
    return bool(stress_high and deep_bad)


def build_stress_context(
    target: date,
    complete_df: pd.DataFrame,
    raw_stress_row: Optional[pd.Series],
    bands_config: Dict[str, str],
    today_wellness: Dict[str, Any],
    activities_df: Optional[pd.DataFrame] = None,
    intensity_minutes_df: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """Yesterday complete stress + intraday shape + 7d trend."""
    yesterday = target - timedelta(days=1)
    empty = {
        "yesterday": {},
        "intraday_shape": {},
        "trend_7d": {},
        "activity_overlap": {},
        "likely_affected_last_night": False,
    }
    if complete_df is None or complete_df.empty:
        return empty

    df = complete_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    hist = df[df["date"] < target].sort_values("date")
    if hist.empty:
        return empty

    yrow = hist[hist["date"] == yesterday]
    if yrow.empty:
        yrow = hist.tail(1)
    y = yrow.iloc[-1]

    base30 = float(pd.to_numeric(hist["avg_stress_fullday"], errors="coerce").tail(30).mean())
    avg_y = safe_float(y.get("avg_stress_fullday"))
    vs_base = round(avg_y - base30, 1) if avg_y is not None and not math.isnan(base30) else None

    intraday = {
        "bands": {
            "morning_06_09": safe_float(y.get("stress_band_morning")),
            "work_09_18": safe_float(y.get("stress_band_work")),
            "evening_18_22": safe_float(y.get("stress_band_evening")),
        },
        "peak_window": y.get("stress_peak_window"),
        "peak_value": safe_float(y.get("stress_peak_value")),
        "settled_after": y.get("stress_settled_after"),
        "high_stress_minutes": safe_float(y.get("high_stress_minutes")),
    }
    if raw_stress_row is not None and raw_stress_row.get("samples_json"):
        live = analyze_stress_day(
            raw_stress_row.get("samples_json"),
            day_avg_stress=avg_y,
            bands_config=bands_config,
        )
        if live.get("bands"):
            intraday.update({k: v for k, v in live.items() if v})

    mag = _magnitude_stress_positive(vs_base)
    direction = "negative" if vs_base and vs_base > 0 else "neutral"
    if vs_base and vs_base < -4:
        direction = "positive"

    yesterday_block = {
        "avg_stress": avg_y,
        "vs_baseline": vs_base,
        "magnitude": mag,
        "direction": direction,
        "high_stress_pct": safe_float(y.get("high_stress_pct_fullday")),
        "rest_pct": safe_float(y.get("rest_pct_fullday")),
        "confidence": "high",
    }

    last7 = hist.tail(7)
    avg7 = float(pd.to_numeric(last7["avg_stress_fullday"], errors="coerce").mean())
    vs30 = round(avg7 - base30, 1) if not math.isnan(base30) else None
    streak = 0
    vals = pd.to_numeric(hist["avg_stress_fullday"], errors="coerce").dropna().tolist()
    if len(vals) >= 2:
        for i in range(len(vals) - 1, 0, -1):
            if vals[i] > vals[i - 1]:
                streak += 1
            else:
                break
    high_days = int((last7["avg_stress_fullday"] > base30 + 3).sum()) if len(last7) else 0

    trend = {
        "avg_stress_7d": round(avg7, 1) if not math.isnan(avg7) else None,
        "vs_baseline_30d": vs30,
        "rising_streak_days": streak,
        "high_stress_day_count_7d": high_days,
        "elevated": bool(vs30 is not None and vs30 >= 4),
    }

    activity_overlap = compute_activity_stress_overlap(
        yesterday,
        intraday,
        activities_df,
        intensity_minutes_df,
    )

    likely = compute_likely_affected_last_night(
        {"avg_stress": avg_y, "magnitude": mag, "vs_baseline": vs_base, "intraday_shape": intraday},
        today_wellness.get("deep_sleep_minutes") or {},
        activity_overlap,
    )

    return {
        "yesterday": yesterday_block,
        "intraday_shape": intraday,
        "trend_7d": trend,
        "activity_overlap": activity_overlap,
        "likely_affected_last_night": likely,
    }
