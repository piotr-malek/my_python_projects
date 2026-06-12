"""Parse Garmin intraday stress timelines into band stats and peak/settle markers."""

from __future__ import annotations

import json
import math
from datetime import datetime, time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

DEFAULT_BANDS: Dict[str, Tuple[time, time]] = {
    "morning_06_09": (time(6, 0), time(9, 0)),
    "work_09_18": (time(9, 0), time(18, 0)),
    "evening_18_22": (time(18, 0), time(22, 0)),
}


def _parse_hhmm(hh: int, mm: int) -> time:
    return time(hh % 24, mm % 60)


def bands_from_config(cfg: Dict[str, str]) -> Dict[str, Tuple[time, time]]:
    """Parse config like {'morning': '06:00–09:00', 'work': '09:00–18:00'}."""
    out = {}
    mapping = {
        "morning": "morning_06_09",
        "work": "work_09_18",
        "evening": "evening_18_22",
    }
    for key, label in mapping.items():
        raw = cfg.get(key)
        if not raw or ("–" not in raw and "-" not in raw):
            out[label] = DEFAULT_BANDS[label]
            continue
        sep = "–" if "–" in raw else "-"
        a, b = raw.split(sep, 1)
        out[label] = (_parse_time_str(a.strip()), _parse_time_str(b.strip()))
    return out


def _minutes_to_time(mins: int) -> time:
    return time(mins // 60, mins % 60)


def _parse_time_str(s: str) -> time:
    parts = s.split(":")
    return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)


def parse_stress_samples(samples_json: Any) -> List[Tuple[datetime, float]]:
    """Return (local datetime, stress level) pairs; skip invalid readings."""
    if samples_json is None:
        return []
    if isinstance(samples_json, str):
        try:
            raw = json.loads(samples_json)
        except (json.JSONDecodeError, TypeError):
            return []
    else:
        raw = samples_json

    pairs: List[Tuple[datetime, float]] = []
    arr = None
    if isinstance(raw, dict):
        arr = raw.get("stressValuesArray") or raw.get("stressValueDescriptors")
    elif isinstance(raw, list):
        arr = raw
    if not arr:
        return []

    for item in arr:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            ts, level = item[0], item[1]
        elif isinstance(item, dict):
            ts = item.get("t") or item.get("timestamp") or item.get("startTimestampGMT")
            level = item.get("stress") or item.get("stressLevel") or item.get("value")
        else:
            continue
        try:
            level_f = float(level)
        except (TypeError, ValueError):
            continue
        if level_f < 0:
            continue
        try:
            if isinstance(ts, (int, float)):
                dt = pd.Timestamp(ts, unit="ms", tz="UTC").tz_convert(None).to_pydatetime()
            else:
                dt = pd.Timestamp(ts).to_pydatetime()
        except (ValueError, TypeError):
            continue
        pairs.append((dt, level_f))
    return pairs


def _in_band(t: time, start: time, end: time) -> bool:
    tm = t.hour * 60 + t.minute
    sm = start.hour * 60 + start.minute
    em = end.hour * 60 + end.minute
    if sm <= em:
        return sm <= tm < em
    return tm >= sm or tm < em


def _fmt_window(start: time, end: time) -> str:
    return f"{start.hour:02d}:{start.minute:02d}–{end.hour:02d}:{end.minute:02d}"


def analyze_stress_day(
    samples_json: Any,
    day_avg_stress: Optional[float] = None,
    bands_config: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Band means, peak window, settled_after, high_stress_minutes, rest_pct."""
    empty = {
        "bands": {},
        "peak_window": None,
        "peak_value": None,
        "settled_after": None,
        "high_stress_minutes": None,
        "rest_pct": None,
    }
    pairs = parse_stress_samples(samples_json)
    if len(pairs) < 6:
        return empty

    bands = bands_from_config(bands_config or {})
    band_vals: Dict[str, List[float]] = {k: [] for k in bands}
    all_levels = []
    rest_n = 0
    high_n = 0
    for dt, level in pairs:
        all_levels.append(level)
        t = dt.time()
        if level <= 25:
            rest_n += 1
        if level >= 50:
            high_n += 1
        for name, (start, end) in bands.items():
            if _in_band(t, start, end):
                band_vals[name].append(level)

    band_means = {
        k: round(float(sum(v) / len(v)), 1) if v else None for k, v in band_vals.items()
    }
    baseline = day_avg_stress if day_avg_stress is not None else (
        float(sum(all_levels) / len(all_levels)) if all_levels else None
    )

    # Peak: 3-hour sliding window (6 samples at ~30min would be 3h at 2min - use 90 samples max)
    # Use 1-hour windows for simplicity: group by hour
    hour_buckets: Dict[int, List[float]] = {}
    for dt, level in pairs:
        hour_buckets.setdefault(dt.hour, []).append(level)
    best_h, best_mean = None, -1.0
    for h, vals in hour_buckets.items():
        m = sum(vals) / len(vals)
        if m > best_mean:
            best_mean, best_h = m, h
    peak_window = f"{best_h:02d}:00–{(best_h + 3) % 24:02d}:00" if best_h is not None else None
    peak_value = int(round(best_mean)) if best_mean >= 0 else None

    settled_after = None
    if baseline is not None and best_h is not None:
        after_peak = [(dt, lvl) for dt, lvl in pairs if dt.hour >= best_h]
        settle_start = None
        run = 0
        for dt, lvl in after_peak:
            if lvl < baseline:
                if settle_start is None:
                    settle_start = dt
                run += 1
                if run >= 30:  # ~60 min at 2-min cadence
                    settled_after = f"{settle_start.hour:02d}:{settle_start.minute:02d}"
                    break
            else:
                settle_start = None
                run = 0

    sample_interval_min = 2
    high_stress_minutes = int(high_n * sample_interval_min)
    rest_pct = round(100.0 * rest_n / len(pairs), 1) if pairs else None

    return {
        "bands": band_means,
        "peak_window": peak_window,
        "peak_value": peak_value,
        "settled_after": settled_after,
        "high_stress_minutes": high_stress_minutes,
        "rest_pct": rest_pct,
    }
