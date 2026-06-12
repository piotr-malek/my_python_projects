"""Garmin nightly HRV ingest + nocturnal HR proxy fallback."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd

from util.json_util import to_json


def fetch_hrv_day(garmin_client, d) -> Dict[str, Any]:
    """Return raw_hrv row dict for date d."""
    cdate = d.isoformat()
    try:
        raw = garmin_client.call(garmin_client.api.get_hrv_data, cdate) or {}
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}

    last_night = raw.get("lastNightAvg") or raw.get("lastNightAverage")
    weekly = raw.get("weeklyAvg") or raw.get("weeklyAverage")
    status = raw.get("status")
    baseline = raw.get("baseline") or {}
    if isinstance(baseline, dict):
        low = baseline.get("lowUpper") or baseline.get("balancedLow")
        high = baseline.get("highLower") or baseline.get("balancedHigh")
    else:
        low = high = None

    return {
        "date": cdate,
        "last_night_avg_ms": float(last_night) if last_night is not None else None,
        "weekly_avg_ms": float(weekly) if weekly is not None else None,
        "status": status,
        "baseline_low_ms": float(low) if low is not None else None,
        "baseline_high_ms": float(high) if high is not None else None,
        "raw_json": to_json(raw),
    }


def nocturnal_proxy_index(
    hr_row: Optional[pd.Series],
    sleep_row: Optional[pd.Series],
) -> Optional[float]:
    """Nocturnal autonomic proxy from overnight HR samples (NOT RMSSD)."""
    if hr_row is None or sleep_row is None:
        return None
    samples_raw = hr_row.get("samples_json")
    if not samples_raw:
        return None
    try:
        samples = json.loads(samples_raw) if isinstance(samples_raw, str) else samples_raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not samples:
        return None

    start_ms, end_ms = _sleep_window_ms(sleep_row)
    if start_ms is None or end_ms is None:
        return None

    hrs = []
    for item in samples:
        if not isinstance(item, dict):
            continue
        t = item.get("t")
        hr = item.get("hr")
        if t is None or hr is None:
            continue
        try:
            t_ms = int(t)
            hr_f = float(hr)
        except (TypeError, ValueError):
            continue
        if start_ms <= t_ms <= end_ms and hr_f > 0:
            hrs.append(hr_f)

    if len(hrs) < 3:
        return None

    min_hr = min(hrs)
    first_chunk = hrs[: max(1, len(hrs) // 6)]
    onset_hr = sum(first_chunk) / len(first_chunk)
    drop = max(0.0, onset_hr - min_hr)
    # Higher index = stronger nocturnal parasympathetic drop (better recovery proxy)
    return round(min_hr * -0.5 + drop * 2.0, 2)


def _sleep_window_ms(sleep_row: pd.Series) -> tuple[Optional[int], Optional[int]]:
    """Return real Unix ms bounds for overnight HR filtering."""
    raw = sleep_row.get("raw_json")
    if raw:
        try:
            payload = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            payload = None
        if isinstance(payload, dict):
            start = payload.get("sleepStartTimestampGMT")
            end = payload.get("sleepEndTimestampGMT")
            if start is not None and end is not None:
                try:
                    return int(start), int(end)
                except (TypeError, ValueError):
                    pass

    sleep_start = sleep_row.get("sleep_start")
    sleep_end = sleep_row.get("sleep_end")
    if pd.isna(sleep_start) or pd.isna(sleep_end):
        return None, None
    return _to_ms(sleep_start), _to_ms(sleep_end)


def _to_ms(ts) -> Optional[int]:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, pd.Timestamp):
        return int(ts.timestamp() * 1000)
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    try:
        return int(pd.Timestamp(ts).timestamp() * 1000)
    except (ValueError, TypeError):
        return None
