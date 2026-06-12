"""Per-metric confidence scoring for v5 digest."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from analytics.digest_features import safe_float


def metric_confidence(field: str, block: Dict[str, Any], wellness: dict) -> str:
    today = block.get("today")
    baseline = block.get("baseline")
    if today is None:
        return "low"

    if field == "sleep_minutes":
        if _sleep_implausible(today, block, wellness):
            return "low"

    if today == 0 and baseline is not None and baseline > 0:
        if field in ("sleep_minutes", "deep_sleep_minutes", "rem_minutes", "light_minutes"):
            return "low"

    if field == "rhr_bpm" and (today < 30 or today > 120):
        return "low"
    if field in ("waking_rr_brpm", "sleep_rr") and (today < 6 or today > 30):
        return "low"

    if block.get("magnitude") is None and baseline is None:
        return "low"

    if field == "hrv_proxy_nocturnal":
        return "medium"

    return "high"


def _sleep_implausible(today_val, block, wellness) -> bool:
    if today_val is None or today_val >= 120:
        return False
    rhr_block = (block if False else {})  # noqa — use wellness
    hr_df = wellness.get("raw_heart_rate", pd.DataFrame())
    bb_df = wellness.get("raw_body_battery", pd.DataFrame())
    rhr = safe_float(block.get("_cross_rhr_today"))
    charged = safe_float(block.get("_cross_bb_charged"))
    if rhr is None and not hr_df.empty and "rhr" in hr_df.columns:
        rhr = safe_float(hr_df.sort_values("date").iloc[-1].get("rhr"))
    if charged is None and not bb_df.empty and "charged" in bb_df.columns:
        charged = safe_float(bb_df.sort_values("date").iloc[-1].get("charged"))
    baseline_rhr = safe_float(block.get("_cross_rhr_baseline"))
    if baseline_rhr is None:
        baseline_rhr = block.get("baseline")
    if rhr is not None and baseline_rhr is not None:
        if baseline_rhr - 3 <= rhr <= baseline_rhr + 3 and (charged or 0) > 30:
            return True
    return False


def apply_confidence_pass(today_wellness: Dict[str, Any], wellness: dict, hrv_source: str = "none") -> Dict[str, Any]:
    """Attach confidence; downgrade magnitude for low-confidence metrics."""
    hr_df = wellness.get("raw_heart_rate", pd.DataFrame())
    bb_df = wellness.get("raw_body_battery", pd.DataFrame())
    cross_rhr = cross_bb = cross_rhr_base = None
    if not hr_df.empty:
        last = hr_df.sort_values("date").iloc[-1]
        cross_rhr = safe_float(last.get("rhr"))
        if len(hr_df) >= 7:
            cross_rhr_base = safe_float(
                pd.to_numeric(hr_df.tail(30)["rhr"], errors="coerce").dropna().mean()
            )
    if not bb_df.empty:
        cross_bb = safe_float(bb_df.sort_values("date").iloc[-1].get("charged"))

    for field, block in today_wellness.items():
        if not isinstance(block, dict):
            continue
        block["_cross_rhr_today"] = cross_rhr
        block["_cross_rhr_baseline"] = cross_rhr_base
        block["_cross_bb_charged"] = cross_bb
        conf = metric_confidence(field, block, wellness)
        if field == "hrv_rmssd_ms" and hrv_source != "garmin_nightly":
            conf = "low"
        block["confidence"] = conf
        for k in ("_cross_rhr_today", "_cross_rhr_baseline", "_cross_bb_charged"):
            block.pop(k, None)
        if conf == "low":
            block["magnitude"] = None
            block["direction"] = None
    return today_wellness
