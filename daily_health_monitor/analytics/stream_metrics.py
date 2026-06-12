import numpy as np

from analytics.training_load import normalized_power
from strava.streams import aligned_arrays, parse_streams_json


def _half_split_metric(streams, value_key, stable_key=None, stable_tolerance=0.1):
    aligned = aligned_arrays(streams)
    if not aligned or value_key not in aligned:
        return {"first_half": None, "second_half": None, "drift": None}

    val = aligned[value_key]
    n = len(val)
    if n < 60:
        return {"first_half": None, "second_half": None, "drift": None}

    mid = n // 2
    first, second = val[:mid], val[mid:]

    if stable_key and stable_key in aligned:
        stable = aligned[stable_key]
        med = np.median(stable)
        mask = np.abs(stable - med) <= med * stable_tolerance
        if mask.sum() >= 30:
            masked = val[mask]
            half = len(masked) // 2
            first, second = masked[:half], masked[half:]

    m1, m2 = float(np.nanmean(first)), float(np.nanmean(second))
    drift = (m2 - m1) / m1 if m1 else None
    return {"first_half": m1, "second_half": m2, "drift": drift}


def compute_activity_metrics(streams_json, sport_type):
    streams = parse_streams_json(streams_json)
    out = {
        "hr_drift": None,
        "aerobic_decoupling": None,
        "efficiency_factor": None,
        "np_proxy": None,
        "tss_proxy": None,
        "tss_source": None,
    }
    if not streams:
        return out

    aligned = aligned_arrays(streams)
    if "heartrate" in aligned:
        stable = "watts" if "watts" in aligned else "velocity_smooth"
        hr_d = _half_split_metric(streams, "heartrate", stable if stable in aligned else None)
        out["hr_drift"] = hr_d.get("drift")

    if "watts" in aligned and "heartrate" in aligned:
        w, hr = aligned["watts"], aligned["heartrate"]
        mid = len(w) // 2
        r1 = float(np.nanmean(w[:mid] / np.maximum(hr[:mid], 1)))
        r2 = float(np.nanmean(w[mid:] / np.maximum(hr[mid:], 1)))
        if r1 > 0:
            out["aerobic_decoupling"] = (r2 / r1) - 1
        np_val = normalized_power(w)
        if np_val:
            out["np_proxy"] = np_val
            out["efficiency_factor"] = np_val / float(np.nanmean(hr)) if np.nanmean(hr) else None
    elif "velocity_smooth" in aligned and "heartrate" in aligned:
        v, hr = aligned["velocity_smooth"], aligned["heartrate"]
        mid = len(v) // 2
        r1 = float(np.nanmean(v[:mid] / np.maximum(hr[:mid], 1)))
        r2 = float(np.nanmean(v[mid:] / np.maximum(hr[mid:], 1)))
        if r1 > 0:
            out["aerobic_decoupling"] = (r2 / r1) - 1

    return out
