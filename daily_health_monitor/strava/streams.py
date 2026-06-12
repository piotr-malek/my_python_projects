import json

import numpy as np


def parse_streams_json(streams_json):
    if not streams_json:
        return {}
    try:
        data = json.loads(streams_json)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, list):
        return {s.get("type"): s.get("data", []) for s in data if isinstance(s, dict)}
    return {k: (v if isinstance(v, list) else []) for k, v in data.items()}


def aligned_arrays(streams):
    time_arr = np.array(streams.get("time") or [], dtype=float)
    if len(time_arr) == 0:
        return {}
    out = {"time": time_arr}
    for key in ("heartrate", "watts", "cadence", "velocity_smooth", "distance"):
        arr = streams.get(key)
        if arr and len(arr) == len(time_arr):
            out[key] = np.array(arr, dtype=float)
    return out
