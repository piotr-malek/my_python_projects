import numpy as np
import pandas as pd


def rolling_mean(series, window):
    s = series.dropna().tail(window)
    if len(s) < max(3, window // 2):
        return None
    return float(s.mean())


def deviation_today(today_val, baseline):
    if today_val is None or baseline is None or pd.isna(today_val) or pd.isna(baseline):
        return None
    return float(today_val - baseline)


def trend_increasing(series, window=5):
    s = pd.to_numeric(series, errors="coerce").dropna().tail(window)
    if len(s) < 3:
        return False
    y = s.to_numpy(dtype=float)
    x = np.arange(len(y))
    return np.polyfit(x, y, 1)[0] > 0
