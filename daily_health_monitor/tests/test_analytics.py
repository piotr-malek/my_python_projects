import json

import numpy as np
import pandas as pd

from analytics import baselines, training_load, wellness_scores
from analytics.stream_metrics import compute_activity_metrics
from strava.transforms import parse_garmin_activity_id
from util.json_util import to_json


def test_normalized_power():
    watts = np.array([200] * 100 + [250] * 100, dtype=float)
    np_val = training_load.normalized_power(watts)
    assert np_val is not None
    assert 200 < np_val < 250


def test_tss_from_power():
    tss = training_load.tss_from_power(200, 3600, 250)
    assert 50 < tss < 90


def test_tss_from_power_long_ride():
    # 2h41m at NP ~203W, FTP 225 → ~218 TSS (was ~0.06 with the old formula)
    tss = training_load.tss_from_power(203.3, 9645, 225)
    assert 180 < tss < 260


def test_cached_power_tss_untrusted():
    row = pd.Series(
        {
            "moving_time": 9645,
            "weighted_avg_watts": 198.0,
            "suffer_score": 196.0,
            "tss_proxy": 0.06,
            "tss_source": "power",
        }
    )
    assert training_load.cached_tss_untrusted(row, 0.06, "power")
    tss, src = training_load.activity_tss_with_source(row, None, 225, 170)
    assert src == "suffer_score"
    assert tss == 196.0


def test_rolling_mean():
    s = pd.Series([50, 52, 51, 53, 54])
    assert baselines.rolling_mean(s, 5) == 52.0


def test_trend_increasing_after_ratio_with_na():
    awake = pd.Series([10.0, 12, 15, 18, 20])
    sleep = pd.Series([400.0, 400, 400, 400, 400])
    ratio = awake / sleep.replace(0, pd.NA)
    assert baselines.trend_increasing(ratio)


def test_activity_tss_suffer_score():
    row = pd.Series({"moving_time": 3600, "suffer_score": 45, "avg_hr": 140})
    tss = training_load.activity_tss(row, None, 250, 170)
    assert tss == 45.0


def test_stream_hr_drift():
    streams = {
        "time": list(range(120)),
        "heartrate": [130] * 60 + [145] * 60,
        "watts": [180] * 120,
    }
    metrics = compute_activity_metrics(to_json(streams), "Ride")
    assert metrics["hr_drift"] is not None
    assert metrics["hr_drift"] > 0


def test_compute_wellness_flags_handles_na_steps():
    dates = pd.date_range("2026-05-20", periods=7, freq="D")
    wellness = {
        "raw_activity_daily": pd.DataFrame(
            {"date": dates, "steps": [8000, 9000, 8500, 9200, 8800, 9100, pd.NA]}
        ),
    }
    flags, _ = wellness_scores.compute_wellness_flags(wellness, dates[-1].date())
    assert flags == []


def test_parse_garmin_external_id():
    assert parse_garmin_activity_id("garmin_ping_346043841044") == "346043841044"
    assert parse_garmin_activity_id(None) is None
