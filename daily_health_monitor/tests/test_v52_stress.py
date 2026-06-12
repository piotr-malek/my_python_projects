"""v5.2 tests: stress curve, materialize, stress context."""

import json
from datetime import date, datetime, timedelta

import pandas as pd

from analytics.stress_curve import analyze_stress_day, parse_stress_samples
from analytics.stress_context import (
    compute_activity_stress_overlap,
    compute_likely_affected_last_night,
)
from analytics.insight_detectors import _yesterday_work_stress
from jobs.materialize_history import materialize_day_row


def _sample_timeline(base_date, hours_levels):
    arr = []
    for h, level in hours_levels:
        ts = int(datetime(base_date.year, base_date.month, base_date.day, h, 0).timestamp() * 1000)
        arr.append([ts, level])
    return json.dumps({"stressValuesArray": arr})


def test_parse_stress_samples():
    raw = _sample_timeline(date(2026, 6, 1), [(10, 40), (11, 55), (12, 50)])
    pairs = parse_stress_samples(raw)
    assert len(pairs) == 3


def test_analyze_stress_day_peak_and_bands():
    raw = _sample_timeline(
        date(2026, 6, 1),
        [(7, 20), (10, 55), (11, 58), (12, 52), (14, 45), (19, 25)],
    )
    out = analyze_stress_day(raw, day_avg_stress=40)
    assert out["bands"].get("work_09_18") is not None
    assert out["peak_window"] is not None
    assert out["rest_pct"] is not None


def test_likely_affected_last_night():
    stress = {"avg_stress": 44, "magnitude": "significant", "vs_baseline": 13, "intraday_shape": {}}
    deep = {"magnitude": "significant", "direction": "negative"}
    assert compute_likely_affected_last_night(stress, deep) is True
    assert compute_likely_affected_last_night(stress, {"magnitude": "noise"}) is False


def test_likely_affected_last_night_skips_exercise_explained_peak():
    stress = {"avg_stress": 44, "magnitude": "significant", "vs_baseline": 13, "intraday_shape": {}}
    deep = {"magnitude": "significant", "direction": "negative"}
    overlap = {"peak_explained_by_exercise": True, "sessions": [{"name": "Ride"}]}
    assert compute_likely_affected_last_night(stress, deep, overlap) is False


def test_activity_stress_overlap_detects_exercise_during_peak():
    day = date(2026, 6, 9)
    activities = pd.DataFrame(
        [
            {
                "name": "Intense ride",
                "sport_type": "Ride",
                "start_date_local": "2026-06-09 11:05:00",
                "local_date": day,
                "moving_time": 4167,
                "tss_proxy": 85,
                "suffer_score": 90,
            },
            {
                "name": "Brief spin",
                "sport_type": "Ride",
                "start_date_local": "2026-06-09 12:18:15",
                "local_date": day,
                "moving_time": 154,
                "tss_proxy": 5,
                "suffer_score": 10,
            },
        ]
    )
    intraday = {"peak_window": "12:00–15:00"}
    out = compute_activity_stress_overlap(day, intraday, activities)
    assert out["peak_explained_by_exercise"] is True
    assert len(out["sessions"]) == 2
    assert out["sessions"][0]["overlaps_peak"] is True


def test_yesterday_work_stress_reframes_exercise_overlap():
    sc = {
        "yesterday": {"avg_stress": 28, "vs_baseline": 6, "magnitude": "mild"},
        "intraday_shape": {"peak_window": "12:00–15:00", "bands": {"work_09_18": 30}},
        "trend_7d": {"high_stress_day_count_7d": 4},
        "activity_overlap": {
            "peak_explained_by_exercise": True,
            "sessions": [
                {
                    "name": "Intense ride",
                    "start_local": "11:05",
                    "end_local": "12:14",
                    "duration_min": 69,
                    "intensity_label": "hard",
                    "overlaps_peak": True,
                    "overlap_minutes": 14,
                }
            ],
        },
    }
    finding = _yesterday_work_stress(sc)
    assert finding is not None
    assert finding.id == "yesterday_exercise_stress"
    assert "work-induced" in finding.summary
    assert "Intense ride" in finding.summary
    assert "work hours" not in finding.summary.lower()


def test_materialize_day_row_full_day_stress():
    d = date(2026, 6, 1)
    stress_row = pd.Series(
        {
            "avg_stress": 41,
            "high_pct": 17,
            "samples_json": _sample_timeline(
                d, [(7, 20), (8, 22), (9, 25), (10, 44), (11, 48), (12, 42), (13, 40), (19, 25)]
            ),
        }
    )
    row = materialize_day_row(
        d,
        pd.Series({"rhr": 49}),
        pd.Series({"sleep_minutes": 420, "deep_minutes": 66, "rem_minutes": 90, "awake_minutes": 20}),
        stress_row,
        pd.Series({"steps": 5000, "intensity_minutes": 30}),
        pd.Series({"charged": 60, "bb_high": 80, "bb_low": 20}),
        pd.Series({"waking_rr": 14, "sleep_rr": 13}),
        pd.Series({"nocturnal_proxy": -17.5}),
        pd.Series({"tss": 20}),
        {"morning": "06:00–09:00", "work": "09:00–18:00", "evening": "18:00–22:00"},
        {"deep_mean": 60, "deep_std": 10, "rem_mean": 85, "rem_std": 15, "eff_mean": 0.9, "eff_std": 0.05},
    )
    assert row["avg_stress_fullday"] == 41
    assert row["steps_fullday"] == 5000
    assert row["stress_band_work"] is not None
