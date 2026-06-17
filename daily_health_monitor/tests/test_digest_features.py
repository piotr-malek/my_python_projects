from datetime import date, timedelta

import pandas as pd

from analytics import digest_features


def _wellness_with_delta(field, today_val, baseline_val):
    return {
        field: {
            "today": today_val,
            "baseline_7d": baseline_val,
            "delta": today_val - baseline_val,
            "unit": "",
        }
    }


def test_intensity_label_tiers():
    assert digest_features.intensity_label(0, 0, 0, False) == "rest"
    assert digest_features.intensity_label(30, 30, 20, True) == "easy"
    assert digest_features.intensity_label(50, 0, 0, True) == "moderate"
    assert digest_features.intensity_label(85, 0, 0, True) == "hard"
    assert digest_features.intensity_label(110, 0, 0, True) == "very_hard"


def test_is_hard_day_composite_rule():
    assert digest_features.is_hard_day(80, 0, 0)
    assert digest_features.is_hard_day(0, 90, 0)
    assert not digest_features.is_hard_day(0, 0, 60)
    assert not digest_features.is_hard_day(50, 50, 30)


def test_build_last_7d_no_activities_does_not_use_intensity_minutes_for_hard_day():
    target = date(2026, 5, 25)
    im = pd.DataFrame(
        [
            {"date": target - timedelta(days=1), "intensity_minutes": 90},
            {"date": target - timedelta(days=2), "intensity_minutes": 70},
        ]
    )
    out = digest_features.build_last_7d(pd.DataFrame(), im, target)
    assert out["hard_day_count"] == 0


def test_infer_tss_source_priority():
    assert digest_features.infer_tss_source({"tss_source": "power"}) == "power"
    assert digest_features.infer_tss_source({"weighted_avg_watts": 180}) == "power"
    assert digest_features.infer_tss_source({"suffer_score": 60}) == "suffer_score"
    assert digest_features.infer_tss_source({"avg_hr": 140}) == "hr"
    assert digest_features.infer_tss_source({}) == "none"


def test_compute_flags_rhr_and_sleep():
    wellness = {
        **_wellness_with_delta("rhr_bpm", 56, 48),
        **_wellness_with_delta("sleep_minutes", 410, 460),
        **_wellness_with_delta("waking_rr_brpm", 17.2, 14.6),
    }
    flags = digest_features.compute_flags(
        wellness, {"load_ratio": 0.9, "monotony": 1.4}, 3, prior_rhr_delta=4
    )
    assert "rhr_elevated_7d" in flags
    assert "sleep_short" in flags
    assert "waking_rr_up" in flags
    assert "load_ratio_high" not in flags


def test_compute_flags_load_and_rest():
    flags = digest_features.compute_flags(
        {}, {"load_ratio": 1.4, "monotony": 2.1}, 7
    )
    assert "load_ratio_high" in flags
    assert "monotony_rising" in flags
    assert "days_since_rest_long" in flags


def test_patterns_pre_illness_signal():
    flags = ["rhr_elevated_7d", "waking_rr_up", "sleep_fragmented"]
    patterns = digest_features.detect_patterns(flags, {"load_ratio": 1.0}, "low")
    assert any(p["id"] == "pre_illness_signal" for p in patterns)


def test_patterns_healthy_adaptation_requires_no_flags():
    load = {"load_ratio": 1.0, "ctl": 40}
    patterns = digest_features.detect_patterns([], load, "balanced", hrv_source="nocturnal_proxy")
    assert any(p["id"] == "healthy_adaptation" for p in patterns)
    patterns_with_flag = digest_features.detect_patterns(["sleep_short"], load, "balanced")
    assert not any(p["id"] == "healthy_adaptation" for p in patterns_with_flag)


def test_shape_wellness_window_today_baseline_delta():
    target = date(2026, 5, 25)
    rows = []
    for i, rhr in enumerate([48, 49, 47, 50, 48, 49, 47, 55]):
        rows.append({"date": target - timedelta(days=7 - i), "rhr_bpm": rhr})
    df = pd.DataFrame(rows)
    out = digest_features.shape_wellness_window(df, target)
    assert out["rhr_bpm"]["today"] == 55
    assert out["rhr_bpm"]["baseline_7d"] == round(
        float(pd.Series([48, 49, 47, 50, 48, 49, 47]).mean()), 2
    )
    assert out["rhr_bpm"]["delta"] == round(55 - out["rhr_bpm"]["baseline_7d"], 2)


def test_shape_wellness_window_nullifies_intraday_metrics_when_partial():
    target = date(2026, 5, 25)
    rows = []
    for i in range(8):
        rows.append(
            {
                "date": target - timedelta(days=7 - i),
                "steps": 8000 + i * 100,
                "avg_stress": 30 + i,
                "high_stress_pct": 5 + i,
                "body_battery_high": 80 + i,
                "body_battery_low": 30 + i,
                "rhr_bpm": 48,
            }
        )
    df = pd.DataFrame(rows)
    out = digest_features.shape_wellness_window(df, target, today_is_partial=True)
    for field in (
        "steps",
        "avg_stress",
        "high_stress_pct",
        "body_battery_high",
        "body_battery_low",
    ):
        assert out[field]["today"] is None, f"{field} today should be nulled"
        assert out[field]["delta"] is None, f"{field} delta should be nulled"
        assert out[field]["baseline_7d"] is not None, f"{field} baseline should remain"
    # Sleep-window and cardio metrics are unaffected by the partial flag.
    assert out["rhr_bpm"]["today"] == 48


def test_shape_wellness_window_keeps_intraday_when_full_day():
    target = date(2026, 5, 25)
    rows = [
        {"date": target - timedelta(days=i), "steps": 8000 if i else 5000}
        for i in range(8)
    ]
    df = pd.DataFrame(rows)
    out = digest_features.shape_wellness_window(df, target, today_is_partial=False)
    assert out["steps"]["today"] == 5000
    assert out["steps"]["delta"] is not None


def test_classify_magnitudes_skips_nulled_intraday_metrics():
    today_wellness = {
        "steps": {"today": None, "baseline_7d": 9000, "delta": None},
        "body_battery_low": {"today": None, "baseline_7d": 30, "delta": None},
    }
    out = digest_features.classify_magnitudes(today_wellness)
    assert out["steps"]["magnitude"] is None
    assert out["body_battery_low"]["magnitude"] is None


def test_build_last_7d_hard_day_count():
    target = date(2026, 5, 25)
    rows = [
        {
            "strava_activity_id": 1,
            "name": "Threshold",
            "sport_type": "Ride",
            "local_date": target - timedelta(days=2),
            "moving_time": 3600,
            "suffer_score": 95,
            "avg_hr": 150,
            "weighted_avg_watts": 200,
            "tss_proxy": 85,
            "tss_source": "power",
            "device_name": "Edge 530",
            "trainer": False,
        },
        {
            "strava_activity_id": 2,
            "name": "Z2 ride",
            "sport_type": "Ride",
            "local_date": target - timedelta(days=4),
            "moving_time": 5400,
            "suffer_score": 40,
            "avg_hr": 135,
            "weighted_avg_watts": 170,
            "tss_proxy": 65,
            "tss_source": "power",
            "device_name": "Edge 530",
            "trainer": False,
        },
    ]
    out = digest_features.build_last_7d(pd.DataFrame(rows), pd.DataFrame(), target)
    assert out["hard_day_count"] == 1
    assert out["total_tss"] == 150.0
    assert out["total_minutes"] == 150.0
    assert out["total_minutes_hm"] == "2h30m"
    assert out["weekly_tss_by_source"]["power"] == 150.0
    assert out["hardest_workout"]["name"] == "Threshold"


def test_build_yesterday_rest_when_no_workout():
    target = date(2026, 5, 25)
    out = digest_features.build_yesterday(pd.DataFrame(), pd.DataFrame(), target)
    assert out["trained"] is False
    assert out["intensity_label"] == "rest"
    assert out["date"] == (target - timedelta(days=1)).isoformat()


def test_classify_magnitudes_known_thresholds():
    today_wellness = {
        "rhr_bpm": {"today": 55, "baseline_7d": 47, "delta": 8.0},
        "sleep_minutes": {"today": 410, "baseline_7d": 460, "delta": -50},
        "deep_sleep_minutes": {"today": 60, "baseline_7d": 65, "delta": -5},
        "awake_minutes": {"today": 18, "baseline_7d": 16, "delta": 2},
        "waking_rr_brpm": {"today": 17.5, "baseline_7d": 14.5, "delta": 3.0},
        "body_battery_high": {"today": 60, "baseline_7d": 90, "delta": -30},
        "steps": {"today": 3000, "baseline_7d": 9000, "delta": -6000},
    }
    out = digest_features.classify_magnitudes(today_wellness)
    assert out["rhr_bpm"]["magnitude"] == "strong"
    assert out["sleep_minutes"]["magnitude"] == "significant"
    assert out["deep_sleep_minutes"]["magnitude"] == "noise"
    assert out["awake_minutes"]["magnitude"] == "noise"
    assert out["waking_rr_brpm"]["magnitude"] == "strong"
    assert out["body_battery_high"]["magnitude"] == "strong"
    assert out["steps"]["magnitude"] == "strong"


def test_classify_magnitudes_positive_direction_only_for_sleep_metrics():
    # Sleeping MORE than baseline is not concerning — should be noise.
    today_wellness = {
        "sleep_minutes": {"today": 520, "baseline_7d": 460, "delta": 60},
        "deep_sleep_minutes": {"today": 95, "baseline_7d": 60, "delta": 35},
        "sleep_score": {"today": 92, "baseline_7d": 78, "delta": 14},
    }
    out = digest_features.classify_magnitudes(today_wellness)
    assert out["sleep_minutes"]["magnitude"] == "noise"
    assert out["deep_sleep_minutes"]["magnitude"] == "noise"
    assert out["sleep_score"]["magnitude"] == "noise"


def test_classify_magnitudes_concerning_direction_only_for_cardio_metrics():
    # RHR dropping or breathing slowing is not concerning — should be noise.
    today_wellness = {
        "rhr_bpm": {"today": 42, "baseline_7d": 50, "delta": -8},
        "waking_rr_brpm": {"today": 12.0, "baseline_7d": 14.5, "delta": -2.5},
    }
    out = digest_features.classify_magnitudes(today_wellness)
    assert out["rhr_bpm"]["magnitude"] == "noise"
    assert out["waking_rr_brpm"]["magnitude"] == "noise"


def test_classify_magnitudes_negative_sleep_still_flagged():
    out = digest_features.classify_magnitudes(
        {"sleep_minutes": {"today": 380, "baseline_7d": 460, "delta": -80}}
    )
    assert out["sleep_minutes"]["magnitude"] == "significant"


def test_classify_magnitudes_null_when_data_missing():
    out = digest_features.classify_magnitudes(
        {"rhr_bpm": {"today": None, "baseline_7d": None, "delta": None}}
    )
    assert out["rhr_bpm"]["magnitude"] is None


def test_compute_health_state_red_on_strong():
    payload = {
        "today_wellness": {
            "rhr_bpm": {"magnitude": "strong"},
            "sleep_minutes": {"magnitude": "noise"},
        },
        "scores": {"illness_probability_score": 10},
        "load": {"load_ratio": 1.0},
        "garmin_status": {"hrv_status": "balanced"},
        "patterns": [],
        "last_7d": {"days_since_rest": 1},
    }
    assert digest_features.compute_health_state(payload) == "red"


def test_compute_health_state_red_on_pattern():
    payload = {
        "today_wellness": {},
        "scores": {"illness_probability_score": 10},
        "load": {"load_ratio": 1.0},
        "garmin_status": {"hrv_status": "balanced"},
        "patterns": [{"id": "pre_illness_signal", "note": "x"}],
        "last_7d": {"days_since_rest": 1},
    }
    assert digest_features.compute_health_state(payload) == "red"


def test_compute_health_state_yellow_on_two_mild():
    payload = {
        "today_wellness": {
            "sleep_minutes": {"magnitude": "mild"},
            "deep_sleep_minutes": {"magnitude": "mild"},
            "rhr_bpm": {"magnitude": "noise"},
        },
        "scores": {"illness_probability_score": 10},
        "load": {"load_ratio": 1.0},
        "garmin_status": {"hrv_status": "balanced"},
        "patterns": [],
        "last_7d": {"days_since_rest": 1},
    }
    assert digest_features.compute_health_state(payload) == "yellow"


def test_compute_health_state_green_when_all_noise():
    payload = {
        "today_wellness": {
            "rhr_bpm": {"magnitude": "noise"},
            "sleep_minutes": {"magnitude": "noise"},
        },
        "scores": {"illness_probability_score": 10},
        "load": {"load_ratio": 0.9},
        "garmin_status": {"hrv_status": "balanced"},
        "patterns": [],
        "last_7d": {"days_since_rest": 1},
    }
    assert digest_features.compute_health_state(payload) == "green"


def test_compute_health_state_training_fatigue_downgrades_red_to_yellow():
    payload = {
        "today_wellness": {
            "rhr_bpm": {
                "magnitude": "significant",
                "delta": 3,
                "confidence": "high",
            },
            "hrv_proxy_nocturnal": {
                "magnitude": "significant",
                "delta": -5,
                "confidence": "high",
            },
            "sleep_minutes": {"magnitude": "noise", "confidence": "high"},
        },
        "load": {"load_ratio": 1.0, "ctl": 40},
        "garmin_status": {"hrv_status": "low"},
        "patterns": [{"id": "pre_illness_signal", "note": "x"}],
        "last_7d": {"days_since_rest": 1},
        "training_load_context": {
            "expected_fatigue_today": {
                "level": "moderate",
                "expected_rhr_bump": 2.5,
                "source_session": "Hard ride 2026-06-03",
            }
        },
    }
    assert digest_features.compute_health_state(payload) == "yellow"


def test_magnitude_after_training_adjustment_rhr_within_bump():
    block = {"magnitude": "significant", "delta": 4}
    ef = {"level": "moderate", "expected_rhr_bump": 2.5}
    assert digest_features.magnitude_after_training_adjustment("rhr_bpm", block, ef) is None


def test_patterns_skip_pre_illness_during_expected_fatigue():
    flags = ["rhr_elevated_7d", "waking_rr_up", "sleep_fragmented"]
    patterns = digest_features.detect_patterns(
        flags,
        {"load_ratio": 1.0},
        "low",
        expected_fatigue={"level": "moderate"},
    )
    assert not any(p["id"] == "pre_illness_signal" for p in patterns)


def test_build_yesterday_uses_calendar_day_not_last_workout():
    target = date(2026, 5, 25)
    yesterday = target - timedelta(days=1)
    activities = pd.DataFrame(
        [
            {
                "strava_activity_id": 1,
                "name": "Endurance",
                "sport_type": "Ride",
                "local_date": yesterday,
                "moving_time": 5400,
                "tss_proxy": 65,
                "suffer_score": 40,
            },
            {
                "strava_activity_id": 2,
                "name": "Older hard ride",
                "sport_type": "Ride",
                "local_date": target - timedelta(days=3),
                "moving_time": 7200,
                "tss_proxy": 120,
                "suffer_score": 150,
            },
        ]
    )
    out = digest_features.build_yesterday(activities, pd.DataFrame(), target)
    assert out["trained"] is True
    assert out["name"] == "Endurance"
    assert out["intensity_label"] == "moderate"
    assert out["tss"] == 65.0
