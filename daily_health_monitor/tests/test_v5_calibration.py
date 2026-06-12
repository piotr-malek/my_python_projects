from analytics import digest_features


def test_compute_flags_uses_baseline_key():
    today_wellness = {
        "sleep_minutes": {"today": 300, "baseline": 450, "delta": -150},
        "steps": {"today": 2000, "baseline": 9000, "delta": -7000},
    }
    flags = digest_features.compute_flags(today_wellness, {}, 0)
    assert "sleep_short" in flags
    assert "activity_suppression" in flags


def test_health_state_ignores_load_ratio_when_ctl_low():
    payload = {
        "today_wellness": {},
        "scores": {"illness_probability_score": {"today": 10}},
        "load": {"load_ratio": 1.9, "ctl": 7.6},
        "patterns": [],
        "garmin_status": {},
        "last_7d": {},
    }
    assert digest_features.compute_health_state(payload, ctl_floor=30) == "green"
