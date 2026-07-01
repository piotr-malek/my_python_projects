"""v6 correctness: net sleep debt, expected fatigue, illness gate."""

from datetime import date, timedelta

import pandas as pd

from analytics.derived_blocks import compute_sleep_debt, sleep_debt_should_surface
from analytics.digest_features import detect_illness_watch
from analytics import training_load


def _sleep_df(nights, typical=420, target=None):
    target = target or date(2026, 6, 4)
    rows = []
    for i, mins in enumerate(nights):
        # Garmin wake-date convention: last night is on `target`.
        d = target - timedelta(days=len(nights) - 1 - i)
        rows.append(
            {
                "date": d,
                "sleep_start": "23:00",
                "sleep_end": "07:00",
                "sleep_minutes": mins,
            }
        )
    return pd.DataFrame(rows)


def test_sleep_debt_credits_surplus():
    """Above-norm nights reduce balance; not a one-way ratchet."""
    target = date(2026, 6, 4)
    typical = 420
    # Six nights at norm, last night +60m surplus
    nights = [420] * 6 + [480]
    debt = compute_sleep_debt(_sleep_df(nights, typical, target), target, typical)
    assert debt["balance_7d_min"] == 60
    assert debt["status"] in ("surplus", "on_track")
    assert not sleep_debt_should_surface(debt)


def test_sleep_debt_notable_deficit_surfaces():
    target = date(2026, 6, 4)
    typical = 420
    nights = [250] * 7
    debt = compute_sleep_debt(_sleep_df(nights, typical, target), target, typical)
    assert debt["status"] == "notable_deficit"
    assert sleep_debt_should_surface(debt)


def test_sleep_debt_silent_when_last_night_good():
    target = date(2026, 6, 4)
    typical = 420
    nights = [390] * 6 + [450]
    debt = compute_sleep_debt(_sleep_df(nights, typical, target), target, typical)
    assert debt["last_night_vs_norm_min"] >= 0
    assert debt["status"] in ("mild_deficit", "on_track")
    assert not sleep_debt_should_surface(debt)


def test_sleep_debt_wake_date_uses_target_row_for_last_night():
    """Garmin stores last night on the wake-up date (digest `target`)."""
    target = date(2026, 6, 24)
    typical = 452
    nights = [435, 492, 477, 420, 417, 468, 219]
    debt = compute_sleep_debt(_sleep_df(nights, typical, target), target, typical)
    assert debt["last_night_vs_norm_min"] == 219 - typical
    assert debt["nights_short_7d"] >= 1
    assert sleep_debt_should_surface(debt)


def test_severe_short_sleep_finding():
    from analytics.insight_detectors import _severe_short_sleep_finding

    ctx = {
        "today_wellness": {
            "sleep_minutes": {
                "today": 219,
                "today_hm": "3h39m",
                "baseline_hm": "7h32m",
                "delta_pm": "-3h53m",
                "magnitude": "strong",
                "direction": "negative",
                "confidence": "high",
            }
        }
    }
    f = _severe_short_sleep_finding(ctx)
    assert f is not None
    assert f.id == "severe_short_sleep"
    assert f.salience >= 85


def test_expected_fatigue_after_hard_day():
    yesterday = {
        "trained": True,
        "name": "Intense ride",
        "date": "2026-06-03",
        "tss": 115,
        "intensity_label": "hard",
    }
    ef = training_load.compute_expected_fatigue(
        yesterday,
        0,
        {"rhr_recovery_days": 1},
        days_since_hard=0,
        target=date(2026, 6, 4),
    )
    assert ef["level"] in ("moderate", "high")
    assert ef["expected_rhr_bump"] >= 2.0
    assert ef["source_session"]


def test_expected_fatigue_day_two_after_rest_yesterday():
    """Hard ride 2 days ago still attributes mild fatigue even if yesterday was rest."""
    yesterday = {"trained": False, "date": "2026-06-07", "intensity_label": "rest"}
    two_days_ago = {
        "trained": True,
        "name": "Afternoon Ride",
        "date": "2026-06-06",
        "tss": 0.06,
        "suffer_score": 196.0,
        "intensity_label": "very_hard",
    }
    ef = training_load.compute_expected_fatigue(
        yesterday,
        0.06,
        {"rhr_recovery_days": 1},
        days_since_hard=2,
        target=date(2026, 6, 8),
        two_days_ago_session=two_days_ago,
    )
    assert ef["level"] == "mild"
    assert "Afternoon Ride" in ef["source_session"]
    assert ef["source_days_ago"] == 2


def test_expected_fatigue_source_phrase_two_days_ago():
    ef = {
        "source_name": "Lunch ride",
        "source_days_ago": 2,
        "source_date": "2026-06-08",
        "source_session": "Lunch ride 2026-06-08",
    }
    phrase = training_load.format_fatigue_source_phrase(ef)
    assert phrase == "your Lunch ride 2 days ago"
    assert "yesterday" not in phrase.lower()


def test_expected_fatigue_lag_two_uses_hard_session_from_two_days_ago():
    yesterday = {"trained": False, "date": "2026-06-09", "intensity_label": "rest"}
    two_days_ago = {
        "trained": True,
        "name": "Lunch ride",
        "date": "2026-06-08",
        "tss": 95,
        "suffer_score": 120,
        "intensity_label": "very_hard",
    }
    ef = training_load.compute_expected_fatigue(
        yesterday,
        95,
        {"rhr_recovery_days": 2},
        days_since_hard=2,
        target=date(2026, 6, 10),
        two_days_ago_session=two_days_ago,
    )
    assert ef["level"] in ("moderate", "high")
    assert ef["source_days_ago"] == 2
    assert "Lunch ride" in training_load.format_fatigue_source_phrase(ef)


def test_illness_watch_silent_when_training_explains():
    signals = {"rhr_z": 1.2, "proxy_z": -0.9, "waking_rr_z": 1.1, "sleep_rr_z": None}
    ef = {"level": "moderate", "expected_rhr_bump": 2.5}
    assert detect_illness_watch(signals, ef) is None


def test_illness_watch_fires_on_cluster():
    signals = {"rhr_z": 1.3, "proxy_z": -1.0, "waking_rr_z": 1.2, "sleep_rr_z": None}
    ef = {"level": "none"}
    out = detect_illness_watch(signals, ef)
    assert out is not None
    assert out["severity"] in ("low", "moderate", "elevated")
