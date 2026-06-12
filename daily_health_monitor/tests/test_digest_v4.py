from datetime import date, timedelta

import pandas as pd

from analytics import digest_v4


def test_circular_mean_hhmm_ignores_nat():
    assert digest_v4.circular_mean_hhmm([pd.NaT, "2026-05-28T04:04:00"]) == "04:00"


def test_compute_sleep_context_ignores_incomplete_rows():
    target = date(2026, 5, 31)
    rows = [
        {
            "date": (target - timedelta(days=i)).isoformat(),
            "sleep_start": pd.NaT,
            "sleep_end": pd.NaT,
            "sleep_minutes": 0,
        }
        if i == 1
        else {
            "date": (target - timedelta(days=i)).isoformat(),
            "sleep_start": f"2026-05-{31 - i:02d}T02:00:00",
            "sleep_end": f"2026-05-{31 - i + 1:02d}T08:00:00",
            "sleep_minutes": 360,
        }
        for i in range(1, 15)
    ]
    sleep_df = pd.DataFrame(rows)
    out = digest_v4.compute_sleep_context(sleep_df, target, {}, {})
    assert out["typical_bedtime"] == "02:00"
    assert out["typical_wake"] == "08:00"


def test_compute_sleep_context_caps_late_target_with_wake_goal():
    target = date(2026, 5, 28)
    rows = [
        {
            "date": (target - timedelta(days=i)).isoformat(),
            "sleep_start": f"2026-05-{27 - i:02d}T00:00:00",
            "sleep_end": f"2026-05-{28 - i:02d}T07:45:00",
            "sleep_minutes": 450,
        }
        for i in range(1, 15)
    ]
    sleep_df = pd.DataFrame(rows)
    out = digest_v4.compute_sleep_context(
        sleep_df,
        target,
        {"magnitude": "significant"},
        {"magnitude": "significant"},
    )
    # Typical bedtime 00:00 minus 45m pullback would be 23:15, but wake-goal
    # guardrail should choose the earlier bedtime that allows >= 7h sleep.
    assert out["typical_wake"] == "07:45"
    assert out["target_bedtime_tonight"] == "23:15"
