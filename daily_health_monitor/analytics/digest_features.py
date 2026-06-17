from datetime import timedelta
from typing import Optional

import pandas as pd

from strava import transforms
from util.formatting import format_minutes_hm

FLAGS_VOCAB = (
    "rhr_elevated_7d",
    "sleep_short",
    "sleep_fragmented",
    "waking_rr_up",
    "stress_high_5d",
    "body_battery_low",
    "load_ratio_high",
    "days_since_rest_long",
    "monotony_rising",
    "activity_suppression",
)

WELLNESS_FIELDS = (
    ("rhr_bpm", "bpm"),
    ("sleep_minutes", "minutes"),
    ("deep_sleep_minutes", "minutes"),
    ("rem_minutes", "minutes"),
    ("light_minutes", "minutes"),
    ("awake_minutes", "minutes"),
    ("sleep_score", "0-100"),
    ("sleep_stress", "0-100"),
    ("sleep_rr", "breaths/min"),
    ("waking_rr_brpm", "breaths/min"),
    ("hrv_rmssd_ms", "ms"),
    ("hrv_proxy_nocturnal", "index"),
    ("bb_recharge_efficiency", "ratio"),
    ("avg_stress", "0-100"),
    ("high_stress_pct", "% of day in high+very-high stress"),
    ("body_battery_high", "0-100"),
    ("body_battery_low", "0-100"),
    ("steps", "count"),
)

MAGNITUDE_LEVELS = ("noise", "mild", "significant", "strong")
ELEVATED_MAGNITUDES = frozenset({"mild", "significant", "strong"})
RISK_PATTERN_IDS = frozenset({"pre_illness_signal", "overreaching", "burnout_accumulation"})
TRAINING_EXPLAINABLE_FIELDS = frozenset(
    {
        "rhr_bpm",
        "hrv_proxy_nocturnal",
        "hrv_rmssd_ms",
        "body_battery_high",
        "body_battery_low",
        "bb_recharge_efficiency",
    }
)

# Metrics that accumulate or aggregate across the whole calendar day. They
# cannot be compared to full-day baselines until the day is complete. The
# digest typically runs in the morning, so for the target day these values
# are partial and we omit them from `today_wellness`.
INTRADAY_METRICS = frozenset(
    {"steps", "avg_stress", "high_stress_pct", "body_battery_high", "body_battery_low"}
)


def safe_float(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return float(value)


def intensity_label(tss, suffer, intensity_minutes, trained):
    if not trained:
        return "rest"
    tss = safe_float(tss) or 0.0
    suffer = safe_float(suffer) or 0.0
    im = safe_float(intensity_minutes) or 0.0
    if tss >= 100 or suffer >= 120:
        return "very_hard"
    if tss >= 70 or suffer >= 80:
        return "hard"
    if tss >= 40 or suffer >= 50 or im >= 45:
        return "moderate"
    return "easy"


def is_hard_day(tss, suffer, intensity_minutes):
    return (
        (safe_float(tss) or 0) >= 80
        or (safe_float(suffer) or 0) >= 80
    )


def infer_tss_source(row):
    src = row.get("tss_source")
    if src and not (isinstance(src, float) and pd.isna(src)):
        return src
    if pd.notna(row.get("weighted_avg_watts")) and row.get("weighted_avg_watts"):
        return "power"
    if pd.notna(row.get("suffer_score")) and row.get("suffer_score"):
        return "suffer_score"
    if pd.notna(row.get("avg_hr")) and row.get("avg_hr"):
        return "hr"
    return "none"


def shape_wellness_window(df, target, today_is_partial=True):
    """Convert a wellness-window DataFrame into {field: {today, baseline_7d, delta, unit}}.

    When `today_is_partial` is True (the default — the digest runs in the
    morning), intraday-accumulator metrics keep their baseline but their
    today/delta are nulled, since comparing a partial-day value to full-day
    baselines is meaningless. Pass `today_is_partial=False` when backfilling
    a historical date where today's data is already complete.
    """
    if df is None or df.empty:
        return {f: _empty_wellness_block(unit) for f, unit in WELLNESS_FIELDS}

    today_rows = df[df["date"] == target]
    baseline_rows = df[df["date"] < target]

    out = {}
    for field, unit in WELLNESS_FIELDS:
        if field not in df.columns:
            out[field] = _empty_wellness_block(unit)
            continue
        baseline_series = pd.to_numeric(baseline_rows[field], errors="coerce").dropna()
        baseline = round(float(baseline_series.mean()), 2) if not baseline_series.empty else None

        if today_is_partial and field in INTRADAY_METRICS:
            today_val = None
        else:
            today_val = (
                safe_float(today_rows.iloc[0][field]) if not today_rows.empty else None
            )

        delta = (
            round(today_val - baseline, 2)
            if today_val is not None and baseline is not None
            else None
        )
        out[field] = {
            "today": today_val,
            "baseline_7d": baseline,
            "delta": delta,
            "unit": unit,
        }
    return out


def _empty_wellness_block(unit):
    return {"today": None, "baseline_7d": None, "delta": None, "unit": unit}


def format_last_workout(row):
    """Normalize the raw row dict from Repository.get_last_workout_with_metrics."""
    if not row:
        return None

    def _pct(v):
        f = safe_float(v)
        return round(f * 100, 2) if f is not None else None

    raw_date = row.get("date")
    date_iso = raw_date.isoformat() if hasattr(raw_date, "isoformat") else (
        str(raw_date)[:10] if raw_date else None
    )

    return {
        "name": row.get("name"),
        "sport": row.get("sport"),
        "date": date_iso,
        "minutes": safe_float(row.get("minutes")),
        "minutes_hm": format_minutes_hm(safe_float(row.get("minutes"))),
        "tss": safe_float(row.get("tss")),
        "tss_source": infer_tss_source(row),
        "avg_hr": safe_float(row.get("avg_hr")),
        "weighted_avg_watts": safe_float(row.get("weighted_avg_watts")),
        "suffer_score": safe_float(row.get("suffer_score")),
        "hr_drift_pct": _pct(row.get("hr_drift_pct")),
        "aerobic_decoupling_pct": _pct(row.get("aerobic_decoupling_pct")),
        "efficiency_factor": safe_float(row.get("efficiency_factor")),
        "device": row.get("device"),
    }


def build_last_7d(activities_7d, intensity_minutes_df, target):
    out = {
        "total_minutes": 0.0,
        "total_tss": 0.0,
        "weekly_intensity_minutes": 0,
        "hard_day_count": 0,
        "days_since_rest": 0,
        "days_since_hard": None,
        "sport_mix_minutes": {},
        "weekly_tss_by_source": {"power": 0.0, "suffer_score": 0.0, "hr": 0.0, "none_count": 0},
        "hardest_workout": None,
    }

    intensity_by_date = {}
    if intensity_minutes_df is not None and not intensity_minutes_df.empty:
        for _, r in intensity_minutes_df.iterrows():
            intensity_by_date[r["date"]] = safe_float(r["intensity_minutes"]) or 0.0
        out["weekly_intensity_minutes"] = int(sum(intensity_by_date.values()))

    if activities_7d is None or activities_7d.empty:
        out["hard_day_count"] = 0
        out["days_since_rest"] = _days_since_rest(set(), target)
        out["total_minutes_hm"] = format_minutes_hm(out["total_minutes"])
        return out

    df = activities_7d.copy()
    df["minutes"] = pd.to_numeric(df.get("moving_time", 0), errors="coerce").fillna(0) / 60.0
    df["tss_proxy"] = pd.to_numeric(df["tss_proxy"], errors="coerce").fillna(0.0)
    df["suffer_score"] = pd.to_numeric(df["suffer_score"], errors="coerce")
    df["local_date"] = pd.to_datetime(df["local_date"]).dt.date

    out["total_minutes"] = round(float(df["minutes"].sum()), 1)
    out["total_minutes_hm"] = format_minutes_hm(out["total_minutes"])
    out["total_tss"] = round(float(df["tss_proxy"].sum()), 1)
    out["sport_mix_minutes"] = {
        k: round(float(v), 1)
        for k, v in df.groupby("sport_type")["minutes"].sum().items()
        if k
    }

    src_totals = {"power": 0.0, "suffer_score": 0.0, "hr": 0.0, "none_count": 0}
    for _, r in df.iterrows():
        src = infer_tss_source(r)
        if src == "none":
            src_totals["none_count"] += 1
        elif src in src_totals:
            src_totals[src] += float(r["tss_proxy"])
    out["weekly_tss_by_source"] = {
        k: (round(v, 1) if isinstance(v, float) else v) for k, v in src_totals.items()
    }

    top_idx = df["tss_proxy"].idxmax() if not df["tss_proxy"].empty else None
    if top_idx is not None and float(df.loc[top_idx, "tss_proxy"]) > 0:
        top = df.loc[top_idx]
        out["hardest_workout"] = {
            "name": top.get("name"),
            "date": top["local_date"].isoformat(),
            "tss": round(float(top["tss_proxy"]), 1),
            "sport": top.get("sport_type"),
        }

    by_day = df.groupby("local_date").agg(
        day_tss=("tss_proxy", "sum"),
        day_suffer=("suffer_score", "max"),
    )
    hard_days = [
        d
        for d, row in by_day.iterrows()
        if is_hard_day(row["day_tss"], row["day_suffer"], intensity_by_date.get(d, 0.0))
    ]
    out["hard_day_count"] = len(hard_days)

    trained_dates = set(df["local_date"].unique())
    out["days_since_rest"] = _days_since_rest(trained_dates, target)
    out["days_since_hard"] = _days_since(hard_days, target)
    return out


def _days_since_rest(trained_dates, target, cap=14):
    streak = 0
    day = target - timedelta(days=1)
    while day in trained_dates and streak < cap:
        streak += 1
        day -= timedelta(days=1)
    return streak


def _days_since(hard_days, target):
    if not hard_days:
        return None
    return (target - max(hard_days)).days


def compute_flags(
    today_wellness,
    load_metrics,
    days_since_rest,
    prior_rhr_delta=None,
    stress_high_flag=False,
):
    flags = []

    def _delta(field):
        return (today_wellness.get(field) or {}).get("delta")

    def _today(field):
        return (today_wellness.get(field) or {}).get("today")

    def _baseline(field):
        block = today_wellness.get(field) or {}
        return block.get("baseline") if block.get("baseline") is not None else block.get("baseline_7d")

    rhr_d = _delta("rhr_bpm")
    if rhr_d is not None and rhr_d >= 5 and (prior_rhr_delta is None or prior_rhr_delta >= 3):
        flags.append("rhr_elevated_7d")

    if (_delta("sleep_minutes") or 0) <= -45:
        flags.append("sleep_short")

    awake_d = _delta("awake_minutes")
    deep_d = _delta("deep_sleep_minutes")
    if (awake_d is not None and awake_d >= 15) or (deep_d is not None and deep_d <= -20):
        flags.append("sleep_fragmented")

    if (_delta("waking_rr_brpm") or 0) >= 1.5:
        flags.append("waking_rr_up")

    if (_delta("sleep_rr") or 0) >= 1.5:
        flags.append("sleep_rr_up")

    if stress_high_flag:
        flags.append("stress_high_5d")

    bb_today = _today("body_battery_high")
    bb_delta = _delta("body_battery_high")
    if bb_today is not None and bb_today < 70 and bb_delta is not None and bb_delta <= -15:
        flags.append("body_battery_low")

    if (load_metrics or {}).get("load_ratio", 0) >= 1.3:
        flags.append("load_ratio_high")

    if days_since_rest is not None and days_since_rest >= 6:
        flags.append("days_since_rest_long")

    if (load_metrics or {}).get("monotony", 0) >= 2.0:
        flags.append("monotony_rising")

    steps_today = _today("steps")
    steps_base = _baseline("steps")
    if (
        steps_today is not None
        and steps_base
        and steps_base > 0
        and steps_today / steps_base < 0.6
    ):
        flags.append("activity_suppression")

    return flags


def payload_expected_fatigue(payload):
    """Expected day-after training response from digest payload."""
    ef = payload.get("expected_fatigue")
    if isinstance(ef, dict) and ef.get("level") not in (None, "none"):
        return ef
    tlc = payload.get("training_load_context") or {}
    return tlc.get("expected_fatigue_today") or {}


def magnitude_after_training_adjustment(field, block, expected_fatigue):
    """Drop training-explained deviations so health_state matches coaching intent."""
    magnitude = block.get("magnitude")
    if not magnitude or magnitude == "noise":
        return magnitude

    level = (expected_fatigue or {}).get("level") or "none"
    if level == "none":
        return magnitude

    delta = block.get("delta")
    if field == "rhr_bpm" and delta is not None:
        bump = safe_float(expected_fatigue.get("expected_rhr_bump")) or 0.0
        if delta > 0 and delta <= bump + 3:
            return None

    if field in ("hrv_proxy_nocturnal", "hrv_rmssd_ms") and level in ("moderate", "high", "mild"):
        if magnitude in ("mild", "significant") and (delta is None or delta <= 0):
            return None

    if field in TRAINING_EXPLAINABLE_FIELDS and level in ("moderate", "high"):
        if magnitude == "mild":
            return None

    return magnitude


def detect_patterns(
    flags,
    load_metrics,
    hrv_status,
    hrv_source=None,
    wellness_window=None,
    ctl_floor=30,
    expected_fatigue=None,
):
    flagset = set(flags)
    patterns = []
    ef_level = (expected_fatigue or {}).get("level") or "none"

    if (
        ef_level not in ("moderate", "high")
        and "rhr_elevated_7d" in flagset
        and ("waking_rr_up" in flagset or "sleep_rr_up" in flagset)
        and ("sleep_fragmented" in flagset or "body_battery_low" in flagset)
    ):
        patterns.append(
            {
                "id": "pre_illness_signal",
                "note": "RHR, respiration and sleep/body-battery moving together",
            }
        )

    if "load_ratio_high" in flagset and (
        "rhr_elevated_7d" in flagset or "sleep_short" in flagset
    ):
        patterns.append(
            {"id": "overreaching", "note": "Acute load high with degrading recovery markers"}
        )

    ctl = (load_metrics or {}).get("ctl") or 0
    load_ratio = (load_metrics or {}).get("load_ratio", 0) or 0
    if "load_ratio_high" in flagset and ctl < ctl_floor:
        patterns = [p for p in patterns if p.get("id") != "overreaching"]

    if (
        "days_since_rest_long" in flagset
        and "monotony_rising" in flagset
        and "stress_high_5d" in flagset
    ):
        patterns.append(
            {
                "id": "burnout_accumulation",
                "note": "Sustained training without variability and rising stress",
            }
        )

    load_ratio = (load_metrics or {}).get("load_ratio", 0) or 0
    proxy_trend_ok = True
    if hrv_source == "nocturnal_proxy" and wellness_window is not None and not wellness_window.empty:
        from analytics.derived_blocks import proxy_trend_z_7d
        if "hrv_proxy_nocturnal" in wellness_window.columns:
            trend_z = proxy_trend_z_7d(wellness_window["hrv_proxy_nocturnal"])
            proxy_trend_ok = trend_z is None or trend_z >= -0.5
    hrv_ok = hrv_status == "balanced" or (
        hrv_source == "nocturnal_proxy" and proxy_trend_ok
    ) or hrv_source == "none"
    if (
        not flagset
        and ctl >= ctl_floor
        and 0.8 <= load_ratio <= 1.2
        and hrv_ok
    ):
        note = (
            "Wellness steady with in-range load ratio and stable recovery proxy trend"
            if hrv_source == "nocturnal_proxy"
            else "Wellness steady with balanced HRV and in-range load ratio"
            if hrv_status == "balanced"
            else "Wellness steady with in-range load ratio"
        )
        patterns.append(
            {
                "id": "healthy_adaptation",
                "note": note,
            }
        )

    return patterns


def detect_partial_load(activities_7d, garmin_devices):
    if activities_7d is None or activities_7d.empty:
        return False
    for _, r in activities_7d.iterrows():
        if bool(r.get("trainer")):
            return True
        if not transforms.is_garmin_device(r.get("device_name"), garmin_devices):
            return True
    return False


def stress_high_5d(wellness_dict):
    stress_df = wellness_dict.get("raw_stress", pd.DataFrame())
    if stress_df.empty or "high_pct" not in stress_df.columns:
        return False
    s = stress_df.copy()
    s["date"] = pd.to_datetime(s["date"])
    s = s.sort_values("date").tail(10)
    if len(s) < 6:
        return False
    baseline = float(s.head(len(s) - 5)["high_pct"].mean())
    return bool((s.tail(5)["high_pct"] > baseline).sum() >= 5)


def prior_day_rhr_delta(wellness_dict, target):
    hr_df = wellness_dict.get("raw_heart_rate", pd.DataFrame())
    if hr_df.empty or "rhr" not in hr_df.columns:
        return None
    df = hr_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    prior_day = target - timedelta(days=1)
    prior_row = df[df["date"] == prior_day]
    if prior_row.empty:
        return None
    baseline_window = df[
        (df["date"] < prior_day) & (df["date"] >= prior_day - timedelta(days=7))
    ]
    baseline_rhr = pd.to_numeric(baseline_window["rhr"], errors="coerce").dropna()
    if baseline_rhr.empty:
        return None
    today_val = safe_float(prior_row.iloc[0]["rhr"])
    if today_val is None:
        return None
    return today_val - float(baseline_rhr.mean())


def _band(value, bands):
    """bands: tuple of (upper_exclusive, label). The last label is used for >= last threshold."""
    for upper, label in bands[:-1]:
        if value < upper:
            return label
    return bands[-1][1]


def _classify_positive(delta, thresholds):
    """Only positive deltas concerning. Negative deltas always 'noise'."""
    if delta is None:
        return None
    if delta < thresholds[0]:
        return "noise"
    return _band(
        delta,
        (
            (thresholds[1], "mild"),
            (thresholds[2], "significant"),
            (None, "strong"),
        ),
    )


def _classify_negative(delta, thresholds):
    """Only negative deltas concerning (e.g. body battery dropping). thresholds are negative."""
    if delta is None:
        return None
    if delta > thresholds[0]:
        return "noise"
    if delta > thresholds[1]:
        return "mild"
    if delta > thresholds[2]:
        return "significant"
    return "strong"


def _classify_steps(delta, baseline):
    """Drops in steps are concerning when baseline is large enough to be meaningful."""
    if delta is None or baseline is None or baseline < 1000:
        return "noise"
    ratio = delta / baseline
    if ratio > -0.2:
        return "noise"
    if ratio > -0.4:
        return "mild"
    if ratio > -0.6:
        return "significant"
    return "strong"


# Magnitude classifiers escalate only in the concerning direction. Examples:
#   - RHR drifting *up* matters (illness / poor recovery). RHR drifting *down* is fine.
#   - Sleep, deep sleep, sleep score only matter when they fall *below* baseline.
#   - Waking respiration only matters when it rises (illness signal).
MAGNITUDE_CLASSIFIERS = {
    "rhr_bpm": lambda block: _classify_positive(_effective_delta(block, "rhr_bpm"), (2, 5, 8)),
    "sleep_minutes": lambda block: _classify_negative(_effective_delta(block, "sleep_minutes"), (-25, -50, -90)),
    "deep_sleep_minutes": lambda block: _classify_negative(_effective_delta(block, "deep_sleep_minutes"), (-10, -25, -40)),
    "rem_minutes": lambda block: _classify_negative(_effective_delta(block, "rem_minutes"), (-10, -25, -40)),
    "light_minutes": lambda block: _classify_negative(_effective_delta(block, "light_minutes"), (-15, -30, -50)),
    "awake_minutes": lambda block: _classify_positive(_effective_delta(block, "awake_minutes"), (10, 25, 45)),
    "sleep_score": lambda block: _classify_negative(_effective_delta(block, "sleep_score"), (-7, -15, -25)),
    "sleep_stress": lambda block: _classify_positive(_effective_delta(block, "sleep_stress"), (5, 12, 20)),
    "sleep_rr": lambda block: _classify_positive(_effective_delta(block, "sleep_rr"), (0.7, 1.5, 2.5)),
    "waking_rr_brpm": lambda block: _classify_positive(_effective_delta(block, "waking_rr_brpm"), (0.7, 1.5, 2.5)),
    "hrv_rmssd_ms": lambda block: _classify_hrv_pct(block),
    "hrv_proxy_nocturnal": lambda block: _classify_hrv_pct(block, higher_is_better=True),
    "bb_recharge_efficiency": lambda block: _classify_negative(_effective_delta(block, "bb_recharge_efficiency"), (-0.05, -0.12, -0.2)),
    "avg_stress": lambda block: _classify_positive(_effective_delta(block, "avg_stress"), (5, 12, 20)),
    "high_stress_pct": lambda block: _classify_positive(_effective_delta(block, "high_stress_pct"), (3, 8, 15)),
    "body_battery_high": lambda block: _classify_negative(_effective_delta(block, "body_battery_high"), (-8, -18, -30)),
    "body_battery_low": lambda block: _classify_negative(_effective_delta(block, "body_battery_low"), (-8, -15, -25)),
    "steps": lambda block: _classify_steps(block.get("delta"), block.get("baseline") or block.get("baseline_7d")),
}

RESOLUTION_FLOOR = {"waking_rr_brpm": 1.0, "sleep_rr": 1.0}
RESOLUTION_K = 2.0


def _effective_delta(block, field):
    """Widen noise band when historical IQR is below measurement resolution."""
    delta = block.get("delta")
    if delta is None:
        return None
    typical = block.get("typical_range")
    floor = RESOLUTION_FLOOR.get(field, 0)
    if typical and len(typical) == 2 and floor > 0:
        iqr = abs(typical[1] - typical[0])
        if iqr < floor and abs(delta) < max(floor * RESOLUTION_K, floor):
            return None
    return delta


def _classify_hrv_pct(block, higher_is_better=False):
    delta = block.get("delta")
    baseline = block.get("baseline")
    if delta is None or baseline is None or baseline == 0:
        return None
    pct = delta / baseline
    if higher_is_better:
        if pct <= 0.1:
            return "noise"
        if pct < 0.2:
            return "mild"
        if pct < 0.35:
            return "significant"
        return "strong"
    if pct >= -0.1:
        return "noise"
    if pct > -0.2:
        return "mild"
    if pct > -0.35:
        return "significant"
    return "strong"


def classify_magnitudes(today_wellness):
    """Attach a `magnitude` field to each metric block. Mutates in place and returns it."""
    for field, block in (today_wellness or {}).items():
        if not isinstance(block, dict):
            continue
        if block.get("today") is None or block.get("baseline_7d") is None:
            block["magnitude"] = None
            continue
        classifier = MAGNITUDE_CLASSIFIERS.get(field)
        block["magnitude"] = classifier(block) if classifier else None
    return today_wellness


def detect_illness_watch(
    illness_signals: dict,
    expected_fatigue: dict,
) -> Optional[dict]:
    """
    Gated illness cluster — silent unless genuine training-adjusted signature fires.
    Returns {severity, supporting} or None.
    """
    if (expected_fatigue or {}).get("level") in ("moderate", "high"):
        return None

    rhr_z = safe_float((illness_signals or {}).get("rhr_z"))
    proxy_z = safe_float((illness_signals or {}).get("proxy_z"))
    waking_z = safe_float((illness_signals or {}).get("waking_rr_z"))
    sleep_rr_z = safe_float((illness_signals or {}).get("sleep_rr_z"))

    if rhr_z is None or rhr_z < 1.0:
        return None
    if waking_z is None or waking_z < 1.0:
        if sleep_rr_z is None or sleep_rr_z < 1.0:
            return None
    if proxy_z is None or proxy_z > -0.75:
        return None

    strength = max(rhr_z, waking_z or 0, sleep_rr_z or 0, abs(proxy_z))
    if strength >= 2.0:
        severity = "elevated"
    elif strength >= 1.5:
        severity = "moderate"
    else:
        severity = "low"

    return {
        "severity": severity,
        "supporting": {
            "rhr_z": round(rhr_z, 2),
            "proxy_z": round(proxy_z, 2),
            "waking_rr_z": round(waking_z, 2) if waking_z is not None else None,
            "sleep_rr_z": round(sleep_rr_z, 2) if sleep_rr_z is not None else None,
        },
    }


def compute_health_state(payload, ctl_floor=30):
    """Walk the rule list red → yellow → green; first match wins."""
    wellness = payload.get("today_wellness", {}) or {}
    expected_fatigue = payload_expected_fatigue(payload)
    ef_level = expected_fatigue.get("level") or "none"

    magnitudes = []
    for field, block in wellness.items():
        if not isinstance(block, dict):
            continue
        if block.get("confidence") == "low":
            continue
        m = magnitude_after_training_adjustment(field, block, expected_fatigue)
        if m:
            magnitudes.append(m)
    n_strong = sum(1 for m in magnitudes if m == "strong")
    n_significant = sum(1 for m in magnitudes if m == "significant")
    n_mild = sum(1 for m in magnitudes if m == "mild")

    insight_ids = {
        i.get("id")
        for i in (payload.get("insights") or [])
        if isinstance(i, dict)
    }
    illness_watch = "illness_watch" in insight_ids

    load = payload.get("load") or {}
    load_ratio = load.get("load_ratio") or 0
    ctl = load.get("ctl") or 0
    hrv = (payload.get("garmin_status", {}) or {}).get("hrv_status")
    pattern_ids = {p.get("id") for p in payload.get("patterns", []) or []}
    risk_patterns = pattern_ids & RISK_PATTERN_IDS
    if ef_level in ("moderate", "high") and not illness_watch:
        risk_patterns -= {"pre_illness_signal"}

    load_overreach = load_ratio >= 1.3 and ctl >= ctl_floor
    hrv_red = hrv in {"low", "poor"}
    if hrv_red and ef_level in ("moderate", "high") and not illness_watch:
        hrv_red = False

    red = (
        n_strong >= 1
        or n_significant >= 2
        or illness_watch
        or load_overreach
        or bool(risk_patterns)
        or hrv_red
    )
    if red:
        if (
            ef_level in ("moderate", "high")
            and not illness_watch
            and not load_overreach
        ):
            return "yellow"
        return "red"

    days_since_rest = (payload.get("last_7d", {}) or {}).get("days_since_rest") or 0
    load_yellow = 1.2 <= load_ratio < 1.3 and ctl >= ctl_floor
    if (
        n_significant == 1
        or n_mild >= 2
        or hrv == "unbalanced"
        or hrv in {"low", "poor"}
        or load_yellow
        or days_since_rest >= 7
    ):
        return "yellow"

    return "green"


def _intensity_minutes_on(intensity_minutes_df, day):
    if intensity_minutes_df is None or intensity_minutes_df.empty:
        return 0.0
    rows = intensity_minutes_df[intensity_minutes_df["date"] == day]
    if rows.empty:
        return 0.0
    return safe_float(rows.iloc[0]["intensity_minutes"]) or 0.0


def build_day_session(activities_df, intensity_minutes_df, day, stream_map=None, ftp=None, threshold_hr=None):
    """Summarize training on a calendar day (for fatigue attribution)."""
    from analytics import training_load

    empty = {
        "trained": False,
        "date": day.isoformat(),
        "name": None,
        "sport": None,
        "tss": None,
        "suffer_score": None,
        "intensity_minutes": _intensity_minutes_on(intensity_minutes_df, day),
        "intensity_label": "rest",
    }
    if activities_df is None or activities_df.empty:
        return empty

    df = activities_df.copy()
    df["local_date"] = pd.to_datetime(df["local_date"]).dt.date
    grp = df[df["local_date"] == day]
    if grp.empty:
        return empty

    im = empty["intensity_minutes"]
    agg = grp.sort_values("tss_proxy", ascending=False).iloc[0]
    day_tss = 0.0
    max_suffer = 0.0
    day_streams = None
    for _, act_row in grp.iterrows():
        aid = int(act_row["strava_activity_id"])
        streams = stream_map.get(aid) if stream_map else None
        if streams and day_streams is None:
            day_streams = streams
        day_tss += training_load.activity_tss_with_source(act_row, streams, ftp, threshold_hr)[0]
        suffer = safe_float(act_row.get("suffer_score")) or 0.0
        max_suffer = max(max_suffer, suffer)

    return {
        "trained": True,
        "date": day.isoformat(),
        "name": agg.get("name"),
        "sport": agg.get("sport_type"),
        "tss": round(day_tss, 1),
        "suffer_score": max_suffer or None,
        "intensity_minutes": im,
        "intensity_label": intensity_label(day_tss, max_suffer, im, True),
    }


def build_yesterday(activities_df, intensity_minutes_df, target, stream_map=None, ftp=None, threshold_hr=None):
    """Training on the calendar day before target (not the most recent workout)."""
    return build_day_session(
        activities_df,
        intensity_minutes_df,
        target - timedelta(days=1),
        stream_map=stream_map,
        ftp=ftp,
        threshold_hr=threshold_hr,
    )
