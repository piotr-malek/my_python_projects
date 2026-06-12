"""Wellness flags and training-aware composite scores (v6)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from analytics import baselines
from analytics.derived_blocks import _z_vs_distribution
from analytics.digest_features import safe_float


def _series(df, col):
    if df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"])
    return d.sort_values("date").set_index("date")[col]


def _today_val(series, target):
    if series.empty:
        return None
    ts = pd.Timestamp(target)
    if ts in series.index:
        v = series.get(ts)
    else:
        v = series.iloc[-1]
    return safe_float(v) if pd.notna(v) else None


def _score_from_z(z, invert=False):
    """Map z to 0–100 (z=0 → 50)."""
    if z is None:
        return None
    if invert:
        z = -z
    return max(0.0, min(100.0, 50.0 + z * 17.5))


def _weighted_mean(components):
    """components: {name: (score 0-100 or None, weight)}"""
    avail = [(s, w) for s, w in components.values() if s is not None]
    if not avail:
        return 50.0
    wsum = sum(w for _, w in avail)
    return sum(s * w for s, w in avail) / wsum


def _last_night_sleep_score(sleep_today, typical_sleep_min):
    """Capped last-night vs norm; surplus nights don't inflate recovery."""
    if sleep_today is None or not typical_sleep_min:
        return None
    delta = sleep_today - typical_sleep_min
    if delta >= 0:
        z_equiv = min(1.5, delta / 45.0)
    else:
        z_equiv = max(-2.0, delta / 45.0)
    return _score_from_z(z_equiv)


def compute_wellness_flags(wellness, target):
    flags = []
    metrics = {}

    hr = _series(wellness.get("raw_heart_rate", pd.DataFrame()), "rhr")
    if not hr.empty:
        base7 = baselines.rolling_mean(hr, 7)
        today_rhr = hr.get(pd.Timestamp(target)) if pd.Timestamp(target) in hr.index else hr.iloc[-1]
        dev = baselines.deviation_today(float(today_rhr) if pd.notna(today_rhr) else None, base7)
        metrics["rhr_deviation"] = dev
        if base7 is not None:
            deviations = hr.tail(7) - base7
            if (deviations > 7).tail(2).sum() >= 2:
                flags.append("RHR elevated >7 bpm above 7d baseline for 2+ days")

    sleep = _series(wellness.get("raw_sleep", pd.DataFrame()), "sleep_minutes")
    awake = _series(wellness.get("raw_sleep", pd.DataFrame()), "awake_minutes")
    if not sleep.empty:
        base_sleep = baselines.rolling_mean(sleep, 7)
        today_sleep = sleep.iloc[-1] if len(sleep) else None
        if base_sleep is not None and pd.notna(today_sleep) and today_sleep < base_sleep - 90:
            flags.append("Sleep duration >90min below 7d baseline")
        if not awake.empty and not sleep.empty:
            frag = (awake / sleep.replace(0, pd.NA)).tail(7).mean()
            metrics["sleep_fragmentation"] = float(frag) if pd.notna(frag) else None
            if baselines.trend_increasing(awake / sleep.replace(0, pd.NA)):
                flags.append("Sleep fragmentation rising")

    stress = _series(wellness.get("raw_stress", pd.DataFrame()), "high_pct")
    if not stress.empty and baselines.trend_increasing(stress, 5):
        flags.append("High stress duration increasing over 5+ days")

    steps = _series(wellness.get("raw_activity_daily", pd.DataFrame()), "steps")
    if not steps.empty:
        med = baselines.rolling_mean(steps, 7)
        today_steps = steps.iloc[-1]
        if med is not None and pd.notna(today_steps) and today_steps / med < 0.6:
            flags.append("Activity suppression: steps <60% of 7d median")

    resp = _series(wellness.get("raw_respiration", pd.DataFrame()), "waking_rr")
    if not resp.empty:
        base = baselines.rolling_mean(resp, 7)
        metrics["respiration_deviation"] = baselines.deviation_today(float(resp.iloc[-1]), base)

    bb = wellness.get("raw_body_battery", pd.DataFrame())
    if not bb.empty and "charged" in bb.columns and "drained" in bb.columns:
        eff = (bb["charged"] / bb["drained"].replace(0, pd.NA)).tail(7)
        if baselines.trend_increasing(1 / eff.replace(0, pd.NA)):
            flags.append("Overnight body battery recharge efficiency declining")

    return flags, metrics


def composite_scores(
    wellness,
    target,
    load,
    wellness_flags=None,
    metrics=None,
    expected_fatigue=None,
):
    """v6 composites: training-adjusted, last-night sleep (not cumulative debt)."""
    target = target if isinstance(target, date) else date.fromisoformat(str(target)[:10])
    wellness_flags = wellness_flags or []
    metrics = metrics or {}
    load = load or {}
    expected_fatigue = expected_fatigue or {}

    hr = _series(wellness.get("raw_heart_rate", pd.DataFrame()), "rhr")
    sleep_df = wellness.get("raw_sleep", pd.DataFrame())
    sleep = _series(sleep_df, "sleep_minutes")
    deep = _series(sleep_df, "deep_minutes")
    rem = _series(sleep_df, "rem_minutes")
    stress = _series(wellness.get("raw_stress", pd.DataFrame()), "avg_stress")
    resp = wellness.get("raw_respiration", pd.DataFrame())
    waking_rr = _series(resp, "waking_rr") if not resp.empty else pd.Series(dtype=float)
    sleep_rr = _series(resp, "sleep_rr") if not resp.empty else pd.Series(dtype=float)
    bb = wellness.get("raw_body_battery", pd.DataFrame())
    hrv_df = wellness.get("raw_hrv", pd.DataFrame())

    rhr_today = _today_val(hr, target)
    rhr_hist = hr.tail(30)
    rhr_baseline = float(rhr_hist.mean()) if not rhr_hist.empty else None
    expected_bump = safe_float(expected_fatigue.get("expected_rhr_bump")) or 0.0
    rhr_adj_today = (
        (rhr_today - expected_bump) if rhr_today is not None and rhr_baseline is not None else None
    )
    rhr_z = _z_vs_distribution(rhr_adj_today, rhr_hist) if rhr_adj_today is not None else None

    deep_today = _today_val(deep, target)
    rem_today = _today_val(rem, target)
    deep_rem_today = (deep_today or 0) + (rem_today or 0) if deep_today is not None or rem_today is not None else None
    deep_rem_hist = (deep.fillna(0) + rem.fillna(0)).tail(14) if not deep.empty else pd.Series(dtype=float)
    deep_rem_z = _z_vs_distribution(deep_rem_today, deep_rem_hist) if deep_rem_today is not None else None

    typical_sleep = float(sleep.tail(14).mean()) if len(sleep.dropna()) >= 7 else None
    sleep_today = _today_val(sleep, target)
    sleep_component = _last_night_sleep_score(sleep_today, typical_sleep)

    proxy_today = None
    proxy_hist = pd.Series(dtype=float)
    if not hrv_df.empty and "nocturnal_proxy" in hrv_df.columns:
        hrv_df = hrv_df.copy()
        hrv_df["date"] = pd.to_datetime(hrv_df["date"]).dt.date
        row = hrv_df[hrv_df["date"] == target]
        if not row.empty:
            proxy_today = safe_float(row.iloc[0].get("nocturnal_proxy"))
        proxy_hist = pd.to_numeric(hrv_df["nocturnal_proxy"], errors="coerce").dropna()
    proxy_dip = safe_float(expected_fatigue.get("expected_proxy_dip")) or 0.0
    proxy_adj = (proxy_today - proxy_dip * 5.0) if proxy_today is not None else None
    proxy_z = _z_vs_distribution(proxy_adj, proxy_hist.tail(30)) if proxy_adj is not None else None

    bb_eff_today = None
    bb_eff_hist = pd.Series(dtype=float)
    if not bb.empty and "charged" in bb.columns:
        sm = sleep_df.copy()
        if not sm.empty and "sleep_minutes" in sm.columns:
            sm["date"] = pd.to_datetime(sm["date"]).dt.date
            bb2 = bb.copy()
            bb2["date"] = pd.to_datetime(bb2["date"]).dt.date
            merged = bb2.merge(sm[["date", "sleep_minutes"]], on="date", how="left")
            merged["eff"] = merged["charged"] / (merged["sleep_minutes"] / 60.0).replace(0, pd.NA)
            bb_eff_hist = pd.to_numeric(merged["eff"], errors="coerce").dropna().tail(30)
            today_row = merged[merged["date"] == target]
            if not today_row.empty:
                bb_eff_today = safe_float(today_row.iloc[0].get("eff"))
    bb_z = _z_vs_distribution(bb_eff_today, bb_eff_hist) if bb_eff_today is not None else None

    prior_stress = None
    if not stress.empty and len(stress.dropna()) >= 2:
        prior_stress = safe_float(stress.iloc[-2])
    prior_stress_z = _z_vs_distribution(prior_stress, stress.tail(30)) if prior_stress is not None else None
    rem_z = _z_vs_distribution(rem_today, rem.tail(14)) if rem_today is not None else None

    training_adjusted = (expected_fatigue.get("level") or "none") != "none"

    recovery = _weighted_mean(
        {
            "rhr": (_score_from_z(rhr_z, invert=True), 0.30),
            "proxy": (_score_from_z(proxy_z), 0.25),
            "bb_eff": (_score_from_z(bb_z), 0.15),
            "sleep": (sleep_component, 0.15),
            "deep_rem": (_score_from_z(deep_rem_z), 0.15),
        }
    )

    cognitive = _weighted_mean(
        {
            "rem": (_score_from_z(rem_z), 0.30),
            "sleep": (sleep_component, 0.20),
            "proxy": (_score_from_z(proxy_z), 0.20),
            "prior_stress": (_score_from_z(prior_stress_z, invert=True), 0.15),
            "rhr": (_score_from_z(rhr_z, invert=True), 0.15),
        }
    )

    burnout = min(
        100.0,
        len(wellness_flags) * 15 + (20 if (load.get("load_ratio") or 0) > 1.3 and (load.get("ctl") or 0) >= 30 else 0),
    )

    return {
        "recovery_score": round(recovery, 1),
        "burnout_risk_score": round(burnout, 1),
        "training_readiness_score": round(recovery, 1),
        "cognitive_readiness_score": round(cognitive, 1),
        "training_adjusted": training_adjusted,
        # Internal signals for illness_watch (not exposed in digest payload scores)
        "_rhr_z_residual": rhr_z,
        "_proxy_z_residual": proxy_z,
        "_waking_rr_z": _z_vs_distribution(_today_val(waking_rr, target), waking_rr.tail(30)),
        "_sleep_rr_z": _z_vs_distribution(_today_val(sleep_rr, target), sleep_rr.tail(30)),
    }


def illness_watch_signals(scores: dict) -> dict:
    """Extract internal z-scores used by detect_illness_watch."""
    return {
        "rhr_z": scores.get("_rhr_z_residual"),
        "proxy_z": scores.get("_proxy_z_residual"),
        "waking_rr_z": scores.get("_waking_rr_z"),
        "sleep_rr_z": scores.get("_sleep_rr_z"),
    }
