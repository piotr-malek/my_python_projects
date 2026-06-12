"""v5 insight detection engine — ranked findings for digest narrative."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from analytics.derived_blocks import (
    add_sleep_quality_index,
    compute_sleep_debt,
    sleep_debt_should_surface,
)
from analytics.digest_features import detect_illness_watch, is_hard_day, safe_float
from analytics.digest_v4 import circular_mean_hhmm
from util.formatting import format_minutes_hm


@dataclass
class Finding:
    id: str
    category: str
    salience: float
    confidence: str
    timeframe: str
    summary: str
    supporting: Dict[str, Any] = field(default_factory=dict)
    suggested_theme: Optional[str] = None
    cadence: str = "daily"
    prospective: bool = False


CONF_WEIGHT = {"high": 1.0, "medium": 0.6, "low": 0.2}
METRIC_LABELS = {
    "sleep_minutes": "Total sleep",
    "deep_sleep_minutes": "Deep sleep",
    "rhr_bpm": "Resting HR",
    "waking_rr_brpm": "Waking respiration",
    "avg_stress": "Average stress",
    "body_battery_high": "Body battery peak",
    "rem_minutes": "REM sleep",
    "hrv_rmssd_ms": "HRV",
    "hrv_proxy_nocturnal": "Nocturnal recovery proxy",
}


def run_all_detectors(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    findings: List[Finding] = []
    findings.extend(_daily_detectors(ctx))
    cached = ctx.get("cached_weekly") or []
    if cached:
        for item in cached:
            if isinstance(item, dict):
                findings.append(_dict_to_finding(item))
    else:
        findings.extend(_weekly_correlations_legacy(ctx))
    return rank_findings(findings, ctx.get("recent_finding_categories") or [])


def _dict_to_finding(d: Dict[str, Any]) -> Finding:
    keys = set(asdict(Finding("", "", 0, "", "", "")).keys())
    return Finding(**{k: d[k] for k in keys if k in d})


def rank_findings(findings: List[Finding], recent_categories: List[str]) -> List[Dict[str, Any]]:
    scored = []
    for f in findings:
        if f.confidence == "low":
            continue
        novelty = 1.0
        if recent_categories.count(f.category) >= 2:
            novelty = 0.5
        conf_w = CONF_WEIGHT.get(f.confidence, 0.2)
        sal = f.salience * conf_w * novelty
        f2 = Finding(**{**asdict(f), "salience": round(sal, 1)})
        scored.append(f2)
    scored.sort(key=lambda x: x.salience, reverse=True)
    high = [f for f in scored if f.salience >= 45 and f.confidence != "low"]
    if not high:
        medium = [f for f in scored if f.confidence in ("high", "medium")][:1]
        return [_finding_dict(f) for f in medium[:3]]
    low_cap = [f for f in high if f.confidence == "low"]
    rest = [f for f in high if f.confidence != "low"]
    out = rest[:3]
    if not out and low_cap:
        out = low_cap[:1]
    return [_finding_dict(f) for f in out]


def _finding_dict(f: Finding) -> Dict[str, Any]:
    d = asdict(f)
    d["salience"] = round(float(f.salience), 1)
    return d


def _daily_detectors(ctx) -> List[Finding]:
    out = []
    f = _sleep_debt_finding(ctx)
    if f:
        out.append(f)
    f = _training_fatigue_finding(ctx)
    if f:
        out.append(f)
    f = _illness_watch_finding(ctx)
    if f:
        out.append(f)
    out.extend(_metric_streak_findings(ctx))
    out.extend(_baseline_drift_findings(ctx))
    f = _circadian_irregularity(ctx)
    if f:
        out.append(f)
    f = _positive_reinforcement(ctx)
    if f:
        out.append(f)
    out.extend(_stress_daily_findings(ctx))
    return out


def _sleep_debt_finding(ctx) -> Optional[Finding]:
    sleep_ctx = ctx.get("sleep_context") or {}
    typical = sleep_ctx.get("typical_duration_min")
    debt = ctx.get("sleep_debt") or compute_sleep_debt(
        ctx.get("sleep_df"), ctx["target"], typical
    )
    if not sleep_debt_should_surface(debt):
        return None
    hm = debt["balance_7d_hm"]
    typ_hm = sleep_ctx.get("typical_duration_hm") or format_minutes_hm(typical)
    salience = min(100, 50 + abs(debt.get("balance_7d_min", 0)) / 4)
    return Finding(
        id="sleep_debt_accumulating",
        category="sleep",
        salience=salience,
        confidence="high",
        timeframe="last 7 days",
        summary=(
            f"Your 7-day sleep balance is {hm} "
            f"({debt['nights_short_7d']} nights under your {typ_hm} norm). "
            "Worth protecting bedtime until it recovers."
        ),
        supporting={
            "balance_7d_hm": hm,
            "status": debt.get("status"),
            "nights_short": debt["nights_short_7d"],
        },
        suggested_theme="sleep_repair",
        cadence="daily",
    )


def _training_fatigue_finding(ctx) -> Optional[Finding]:
    from analytics.training_load import format_fatigue_source_phrase

    tlc = ctx.get("training_load_context") or {}
    ef = tlc.get("expected_fatigue_today") or {}
    level = ef.get("level") or "none"
    if level == "none":
        return None
    source_phrase = format_fatigue_source_phrase(ef)
    clears = ef.get("clears_by") or "soon"
    bump = ef.get("expected_rhr_bump")
    days_ago = ef.get("source_days_ago") or 1
    lag_phrase = "day-after" if days_ago == 1 else f"{days_ago}-day lag"
    salience = {"mild": 55, "moderate": 72, "high": 78}.get(level, 60)
    return Finding(
        id="training_fatigue_expected",
        category="training_response",
        salience=salience,
        confidence="high",
        timeframe=f"lag +{days_ago}",
        summary=(
            f"This morning's slightly raised RHR and lower recovery proxy are the expected "
            f"echo of {source_phrase} — within your normal {lag_phrase} range; "
            f"should clear by {clears}."
        ),
        supporting={
            "expected_rhr_bump": bump,
            "source": ef.get("source_session"),
            "source_days_ago": days_ago,
            "clears_by": clears,
            "level": level,
        },
        suggested_theme="recovery_movement",
        cadence="daily",
    )


def _illness_watch_finding(ctx) -> Optional[Finding]:
    watch = ctx.get("illness_watch")
    if not watch:
        return None
    sev = watch.get("severity", "low")
    return Finding(
        id="illness_watch",
        category="illness_watch",
        salience={"low": 70, "moderate": 85, "elevated": 95}.get(sev, 75),
        confidence="medium" if sev == "low" else "high",
        timeframe="this morning",
        summary=(
            "RHR and respiration are elevated beyond what recent training explains, "
            "with your overnight recovery proxy depressed — an early illness cluster worth watching."
        ),
        supporting=watch.get("supporting", {}),
        suggested_theme="illness_defense",
        cadence="daily",
    )


def _metric_streak_findings(ctx) -> List[Finding]:
    df = ctx.get("wellness_window_df")
    target = ctx["target"]
    if df is None or df.empty:
        return []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    metrics = [
        ("deep_sleep_minutes", "sleep", True),
        ("sleep_minutes", "sleep", True),
        ("rhr_bpm", "cardio", False),
        ("waking_rr_brpm", "respiration", False),
        ("rem_minutes", "sleep", True),
    ]
    out = []
    for col, cat, higher_better in metrics:
        if col not in df.columns:
            continue
        series = df[df["date"] <= target].sort_values("date").tail(7)
        vals = pd.to_numeric(series[col], errors="coerce").dropna().tolist()
        if len(vals) < 4:
            continue
        run_len, direction = _run_length(vals)
        if run_len < 3:
            continue
        move = vals[-1] - vals[-run_len]
        threshold = {"deep_sleep_minutes": 10, "sleep_minutes": 25, "rhr_bpm": 2, "waking_rr_brpm": 1.5, "rem_minutes": 8}.get(col, 5)
        if abs(move) < threshold:
            continue
        bad = (not higher_better and move > 0) or (higher_better and move < 0)
        cat_final = "positive" if not bad else cat
        label = METRIC_LABELS.get(col, col)
        dir_word = "fallen" if move < 0 else "risen"
        if higher_better:
            dir_word = "risen" if move > 0 else "fallen"
        theme = "sleep_repair" if bad and "sleep" in col else ("maintenance" if not bad else None)
        out.append(
            Finding(
                id=f"metric_streak_{col}",
                category=cat_final,
                salience=min(85, 50 + run_len * 8),
                confidence="high",
                timeframe=f"last {run_len} days",
                summary=f"{label} has {dir_word} {run_len} days running ({vals[-run_len]:.0f} → {vals[-1]:.0f}).",
                supporting={"metric": col, "run_len": run_len, "move": round(move, 1)},
                suggested_theme=theme,
                cadence="daily",
            )
        )
    return out[:2]


def _run_length(vals):
    if len(vals) < 2:
        return 0, 0
    direction = 1 if vals[-1] > vals[-2] else -1 if vals[-1] < vals[-2] else 0
    if direction == 0:
        return 0, 0
    run = 1
    for i in range(len(vals) - 2, -1, -1):
        if (direction > 0 and vals[i + 1] > vals[i]) or (direction < 0 and vals[i + 1] < vals[i]):
            run += 1
        else:
            break
    return run, direction


def _baseline_drift_findings(ctx) -> List[Finding]:
    df = ctx.get("wellness_window_df")
    if df is None or df.empty:
        return []
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    target = ctx["target"]
    out = []
    hrv_col = "hrv_rmssd_ms"
    if "hrv_rmssd_ms" in df.columns:
        s = pd.to_numeric(df[df["date"] < target]["hrv_rmssd_ms"], errors="coerce").dropna()
        if s.empty and "hrv_proxy_nocturnal" in df.columns:
            hrv_col = "hrv_proxy_nocturnal"
    for col, label in [
        ("rhr_bpm", "Resting HR"),
        ("waking_rr_brpm", "Waking respiration"),
        (hrv_col, "Nocturnal recovery proxy" if hrv_col == "hrv_proxy_nocturnal" else "HRV"),
    ]:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[df["date"] < target][col], errors="coerce").dropna()
        if len(s) < 14:
            continue
        m7 = float(s.tail(7).mean())
        m30 = float(s.tail(30).mean())
        drift = m7 - m30
        thr = {"rhr_bpm": 2.0, "waking_rr_brpm": 0.8, "hrv_rmssd_ms": 3.0, "hrv_proxy_nocturnal": 1.5}.get(col, 2.0)
        if abs(drift) <= thr:
            continue
        dir_word = "creeping up" if drift > 0 and col not in ("hrv_rmssd_ms", "hrv_proxy_nocturnal") else "creeping down"
        if col in ("hrv_rmssd_ms", "hrv_proxy_nocturnal"):
            dir_word = "creeping down" if drift < 0 else "creeping up"
        out.append(
            Finding(
                id=f"baseline_drift_{col}",
                category="recovery",
                salience=55,
                confidence="medium",
                timeframe="7d vs 30d baseline",
                summary=f"Your typical {label} has drifted: 7-day average {m7:.1f} vs 30-day {m30:.1f}. The baseline itself is {dir_word}.",
                supporting={"mean_7d": round(m7, 1), "mean_30d": round(m30, 1)},
                cadence="daily",
            )
        )
    return out[:1]


def _circadian_irregularity(ctx) -> Optional[Finding]:
    circ = ctx.get("circadian") or {}
    bt_stdev = circ.get("bedtime_stdev_min")
    if bt_stdev is None or bt_stdev < 60:
        return None
    best = circ.get("best_bedtime") or "23:15"
    hm = format_minutes_hm(bt_stdev)
    return Finding(
        id="circadian_irregularity",
        category="circadian",
        salience=min(75, 45 + bt_stdev // 2),
        confidence="high",
        timeframe="last 14 days",
        summary=(
            f"Your bedtime swung {hm} over the last 2 weeks. "
            f"Your best deep-sleep nights came when you were in bed by {best}."
        ),
        supporting={"bedtime_stdev_hm": hm, "best_bedtime": best},
        suggested_theme="sleep_repair",
        cadence="daily",
        prospective=True,
    )


def _positive_reinforcement(ctx) -> Optional[Finding]:
    tw = ctx.get("today_wellness") or {}
    proxy = tw.get("hrv_proxy_nocturnal") or {}
    rem = tw.get("rem_minutes") or {}
    rhr = tw.get("rhr_bpm") or {}
    rec = (ctx.get("scores") or {}).get("recovery_score") or {}

    proxy_pct = proxy.get("percentile_30d")
    if (
        proxy_pct is not None
        and proxy_pct >= 75
        and rem.get("direction") == "positive"
        and rem.get("magnitude") in ("mild", "significant", "strong")
    ):
        return Finding(
            id="positive_reinforcement",
            category="positive",
            salience=68,
            confidence="high",
            timeframe="last 14 days",
            summary=(
                f"REM has been above your norm and your overnight recovery proxy "
                f"is in the top quartile of the month (percentile {int(proxy_pct)}). "
                "Whatever you changed recently is working."
            ),
            supporting={"proxy_percentile": proxy_pct, "rem_today": rem.get("today")},
            suggested_theme="maintenance",
            cadence="daily",
        )
    if rec.get("direction_of_change") == "improving" and rec.get("typical_band") == "high":
        return Finding(
            id="positive_reinforcement",
            category="positive",
            salience=65,
            confidence="high",
            timeframe="last 7 days",
            summary="Recovery score is in your personal high band with an improving trajectory — whatever you've changed recently is working.",
            supporting={"recovery_today": rec.get("today")},
            suggested_theme="maintenance",
            cadence="daily",
        )
    if rem.get("direction") == "positive" and rem.get("is_max_7d"):
        return Finding(
            id="positive_reinforcement",
            category="positive",
            salience=60,
            confidence="high",
            timeframe="last 7 days",
            summary=f"REM sleep is the best of the week at {rem.get('today_hm', rem.get('today'))} — a good sign for focus today.",
            supporting={"rem_today": rem.get("today")},
            suggested_theme="maintenance",
            cadence="daily",
        )
    if rhr.get("direction") == "positive" and rhr.get("is_min_7d"):
        return Finding(
            id="positive_reinforcement",
            category="positive",
            salience=58,
            confidence="high",
            timeframe="last 7 days",
            summary=f"Resting HR is the lowest of the week at {rhr.get('today')} bpm — a calm cardiovascular read.",
            supporting={"rhr_today": rhr.get("today")},
            suggested_theme="maintenance",
            cadence="daily",
        )
    return None


def _weekly_correlations_legacy(ctx) -> List[Finding]:
    """Compute weekly correlation findings when enough history exists."""
    wellness = ctx.get("wellness") or {}
    sleep_df = wellness.get("raw_sleep", pd.DataFrame())
    stress_df = wellness.get("raw_stress", pd.DataFrame())
    hr_df = wellness.get("raw_heart_rate", pd.DataFrame())
    act_df = wellness.get("raw_activity_daily", pd.DataFrame())
    activities_df = ctx.get("activities_df")
    if sleep_df.empty:
        return []
    findings = []
    f = _stress_costs_deep_sleep(stress_df, sleep_df)
    if f:
        findings.append(f)
    f = _rhr_recovery_lag(activities_df, hr_df, ctx)
    if f:
        findings.append(f)
    f = _morning_activity_helps_sleep(act_df, sleep_df)
    if f:
        findings.append(f)
    f = _sleep_drives_next_day_calm(sleep_df, stress_df)
    if f:
        findings.append(f)
    f = _bedtime_regularity_drives_quality(sleep_df)
    if f:
        findings.append(f)
    return findings


def _sleep_quality_series(sleep_df: pd.DataFrame) -> pd.DataFrame:
    sl = sleep_df.copy()
    sl["date"] = pd.to_datetime(sl["date"]).dt.date
    if "deep_sleep_minutes" not in sl.columns and "deep_minutes" in sl.columns:
        sl["deep_sleep_minutes"] = sl["deep_minutes"]
    if "rem_minutes" not in sl.columns:
        sl["rem_minutes"] = pd.NA
    if "awake_minutes" not in sl.columns:
        sl["awake_minutes"] = 0
    return add_sleep_quality_index(sl)


def _morning_activity_helps_sleep(act_df, sleep_df) -> Optional[Finding]:
    if act_df is None or act_df.empty or sleep_df.empty:
        return None
    sl = _sleep_quality_series(sleep_df)
    act = act_df.copy()
    act["date"] = pd.to_datetime(act["date"]).dt.date
    act["steps"] = pd.to_numeric(act.get("steps"), errors="coerce")
    merged = act.merge(sl[["date", "sleep_quality_index"]], on="date", how="inner")
    merged = merged.dropna(subset=["steps", "sleep_quality_index"])
    if len(merged) < 30:
        return None
    active = merged[merged["steps"] >= merged["steps"].quantile(0.66)]
    quiet = merged[merged["steps"] <= merged["steps"].quantile(0.33)]
    if len(active) < 10 or len(quiet) < 10:
        return None
    r = merged["steps"].corr(merged["sleep_quality_index"])
    if r is None or r < 0.25:
        return None
    diff = float(active["sleep_quality_index"].mean() - quiet["sleep_quality_index"].mean())
    if diff <= 0.15:
        return None
    return Finding(
        id="morning_activity_helps_sleep",
        category="sleep",
        salience=52,
        confidence="medium" if len(merged) < 50 else "high",
        timeframe=f"{len(merged)}-day pattern",
        summary=(
            f"Your higher-quality sleep nights line up with more active days "
            f"(r={r:.2f}, {len(merged)} days). Movement is one of your reliable sleep levers."
        ),
        supporting={"r": round(r, 2), "n": len(merged)},
        suggested_theme="parasympathetic_morning",
        cadence="weekly_cached",
        prospective=True,
    )


def _sleep_drives_next_day_calm(sleep_df, stress_df) -> Optional[Finding]:
    if sleep_df.empty or stress_df.empty:
        return None
    sl = _sleep_quality_series(sleep_df)
    st = stress_df.copy()
    st["date"] = pd.to_datetime(st["date"]).dt.date
    st["avg_stress"] = pd.to_numeric(st["avg_stress"], errors="coerce")
    sl["next_date"] = sl["date"].apply(lambda d: d + timedelta(days=1))
    merged = sl.merge(
        st[["date", "avg_stress"]],
        left_on="next_date",
        right_on="date",
        how="inner",
        suffixes=("", "_stress"),
    )
    merged = merged.dropna(subset=["sleep_quality_index", "avg_stress"])
    if len(merged) < 30:
        return None
    r = merged["sleep_quality_index"].corr(merged["avg_stress"])
    if r is None or r > -0.25:
        return None
    return Finding(
        id="sleep_drives_next_day_calm",
        category="sleep",
        salience=50,
        confidence="medium",
        timeframe=f"{len(merged)}-day lag pattern",
        summary=(
            f"Better sleep quality tends to precede calmer next days "
            f"(r={r:.2f}, {len(merged)} nights). Protecting sleep pays off the following afternoon."
        ),
        supporting={"r": round(r, 2), "n": len(merged)},
        suggested_theme="sleep_repair",
        cadence="weekly_cached",
        prospective=True,
    )


def _bedtime_regularity_drives_quality(sleep_df) -> Optional[Finding]:
    if sleep_df.empty or "sleep_start" not in sleep_df.columns:
        return None
    sl = _sleep_quality_series(sleep_df)
    sl = sl[sl["sleep_start"].notna()].copy()
    if len(sl) < 30:
        return None
    sl["bed_min"] = sl["sleep_start"].apply(
        lambda t: pd.Timestamp(t).hour * 60 + pd.Timestamp(t).minute if pd.notna(t) else None
    )
    sl = sl.dropna(subset=["bed_min", "sleep_quality_index"])
    if len(sl) < 30:
        return None
    r = sl["bed_min"].corr(sl["sleep_quality_index"])
    if r is None or abs(r) < 0.2:
        return None
    bt_std = float(sl["bed_min"].std())
    if bt_std < 30:
        return None
    return Finding(
        id="bedtime_regularity_drives_quality",
        category="circadian",
        salience=48,
        confidence="medium",
        timeframe=f"{len(sl)}-day pattern",
        summary=(
            f"More regular bedtimes correlate with better sleep quality in your data "
            f"(bedtime swing ~{int(bt_std)} min). Consistency is a lever worth protecting."
        ),
        supporting={"bedtime_stdev_min": int(bt_std), "n": len(sl)},
        suggested_theme="sleep_repair",
        cadence="weekly_cached",
        prospective=True,
    )


def _stress_costs_deep_sleep(stress_df, sleep_df) -> Optional[Finding]:
    if stress_df.empty or sleep_df.empty:
        return None
    s = stress_df.copy()
    sl = sleep_df.copy()
    s["date"] = pd.to_datetime(s["date"]).dt.date
    sl["date"] = pd.to_datetime(sl["date"]).dt.date
    merged = s.merge(sl[["date", "deep_minutes"]], on="date", how="inner")
    merged["avg_stress"] = pd.to_numeric(merged["avg_stress"], errors="coerce")
    merged["deep_minutes"] = pd.to_numeric(merged["deep_minutes"], errors="coerce")
    merged = merged.dropna()
    if len(merged) < 30:
        return None
    r = merged["avg_stress"].corr(merged["deep_minutes"])
    if r is None or abs(r) < 0.35:
        return None
    high = merged[merged["avg_stress"] >= merged["avg_stress"].quantile(0.66)]["deep_minutes"].mean()
    low = merged[merged["avg_stress"] <= merged["avg_stress"].quantile(0.33)]["deep_minutes"].mean()
    penalty = int(round(low - high)) if low > high else int(round(high - low))
    return Finding(
        id="stress_costs_deep_sleep",
        category="stress",
        salience=76,
        confidence="high" if len(merged) >= 50 else "medium",
        timeframe=f"{len(merged)}-day pattern, same-night",
        summary=(
            f"On your higher-stress days, deep sleep drops about {penalty} min that night "
            f"(r={r:.2f}, {len(merged)} days). The link is consistent enough to plan around."
        ),
        supporting={"penalty_min": penalty, "r": round(r, 2), "n": len(merged)},
        suggested_theme="stress_reset",
        cadence="weekly_cached",
        prospective=True,
    )


def _rhr_recovery_lag(act_df, hr_df, ctx) -> Optional[Finding]:
    if act_df is None or act_df.empty or hr_df.empty:
        return None
    hr = hr_df.copy()
    hr["date"] = pd.to_datetime(hr["date"]).dt.date
    hr["rhr"] = pd.to_numeric(hr["rhr"], errors="coerce")
    base = float(hr["rhr"].dropna().tail(30).mean())
    act = act_df.copy()
    if "local_date" not in act.columns:
        act["local_date"] = pd.to_datetime(act.get("start_date_local", act.get("start_date"))).dt.date
    act["tss"] = pd.to_numeric(act.get("tss_proxy", 0), errors="coerce").fillna(0)
    hard_days = []
    for d, grp in act.groupby("local_date"):
        if is_hard_day(float(grp["tss"].sum()), None, 0):
            hard_days.append(d)
    if len(hard_days) < 8:
        return None
    best_lag, best_bump, best_n = None, 0.0, 0
    for lag in (1, 2):
        bumps = []
        for hd in hard_days:
            row = hr[hr["date"] == hd + timedelta(days=lag)]
            if row.empty:
                continue
            rhr = safe_float(row.iloc[0]["rhr"])
            if rhr is not None:
                bumps.append(rhr - base)
        if len(bumps) >= 8:
            mb = float(np.mean(bumps))
            if mb > best_bump:
                best_bump, best_lag, best_n = mb, lag, len(bumps)
    if best_lag is None or best_bump < 2:
        return None
    yesterday = ctx.get("yesterday") or {}
    today_rel = ""
    if yesterday.get("intensity_label") in ("hard", "very_hard"):
        if best_lag == 2:
            today_rel = " Yesterday was a hard session, so tomorrow is the watch morning, not today."
        else:
            today_rel = " Yesterday was a hard session — today is the watch morning."
    return Finding(
        id="rhr_recovery_lag",
        category="training_response",
        salience=82,
        confidence="high",
        timeframe=f"lag +{best_lag} days, {best_n}-session pattern",
        summary=(
            f"Your RHR typically rises ~{best_bump:.0f} bpm {best_lag} day(s) after a hard session, "
            f"not always the next morning ({best_n} sessions).{today_rel}"
        ),
        supporting={"rhr_bump": round(best_bump, 1), "lag": f"+{best_lag} days", "n": best_n},
        cadence="weekly_cached",
        prospective=True,
    )


def _stress_daily_findings(ctx) -> List[Finding]:
    out = []
    sc = ctx.get("stress_context") or {}
    complete = ctx.get("complete_df")
    f = _yesterday_work_stress(sc)
    if f:
        out.append(f)
    elif complete is not None and not complete.empty:
        f = _stress_timing_pattern(complete, ctx["target"])
        if f:
            out.append(f)
    if complete is not None and not complete.empty:
        f = _stress_trend_rising(complete, ctx["target"])
        if f:
            out.append(f)
        f = _stress_poor_daytime_recovery(complete, ctx["target"])
        if f:
            out.append(f)
    return out[:3]


def _yesterday_work_stress(sc: dict) -> Optional[Finding]:
    y = sc.get("yesterday") or {}
    intraday = sc.get("intraday_shape") or {}
    trend = sc.get("trend_7d") or {}
    overlap = sc.get("activity_overlap") or {}
    avg = y.get("avg_stress")
    vs = y.get("vs_baseline")
    work = (intraday.get("bands") or {}).get("work_09_18")
    if avg is None or vs is None:
        return None
    if y.get("magnitude") not in ("mild", "significant", "strong") and vs < 6:
        return None
    peak = intraday.get("peak_window") or "work hours"
    settled = intraday.get("settled_after")
    settled_txt = f", settled after {settled}" if settled else ""
    high_days = trend.get("high_stress_day_count_7d") or 0
    tail = f"; {high_days} of the last 7 days have been above your stress norm." if high_days >= 3 else "."
    base = int(avg - vs) if vs else None

    if overlap.get("peak_explained_by_exercise"):
        sessions = overlap.get("sessions") or []
        primary = max(
            sessions,
            key=lambda s: (s.get("overlap_minutes") or 0, s.get("duration_min") or 0),
            default=None,
        )
        if primary:
            name = primary.get("name") or "your session"
            intensity = primary.get("intensity_label", "moderate")
            intensity_txt = f"{intensity.replace('_', ' ')} " if intensity not in ("easy", "rest") else ""
            extra = ""
            if len(sessions) > 1:
                extra = f" (+ {len(sessions) - 1} other session{'s' if len(sessions) > 2 else ''})"
            summary = (
                f"Yesterday's stress peaked {peak} during your {intensity_txt}{name} "
                f"({primary.get('start_local')}–{primary.get('end_local')}){extra} — "
                f"expected during exercise, not work-induced stress. "
                f"Day avg {int(avg)} vs your {base} norm{settled_txt}{tail}"
            )
        else:
            summary = (
                f"Yesterday's stress peaked {peak} during logged exercise — "
                f"expected during hard efforts, not work-induced stress. "
                f"Day avg {int(avg)} vs your {base} norm{settled_txt}{tail}"
            )
        return Finding(
            id="yesterday_exercise_stress",
            category="training_response",
            salience=70,
            confidence="high",
            timeframe="yesterday",
            summary=summary,
            supporting={
                "peak_window": peak,
                "settled_after": settled,
                "activity_overlap": overlap,
                "base": base,
            },
            suggested_theme="recovery_movement",
            cadence="daily",
            prospective=False,
        )

    work_txt = f" (work hours avg {work})" if work else ""
    return Finding(
        id="yesterday_work_stress",
        category="stress",
        salience=78,
        confidence="high",
        timeframe="yesterday + 7-day trend",
        summary=(
            f"Yesterday's stress ran high{work_txt} — avg {int(avg)} vs your {base} norm, "
            f"peak {peak}{settled_txt}{tail}"
        ),
        supporting={
            "work_mean": work,
            "base": base,
            "peak_window": peak,
            "settled_after": settled,
            "high_days_7d": high_days,
        },
        suggested_theme="stress_reset",
        cadence="daily",
        prospective=False,
    )


def _stress_trend_rising(complete_df: pd.DataFrame, target) -> Optional[Finding]:
    df = complete_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    hist = df[df["date"] < target].tail(30)
    if len(hist) < 14:
        return None
    base = float(pd.to_numeric(hist["avg_stress_fullday"], errors="coerce").mean())
    avg7 = float(pd.to_numeric(hist.tail(7)["avg_stress_fullday"], errors="coerce").mean())
    drift = avg7 - base
    if drift < 4:
        return None
    streak = 0
    vals = pd.to_numeric(hist["avg_stress_fullday"], errors="coerce").dropna().tolist()
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] > vals[i - 1]:
            streak += 1
        else:
            break
    if streak < 2 and drift < 6:
        return None
    return Finding(
        id="stress_trend_rising",
        category="stress",
        salience=62,
        confidence="high",
        timeframe="7d vs 30d baseline",
        summary=(
            f"Your daily stress has run above your norm — 7-day avg {avg7:.0f} vs your {base:.0f} baseline"
            + (f", {streak} days rising." if streak >= 2 else ".")
        ),
        supporting={"avg7": round(avg7, 1), "base30": round(base, 1), "streak": streak},
        suggested_theme="stress_reset",
        cadence="daily",
    )


def _stress_timing_pattern(complete_df: pd.DataFrame, target) -> Optional[Finding]:
    df = complete_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    last7 = df[df["date"] < target].tail(7)
    if len(last7) < 5:
        return None
    work = pd.to_numeric(last7["stress_band_work"], errors="coerce")
    morning = pd.to_numeric(last7["stress_band_morning"], errors="coerce")
    evening = pd.to_numeric(last7["stress_band_evening"], errors="coerce")
    if work.dropna().empty:
        return None
    work_mean = float(work.mean())
    other_mean = float(pd.concat([morning, evening]).mean())
    if work_mean <= other_mean + 3:
        return None
    dom = int((work > morning.fillna(0)).sum() + (work > evening.fillna(0)).sum())
    if dom < 4:
        return None
    return Finding(
        id="stress_timing_pattern",
        category="stress",
        salience=58,
        confidence="high",
        timeframe="last 7 days",
        summary=(
            f"Your stress concentrates in work hours — averaging {work_mean:.0f} between 09:00 and 18:00 "
            f"vs {other_mean:.0f} the rest of the day, most days this week."
        ),
        supporting={"work_mean": round(work_mean, 1), "other_mean": round(other_mean, 1)},
        suggested_theme="stress_reset",
        cadence="daily",
    )


def _stress_poor_daytime_recovery(complete_df: pd.DataFrame, target) -> Optional[Finding]:
    df = complete_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    hist = df[df["date"] < target].tail(30)
    if len(hist) < 14:
        return None
    base = float(pd.to_numeric(hist["rest_pct_fullday"], errors="coerce").mean())
    rest7 = float(pd.to_numeric(hist.tail(7)["rest_pct_fullday"], errors="coerce").mean())
    if rest7 >= base - 3 and rest7 >= 15:
        return None
    return Finding(
        id="stress_poor_daytime_recovery",
        category="stress",
        salience=55,
        confidence="medium",
        timeframe="last 7 days",
        summary=(
            f"Little daytime recovery lately — only {rest7:.0f}% of the day in a rest state "
            f"vs your {base:.0f}% norm. Your system isn't getting downshift windows."
        ),
        supporting={"rest7": round(rest7, 1), "restbase": round(base, 1)},
        suggested_theme="parasympathetic_morning",
        cadence="daily",
    )
