from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from analytics.digest_features import intensity_label, is_hard_day, safe_float


def normalized_power(watts):
    if len(watts) < 30:
        return None
    roll = pd.Series(watts).rolling(30, min_periods=15).mean().dropna()
    if roll.empty:
        return None
    return float((roll**4).mean() ** 0.25)


def tss_from_power(np_val, duration_sec, ftp):
    """TrainingPeaks-style TSS: (seconds × NP × IF) / (FTP × 3600) × 100."""
    if not ftp or duration_sec <= 0 or not np_val:
        return 0.0
    intensity_factor = np_val / ftp
    return float((duration_sec * np_val * intensity_factor) / (ftp * 3600.0) * 100.0)


def cached_tss_untrusted(row, cached_tss, cached_source) -> bool:
    """Detect power TSS values that were stored with the old 3600×-too-small formula."""
    if cached_source != "power":
        return False
    duration = safe_float(row.get("moving_time")) or 0.0
    if duration < 1800:
        return False
    tss = safe_float(cached_tss) or 0.0
    watts = safe_float(row.get("weighted_avg_watts"))
    suffer = safe_float(row.get("suffer_score")) or 0.0
    if watts and watts >= 120 and tss < 25:
        return True
    if suffer >= 80 and tss < 40:
        return True
    return False


def activity_needs_tss_recompute(row) -> bool:
    if pd.isna(row.get("tss_proxy")):
        return True
    src = row.get("tss_source")
    if src is None or (isinstance(src, float) and pd.isna(src)):
        src = "cached"
    return cached_tss_untrusted(row, row.get("tss_proxy"), src)


def tss_from_hr(avg_hr, duration_sec, threshold_hr):
    if not threshold_hr or duration_sec <= 0 or not avg_hr:
        return 0.0
    hours = duration_sec / 3600
    return float(hours * (avg_hr / threshold_hr) ** 2 * 100)


def activity_tss_with_source(row, streams, ftp, threshold_hr):
    """Return (tss, source) where source is one of: cached, power, suffer_score, hr, none."""
    duration = row.get("moving_time") or 0
    if pd.notna(row.get("tss_proxy")):
        cached_source = row.get("tss_source") if "tss_source" in row.index else None
        cached_source = cached_source or "cached"
        if not cached_tss_untrusted(row, row["tss_proxy"], cached_source):
            return float(row["tss_proxy"]), cached_source

    if streams:
        watts = streams.get("watts")
        if watts is not None and len(watts) >= 30:
            np_val = normalized_power(np.array(watts, dtype=float))
            if np_val:
                return tss_from_power(np_val, duration, ftp), "power"
    if pd.notna(row.get("suffer_score")) and row.get("suffer_score"):
        return float(row["suffer_score"]), "suffer_score"
    if pd.notna(row.get("avg_hr")):
        return tss_from_hr(float(row["avg_hr"]), duration, threshold_hr), "hr"
    if pd.notna(row.get("tss_proxy")) and not cached_tss_untrusted(
        row, row["tss_proxy"], row.get("tss_source") or "cached"
    ):
        return float(row["tss_proxy"]), row.get("tss_source") or "cached"
    return 0.0, "none"


def activity_tss(row, streams, ftp, threshold_hr):
    return activity_tss_with_source(row, streams, ftp, threshold_hr)[0]


def daily_tss_series(activities, stream_map, ftp, threshold_hr):
    by_date = {}
    for _, row in activities.iterrows():
        aid = int(row["strava_activity_id"])
        tss = activity_tss(row, stream_map.get(aid), ftp, threshold_hr)
        d = pd.to_datetime(row["start_date"]).date()
        by_date[d] = by_date.get(d, 0) + tss
    if not by_date:
        return pd.Series(dtype=float)
    return pd.Series(by_date).sort_index()


def ewma(series, tau_days):
    if series.empty:
        return 0.0
    alpha = 2 / (tau_days + 1)
    val = series.iloc[0]
    for v in series.iloc[1:]:
        val = alpha * v + (1 - alpha) * val
    return float(val)


def compute_load_metrics(daily_tss):
    if daily_tss.empty:
        return {"atl": 0, "ctl": 0, "load_ratio": 0, "monotony": 0, "strain": 0}
    atl = ewma(daily_tss.tail(14), 7)
    ctl = ewma(daily_tss.tail(60), 42)
    load_ratio = atl / ctl if ctl > 0 else 0
    week = daily_tss.tail(7)
    monotony = float(week.mean() / week.std()) if week.std() and week.std() > 0 else 0
    strain = float(week.sum() * monotony)
    return {
        "atl": round(atl, 1),
        "ctl": round(ctl, 1),
        "load_ratio": round(load_ratio, 2),
        "monotony": round(monotony, 2),
        "strain": round(strain, 1),
    }


def _session_row(
    row,
    intensity_by_date: Optional[Dict[date, float]] = None,
    streams=None,
    ftp: Optional[float] = None,
    threshold_hr: Optional[float] = None,
) -> Dict[str, Any]:
    local_date = row.get("local_date")
    if hasattr(local_date, "isoformat"):
        date_iso = local_date.isoformat()
    else:
        date_iso = str(local_date)[:10] if local_date else None
    tss, _ = activity_tss_with_source(row, streams, ftp, threshold_hr)
    suffer = safe_float(row.get("suffer_score")) or 0.0
    im = 0.0
    if intensity_by_date and local_date in intensity_by_date:
        im = intensity_by_date[local_date]
    label = intensity_label(tss, suffer, im, True)
    src = row.get("tss_source")
    if src is None or (isinstance(src, float) and pd.isna(src)):
        src = "cached" if tss else "none"
    return {
        "date": date_iso,
        "name": row.get("name"),
        "sport": row.get("sport_type"),
        "intensity_label": label,
        "tss": round(tss, 1) if tss else None,
        "tss_source": str(src),
    }


def build_recent_sessions(
    activities_7d: pd.DataFrame,
    intensity_minutes_df: Optional[pd.DataFrame],
    target: date,
    limit: int = 7,
    stream_map: Optional[dict] = None,
    ftp: Optional[float] = None,
    threshold_hr: Optional[float] = None,
) -> list:
    """Last N training days before target, newest first."""
    if activities_7d is None or activities_7d.empty:
        return []
    intensity_by_date = {}
    if intensity_minutes_df is not None and not intensity_minutes_df.empty:
        for _, r in intensity_minutes_df.iterrows():
            intensity_by_date[r["date"]] = safe_float(r.get("intensity_minutes")) or 0.0

    df = activities_7d.copy()
    df["local_date"] = pd.to_datetime(df["local_date"]).dt.date
    by_day = []
    for d, grp in df.groupby("local_date"):
        if d >= target:
            continue
        agg = grp.sort_values("tss_proxy", ascending=False).iloc[0]
        agg = agg.copy()
        day_tss = 0.0
        day_streams = None
        for _, act_row in grp.iterrows():
            aid = int(act_row["strava_activity_id"])
            streams = stream_map.get(aid) if stream_map else None
            if streams and day_streams is None:
                day_streams = streams
            day_tss += activity_tss_with_source(act_row, streams, ftp, threshold_hr)[0]
        agg["tss_proxy"] = day_tss
        agg["local_date"] = d
        by_day.append(
            _session_row(agg, intensity_by_date, day_streams, ftp, threshold_hr)
        )
    by_day.sort(key=lambda x: x["date"] or "", reverse=True)
    return by_day[:limit]


def _session_was_hard(session: Dict[str, Any], tss: float, intensity_minutes: float = 0.0) -> bool:
    label = session.get("intensity_label")
    if label in ("hard", "very_hard"):
        return True
    return is_hard_day(tss, session.get("suffer_score"), intensity_minutes)


def format_fatigue_source_phrase(ef: Dict[str, Any]) -> str:
    """Human-readable attribution that respects how many days ago the session was."""
    name = ef.get("source_name")
    days = ef.get("source_days_ago")
    if days == 1 and name:
        return f"yesterday's {name}"
    if days == 2 and name:
        return f"your {name} 2 days ago"
    if name and ef.get("source_date"):
        return f"{name} ({ef['source_date']})"
    return ef.get("source_session") or "recent training"


def _resolve_fatigue_driver(
    yesterday: Dict[str, Any],
    two_days_ago_session: Optional[Dict[str, Any]],
    target: date,
    lag: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[int], bool]:
    """
    Pick the session driving this morning's fatigue and how many days ago it was.
    Returns (session, source_days_ago, delayed_mild_only).
    """
    yesterday_date = (target - timedelta(days=1)).isoformat()
    two_days_ago_date = (target - timedelta(days=2)).isoformat()

    if lag == 2 and two_days_ago_session and two_days_ago_session.get("trained"):
        if str(two_days_ago_session.get("date"))[:10] == two_days_ago_date:
            return two_days_ago_session, 2, False

    if yesterday.get("trained") and str(yesterday.get("date"))[:10] == yesterday_date:
        return yesterday, 1, False

    if (
        lag == 1
        and not yesterday.get("trained")
        and two_days_ago_session
        and two_days_ago_session.get("trained")
        and str(two_days_ago_session.get("date"))[:10] == two_days_ago_date
    ):
        return two_days_ago_session, 2, True

    return None, None, False


def compute_expected_fatigue(
    yesterday: Dict[str, Any],
    two_days_ago_tss: float,
    recovery_response: Dict[str, Any],
    days_since_hard: Optional[int],
    target: date,
    two_days_ago_session: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Expected physiological deviation from recent training (normal day-after response).
    """
    empty = {
        "level": "none",
        "expected_rhr_bump": 0.0,
        "expected_proxy_dip": 0.0,
        "source_session": None,
        "source_name": None,
        "source_date": None,
        "source_days_ago": None,
        "fatigue_lag_days": None,
        "clears_by": None,
    }

    lag = recovery_response.get("rhr_recovery_days") or 1
    try:
        lag = max(1, int(lag))
    except (TypeError, ValueError):
        lag = 1

    sess, source_days_ago, delayed_mild_only = _resolve_fatigue_driver(
        yesterday, two_days_ago_session, target, lag
    )
    if not sess or source_days_ago is None:
        return empty

    tss = safe_float(sess.get("tss")) or safe_float(two_days_ago_tss) or 0.0
    im = safe_float(sess.get("intensity_minutes")) or 0.0
    label = sess.get("intensity_label") or intensity_label(
        tss, sess.get("suffer_score"), im, True
    )
    name = sess.get("name") or "session"
    sess_date = str(sess.get("date") or (target - timedelta(days=source_days_ago)).isoformat())[:10]

    hard = _session_was_hard(sess, tss, im)
    moderate = label == "moderate" or (40 <= tss < 70)
    if not hard and not moderate:
        return empty

    if delayed_mild_only:
        level = "mild"
        rhr_bump = 1.0
        proxy_dip = 0.15
    elif hard:
        level = "high" if label == "very_hard" or tss >= 90 else "moderate"
        rhr_bump = 4.0 if level == "high" else 2.5
        proxy_dip = 0.6 if level == "high" else 0.35
    else:
        level = "mild"
        rhr_bump = 1.5
        proxy_dip = 0.2

    clears_by = (date.fromisoformat(sess_date) + timedelta(days=lag)).isoformat()

    return {
        "level": level,
        "expected_rhr_bump": round(rhr_bump, 1),
        "expected_proxy_dip": round(proxy_dip, 2),
        "source_session": f"{name} {sess_date}",
        "source_name": name,
        "source_date": sess_date,
        "source_days_ago": source_days_ago,
        "fatigue_lag_days": lag,
        "clears_by": clears_by,
    }


def training_load_pattern_note(recent_sessions: list, hard_days_7d: int) -> Optional[str]:
    if not recent_sessions:
        return None
    trained = [s for s in recent_sessions if s.get("intensity_label") != "rest"]
    if len(trained) >= 4:
        return (
            f"You trained {len(trained)} days in the last week "
            f"with {hard_days_7d} hard session{'s' if hard_days_7d != 1 else ''} — "
            "a normal load week for you."
        )
    return None
