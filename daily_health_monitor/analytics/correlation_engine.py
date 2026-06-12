"""Weekly correlation detectors on wellness_daily_complete with diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from analytics.insight_detectors import Finding


@dataclass
class DetectorDiagnostic:
    detector_id: str
    n_pairs: int
    best_lag: Optional[str]
    best_r: Optional[float]
    passed: bool
    reason: str

    def to_row(self, computed_on):
        return {
            "computed_on": computed_on.isoformat(),
            "detector_id": self.detector_id,
            "n_pairs": self.n_pairs,
            "best_lag": self.best_lag,
            "best_r": self.best_r,
            "passed": self.passed,
            "reason": self.reason,
        }


def _tier(r: float, n: int) -> Tuple[Optional[str], bool]:
    if r is None or n < 20:
        return None, False
    ar = abs(r)
    if ar >= 0.35 and n >= 30:
        return "high", True
    if ar >= 0.25 and n >= 20:
        return "medium", True
    return None, False


def _corr(merged: pd.DataFrame, x: str, y: str) -> Tuple[Optional[float], int]:
    m = merged[[x, y]].dropna()
    if len(m) < 20:
        return None, len(m)
    r = m[x].corr(m[y])
    return (float(r) if r is not None and not np.isnan(r) else None), len(m)


def run_weekly_correlations(
    complete_df: pd.DataFrame,
    activities_df: pd.DataFrame,
    target,
) -> Tuple[List[Finding], List[DetectorDiagnostic]]:
    if complete_df is None or complete_df.empty:
        return [], [
            DetectorDiagnostic("all", 0, None, None, False, "wellness_daily_complete empty")
        ]

    df = complete_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] < target].sort_values("date")
    findings: List[Finding] = []
    diags: List[DetectorDiagnostic] = []

    # stress_costs_deep_sleep
    merged = df.dropna(subset=["avg_stress_fullday", "deep_minutes"])
    r, n = _corr(merged, "avg_stress_fullday", "deep_minutes")
    conf, ok = _tier(r, n) if r is not None else (None, False)
    if ok and r is not None and r < 0:
        high = merged[merged["avg_stress_fullday"] >= merged["avg_stress_fullday"].quantile(0.66)]["deep_minutes"].mean()
        low = merged[merged["avg_stress_fullday"] <= merged["avg_stress_fullday"].quantile(0.33)]["deep_minutes"].mean()
        penalty = int(round(float(low - high))) if low > high else int(round(float(high - low)))
        label = "an early signal, still firming up" if conf == "medium" else "consistent enough to plan around"
        findings.append(
            Finding(
                id="stress_costs_deep_sleep",
                category="stress",
                salience=68 if conf == "medium" else 76,
                confidence=conf,
                timeframe=f"{n}-day pattern, same-night",
                summary=(
                    f"On your higher-stress days, deep sleep drops about {penalty} min that night "
                    f"(r={r:.2f}, {n} days). The link is {label}."
                ),
                supporting={"penalty_min": penalty, "r": round(r, 2), "n": n},
                suggested_theme="stress_reset",
                cadence="weekly_cached",
                prospective=True,
            )
        )
        diags.append(DetectorDiagnostic("stress_costs_deep_sleep", n, None, r, True, "passed"))
    else:
        reason = f"r below threshold (r={r}, n={n})" if n >= 20 else f"insufficient pairs (n={n}, need >=20)"
        diags.append(DetectorDiagnostic("stress_costs_deep_sleep", n, None, r, False, reason))

    # rhr_recovery_lag
    hard = df[df["is_hard_day"] == True]["date"].tolist()  # noqa: E712
    hr = df.set_index("date")["rhr"]
    base = float(hr.dropna().tail(30).mean()) if not hr.dropna().empty else None
    best_lag, best_bump, best_n, best_r = None, 0.0, len(hard), None
    if base is not None and len(hard) >= 8:
        for lag in (1, 2):
            bumps = []
            for hd in hard:
                check = hd + timedelta(days=lag)
                if check in hr.index and pd.notna(hr.loc[check]):
                    bumps.append(float(hr.loc[check]) - base)
            if len(bumps) >= 8:
                mb = float(np.mean(bumps))
                if mb > best_bump:
                    best_bump, best_lag, best_n = mb, lag, len(bumps)
        if best_lag and best_bump >= 2:
            findings.append(
                Finding(
                    id="rhr_recovery_lag",
                    category="training_response",
                    salience=82,
                    confidence="high",
                    timeframe=f"lag +{best_lag} days, {best_n}-session pattern",
                    summary=(
                        f"Your RHR typically rises ~{best_bump:.0f} bpm {best_lag} day(s) after a hard session "
                        f"({best_n} sessions)."
                    ),
                    supporting={"rhr_bump": round(best_bump, 1), "lag": f"+{best_lag} days", "n": best_n},
                    cadence="weekly_cached",
                    prospective=True,
                )
            )
            diags.append(
                DetectorDiagnostic("rhr_recovery_lag", best_n, f"+{best_lag}d", best_bump, True, "passed")
            )
        else:
            diags.append(
                DetectorDiagnostic(
                    "rhr_recovery_lag",
                    best_n,
                    None,
                    None,
                    False,
                    "insufficient hard days or weak lag bump (need >=8 hard days)",
                )
            )
    else:
        diags.append(
            DetectorDiagnostic(
                "rhr_recovery_lag",
                len(hard),
                None,
                None,
                False,
                f"insufficient hard days (need >=8, have {len(hard)})",
            )
        )

    # morning_activity_helps_sleep — steps vs sleep quality
    merged = df.dropna(subset=["steps_fullday", "sleep_quality_index"])
    r, n = _corr(merged, "steps_fullday", "sleep_quality_index")
    conf, ok = _tier(r, n) if r is not None else (None, False)
    if ok and r is not None and r > 0:
        findings.append(
            Finding(
                id="morning_activity_helps_sleep",
                category="sleep",
                salience=50 if conf == "medium" else 52,
                confidence=conf,
                timeframe=f"{n}-day pattern",
                summary=(
                    f"Your higher-quality sleep nights line up with more active days "
                    f"(r={r:.2f}, {n} days)."
                    + (" An early signal, still firming up." if conf == "medium" else "")
                ),
                supporting={"r": round(r, 2), "n": n},
                suggested_theme="parasympathetic_morning",
                cadence="weekly_cached",
                prospective=True,
            )
        )
        diags.append(DetectorDiagnostic("morning_activity_helps_sleep", n, None, r, True, "passed"))
    else:
        diags.append(
            DetectorDiagnostic(
                "morning_activity_helps_sleep",
                n,
                None,
                r,
                False,
                f"r below threshold (r={r}, n={n})" if n >= 20 else f"insufficient pairs (n={n})",
            )
        )

    # bedtime_regularity_drives_quality
    sl = df[df["sleep_start_local"].notna()].copy()
    if not sl.empty:
        sl["bed_min"] = pd.to_datetime(sl["sleep_start_local"]).dt.hour * 60 + pd.to_datetime(
            sl["sleep_start_local"]
        ).dt.minute
        merged = sl.dropna(subset=["bed_min", "sleep_quality_index"])
        r, n = _corr(merged, "bed_min", "sleep_quality_index")
        conf, ok = _tier(r, n) if r is not None else (None, False)
        bt_std = float(merged["bed_min"].std()) if len(merged) >= 3 else 0
        if ok and r is not None and abs(r) >= 0.2 and bt_std >= 30:
            findings.append(
                Finding(
                    id="bedtime_regularity_drives_quality",
                    category="circadian",
                    salience=46 if conf == "medium" else 48,
                    confidence=conf,
                    timeframe=f"{n}-day pattern",
                    summary=(
                        f"More regular bedtimes correlate with better sleep quality "
                        f"(bedtime swing ~{int(bt_std)} min, r={r:.2f})."
                        + (" Early signal." if conf == "medium" else "")
                    ),
                    supporting={"bedtime_stdev_min": int(bt_std), "n": n, "r": round(r, 2)},
                    suggested_theme="sleep_repair",
                    cadence="weekly_cached",
                    prospective=True,
                )
            )
            diags.append(DetectorDiagnostic("bedtime_regularity_drives_quality", n, None, r, True, "passed"))
        else:
            diags.append(
                DetectorDiagnostic(
                    "bedtime_regularity_drives_quality",
                    n,
                    None,
                    r,
                    False,
                    f"r={r}, n={n}, bt_std={bt_std:.0f}" if n >= 20 else f"insufficient pairs (n={n})",
                )
            )
    else:
        diags.append(
            DetectorDiagnostic("bedtime_regularity_drives_quality", 0, None, None, False, "no sleep_start data")
        )

    # sleep_drives_next_day_calm
    sl = df.dropna(subset=["sleep_quality_index", "avg_stress_fullday"]).copy()
    sl["next_date"] = sl["date"].apply(lambda d: d + timedelta(days=1))
    nxt = df[["date", "avg_stress_fullday"]].rename(columns={"avg_stress_fullday": "next_stress"})
    merged = sl.merge(nxt, left_on="next_date", right_on="date", how="inner", suffixes=("", "_n"))
    r, n = _corr(merged, "sleep_quality_index", "next_stress")
    conf, ok = _tier(r, n) if r is not None else (None, False)
    if ok and r is not None and r < 0:
        findings.append(
            Finding(
                id="sleep_drives_next_day_calm",
                category="sleep",
                salience=48 if conf == "medium" else 50,
                confidence=conf,
                timeframe=f"{n}-day lag pattern",
                summary=(
                    f"Better sleep quality tends to precede calmer next days (r={r:.2f}, {n} nights)."
                    + (" Early signal." if conf == "medium" else "")
                ),
                supporting={"r": round(r, 2), "n": n},
                suggested_theme="sleep_repair",
                cadence="weekly_cached",
                prospective=True,
            )
        )
        diags.append(DetectorDiagnostic("sleep_drives_next_day_calm", n, "lag +1d", r, True, "passed"))
    else:
        diags.append(
            DetectorDiagnostic(
                "sleep_drives_next_day_calm",
                n,
                "+1d",
                r,
                False,
                f"r below threshold (r={r}, n={n})" if n >= 20 else f"insufficient pairs (n={n})",
            )
        )

    return findings, diags
