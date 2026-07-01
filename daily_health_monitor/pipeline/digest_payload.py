from datetime import date, timedelta

import pandas as pd

from analytics.confidence import apply_confidence_pass
from analytics.derived_blocks import (
    add_sleep_quality_index,
    compute_circadian,
    compute_recovery_response,
    compute_sleep_debt,
    enrich_hrv_proxy_block,
)
from analytics import digest_features
from analytics.digest_features import safe_float
from analytics import digest_v4
from analytics.insight_detectors import run_all_detectors
from garmin.capabilities import load_capabilities
from analytics.stress_context import build_stress_context
from analytics import training_load
from analytics.wellness_scores import illness_watch_signals


CAPABILITY_DROP = frozenset({"sleep_score", "sleep_stress", "hrv_rmssd_ms"})


class DigestPayloadBuilder:
    def __init__(self, settings, repo):
        self._settings = settings
        self._repo = repo

    def build(self, target, wellness, load, scores, expected_fatigue=None):
        caps = self._load_caps()
        wellness_window = self._repo.get_wellness_window(target, days=30)
        wellness_window = self._augment_wellness_window(wellness_window)
        today_is_partial = target >= date.today()
        today_wellness = digest_v4.enrich_today_wellness(
            wellness_window, target, today_is_partial=today_is_partial
        )
        hrv_source = self._hrv_source(wellness, wellness_window, target, caps)
        today_wellness = self._capability_gate(today_wellness, caps, hrv_source)
        today_wellness = self._reshape_hrv_proxy(today_wellness, wellness_window, hrv_source)
        today_wellness = apply_confidence_pass(today_wellness, wellness, hrv_source=hrv_source)

        garmin_status = self._repo.get_garmin_status_today(target, caps=caps)
        activities_7d = self._repo.get_last_7d_activities(target)
        intensity_minutes_df = self._repo.get_intensity_minutes_window(target, days=7)
        last_workout_raw = self._repo.get_last_workout_with_metrics(target)
        last_workout = digest_features.format_last_workout(last_workout_raw)
        last_7d = digest_features.build_last_7d(activities_7d, intensity_minutes_df, target)
        yesterday = digest_features.build_yesterday(activities_7d, intensity_minutes_df, target)

        sleep_hist = self._repo.get_sleep_history_window(target, days=30)
        sleep_context = digest_v4.compute_sleep_context(
            sleep_hist,
            target,
            today_wellness.get("sleep_minutes") or {},
            today_wellness.get("deep_sleep_minutes") or {},
        )
        sleep_debt = compute_sleep_debt(
            wellness.get("raw_sleep", sleep_hist),
            target,
            sleep_context.get("typical_duration_min"),
        )
        circadian = compute_circadian(sleep_hist, target)
        recovery_response = compute_recovery_response(
            activities_7d,
            wellness.get("raw_heart_rate"),
            target,
            yesterday.get("trained", False),
            yesterday.get("intensity_label") in ("hard", "very_hard"),
        )

        two_days_ago = target - timedelta(days=2)
        two_day_sess = digest_features.build_day_session(
            activities_7d, intensity_minutes_df, two_days_ago
        )
        expected_fatigue = expected_fatigue or training_load.compute_expected_fatigue(
            yesterday,
            safe_float(two_day_sess.get("tss")) or 0.0,
            recovery_response,
            last_7d.get("days_since_hard"),
            target,
            two_days_ago_session=two_day_sess,
        )
        recent_sessions = training_load.build_recent_sessions(
            activities_7d, intensity_minutes_df, target, limit=7
        )
        training_load_context = {
            "recent_sessions": recent_sessions,
            "days_since_hard": last_7d.get("days_since_hard"),
            "hard_days_7d": last_7d.get("hard_day_count"),
            "load_ratio": load.get("load_ratio"),
            "ctl": load.get("ctl"),
            "expected_fatigue_today": expected_fatigue,
            "pattern_note": training_load.training_load_pattern_note(
                recent_sessions, last_7d.get("hard_day_count") or 0
            ),
        }
        illness_watch = digest_features.detect_illness_watch(
            illness_watch_signals(scores),
            expected_fatigue,
        )

        complete_df = self._repo.load_wellness_daily_complete(days=30, target=target)
        raw_stress = wellness.get("raw_stress", pd.DataFrame())
        ystress = None
        if not raw_stress.empty:
            rs = raw_stress.copy()
            rs["date"] = pd.to_datetime(rs["date"]).dt.date
            yd = target - timedelta(days=1)
            row = rs[rs["date"] == yd]
            if not row.empty:
                ystress = row.iloc[0]
        stress_context = build_stress_context(
            target,
            complete_df,
            ystress,
            self._settings.STRESS_TIME_BANDS,
            today_wellness,
            activities_df=activities_7d,
            intensity_minutes_df=intensity_minutes_df,
        )

        prior_rhr_d = digest_features.prior_day_rhr_delta(wellness, target)
        flags = digest_features.compute_flags(
            today_wellness,
            load,
            last_7d.get("days_since_rest"),
            prior_rhr_delta=prior_rhr_d,
            stress_high_flag=digest_features.stress_high_5d(wellness),
        )
        patterns = digest_features.detect_patterns(
            flags,
            load,
            garmin_status.get("hrv_status"),
            hrv_source=hrv_source,
            wellness_window=wellness_window,
            ctl_floor=self._settings.CTL_FLOOR,
            expected_fatigue=expected_fatigue,
        )
        sources_used = self._sources_used(last_7d["weekly_tss_by_source"])
        garmin_fitness_available = bool(
            caps.get("training_readiness")
            or caps.get("morning_readiness")
            or caps.get("hrv_status")
            or caps.get("training_status")
        )

        recovery_traj = digest_v4.compute_score_trajectory(
            self._repo.get_score_history(target, "recovery_score", days=30),
            target,
            higher_is_better=True,
        )
        cognitive_traj = digest_v4.compute_score_trajectory(
            self._repo.get_score_history(target, "cognitive_readiness_score", days=30),
            target,
            higher_is_better=True,
        )
        if scores.get("training_adjusted"):
            recovery_traj["training_adjusted"] = True
        score_blocks = {
            "recovery_score": recovery_traj,
            "cognitive_readiness_score": cognitive_traj,
        }
        if recovery_traj.get("today") is None and scores.get("recovery_score") is not None:
            recovery_traj["today"] = round(float(scores["recovery_score"]), 1)
        if cognitive_traj.get("today") is None and scores.get("cognitive_readiness_score") is not None:
            cognitive_traj["today"] = round(float(scores["cognitive_readiness_score"]), 1)

        payload = {
            "date": target.isoformat(),
            "user_context": {
                "ftp_watts": int(self._settings.FTP_WATTS),
                "threshold_hr_bpm": int(self._settings.THRESHOLD_HR),
            },
            "today_wellness": today_wellness,
            "garmin_status": garmin_status,
            "load": {
                "atl": load.get("atl"),
                "ctl": load.get("ctl"),
                "load_ratio": load.get("load_ratio"),
                "monotony": load.get("monotony"),
                "strain": load.get("strain"),
            },
            "yesterday": yesterday,
            "last_workout": self._payload_last_workout(last_workout),
            "last_7d": last_7d,
            "scores": score_blocks,
            "flags": flags,
            "patterns": patterns,
            "data_quality": {
                "tss_sources_used_7d": sources_used,
                "hrv_source": hrv_source,
                "garmin_fitness_available": garmin_fitness_available,
            },
            "sleep_context": sleep_context,
            "sleep_debt": sleep_debt,
            "circadian": circadian,
            "recovery_response": recovery_response,
            "stress_context": stress_context,
            "training_load_context": training_load_context,
            "recent_digests": self._repo.get_recent_digests(target, n=3),
            "recent_themes_7d": self._repo.get_recent_themes_7d(target),
            "insights": [],
        }

        ctx = {
            "target": target,
            "wellness": wellness,
            "wellness_window_df": wellness_window,
            "today_wellness": today_wellness,
            "sleep_context": sleep_context,
            "sleep_debt": sleep_debt,
            "circadian": circadian,
            "recovery_response": recovery_response,
            "yesterday": yesterday,
            "last_workout": last_workout,
            "load": payload["load"],
            "scores": score_blocks,
            "sleep_df": wellness.get("raw_sleep", sleep_hist),
            "activities_df": activities_7d,
            "cached_weekly": self._repo.get_insight_cache(),
            "recent_finding_categories": self._repo.get_recent_finding_categories(target),
            "hrv_source": hrv_source,
            "stress_context": stress_context,
            "complete_df": complete_df,
            "training_load_context": training_load_context,
            "expected_fatigue": expected_fatigue,
            "illness_watch": illness_watch,
        }
        payload["insights"] = run_all_detectors(ctx)
        payload["health_state"] = digest_features.compute_health_state(
            payload, ctl_floor=self._settings.CTL_FLOOR
        )
        return payload

    def _load_caps(self):
        path = self._settings.LOCAL_STATE_DIR / "garmin_capabilities.json"
        caps = load_capabilities(path)
        if caps is not None:
            return caps
        return {
            "nightly_hrv": False,
            "sleep_score": False,
            "sleep_stress": False,
            "training_readiness": False,
            "morning_readiness": False,
            "training_status": False,
            "hrv_status": False,
            "nocturnal_proxy": True,
        }

    @staticmethod
    def _augment_wellness_window(df):
        if df is None or df.empty:
            return df
        out = df.copy()
        if "bb_charged" in out.columns and "sleep_minutes" in out.columns:
            sm = out["sleep_minutes"].replace(0, None)
            out["bb_recharge_efficiency"] = out["bb_charged"] / (sm / 60.0)
            out.loc[sm.isna(), "bb_recharge_efficiency"] = None
        return add_sleep_quality_index(out)

    @staticmethod
    def _hrv_source(wellness, wellness_window, target, caps):
        if caps.get("nightly_hrv"):
            hrv_df = wellness.get("raw_hrv")
            if hrv_df is not None and not hrv_df.empty:
                hrv_df = hrv_df.copy()
                hrv_df["date"] = pd.to_datetime(hrv_df["date"]).dt.date
                row = hrv_df[hrv_df["date"] == target]
                if not row.empty and row.iloc[0].get("last_night_avg_ms") is not None:
                    return "garmin_nightly"
        if caps.get("nocturnal_proxy", True):
            hrv_df = wellness.get("raw_hrv")
            if hrv_df is not None and not hrv_df.empty:
                hrv_df = hrv_df.copy()
                hrv_df["date"] = pd.to_datetime(hrv_df["date"]).dt.date
                row = hrv_df[hrv_df["date"] == target]
                if not row.empty and row.iloc[0].get("nocturnal_proxy") is not None:
                    return "nocturnal_proxy"
            if wellness_window is not None and not wellness_window.empty:
                if "hrv_proxy_nocturnal" in wellness_window.columns:
                    tw = wellness_window[wellness_window["date"] == target]
                    if not tw.empty and tw.iloc[0].get("hrv_proxy_nocturnal") is not None:
                        return "nocturnal_proxy"
        return "none"

    @staticmethod
    def _capability_gate(today_wellness, caps, hrv_source):
        drop = set(CAPABILITY_DROP)
        if not caps.get("nightly_hrv") or hrv_source != "garmin_nightly":
            drop.add("hrv_rmssd_ms")
        if not caps.get("sleep_score"):
            drop.add("sleep_score")
        if not caps.get("sleep_stress"):
            drop.add("sleep_stress")
        for key in drop:
            today_wellness.pop(key, None)
        return today_wellness

    @staticmethod
    def _reshape_hrv_proxy(today_wellness, wellness_window, hrv_source):
        if hrv_source != "nocturnal_proxy":
            return today_wellness
        block = today_wellness.get("hrv_proxy_nocturnal")
        if not isinstance(block, dict):
            return today_wellness
        hist = pd.Series(dtype=float)
        if wellness_window is not None and not wellness_window.empty:
            if "hrv_proxy_nocturnal" in wellness_window.columns:
                hist = wellness_window["hrv_proxy_nocturnal"]
        today_wellness["hrv_proxy_nocturnal"] = enrich_hrv_proxy_block(block, hist)
        return today_wellness

    @staticmethod
    def _sources_used(by_source):
        out = [s for s in ("power", "suffer_score", "hr") if (by_source.get(s) or 0) > 0]
        if by_source.get("none_count", 0) > 0:
            out.append("none")
        return out

    @staticmethod
    def _payload_last_workout(last_workout):
        if not last_workout:
            return None
        out = dict(last_workout)
        suffer = out.pop("suffer_score", None)
        if suffer is not None and "relative_effort" not in out:
            out["relative_effort"] = suffer
        return out
