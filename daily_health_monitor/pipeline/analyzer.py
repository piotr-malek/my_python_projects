from datetime import date, timedelta

import pandas as pd

from analytics import correlations, stream_metrics, training_load, wellness_scores
from analytics.derived_blocks import compute_recovery_response
from analytics.digest_features import build_day_session, build_yesterday, format_last_workout, safe_float
from analytics import digest_features
from pipeline.digest_payload import DigestPayloadBuilder
from strava import transforms
from strava.streams import parse_streams_json
from util.json_util import to_json


class Analyzer:
    def __init__(self, settings, repo):
        self._settings = settings
        self._repo = repo
        self._payload_builder = DigestPayloadBuilder(settings, repo)
        self.stats = {}

    @staticmethod
    def _garmin_fitness_partial(activities, garmin_devices):
        if activities.empty:
            return False
        for _, r in activities.head(20).iterrows():
            if r.get("trainer") or not transforms.is_garmin_device(
                r.get("device_name"), garmin_devices
            ):
                return True
        return False

    def run(self, target=None):
        target = target or date.today()
        days = self._settings.ANALYSIS_DAYS
        ftp = self._settings.FTP_WATTS
        thr = self._settings.THRESHOLD_HR

        wellness = self._repo.load_wellness(days)
        activities = self._repo.load_activities_for_analysis(days)

        if "tss_proxy" in activities.columns:
            needs_metrics = activities[
                pd.isna(activities["tss_proxy"])
                | activities.apply(training_load.activity_needs_tss_recompute, axis=1)
            ]
        else:
            needs_metrics = activities

        stream_map = {}
        if not needs_metrics.empty:
            ids = [int(x) for x in needs_metrics["strava_activity_id"].unique()]
            raw_streams = self._repo.load_streams_by_ids(ids)
            stream_map = {aid: parse_streams_json(raw) for aid, raw in raw_streams.items()}

        self.stats = {
            "activities": len(activities),
            "metrics_computed": 0,
            "metrics_skipped": len(activities) - len(needs_metrics),
        }

        daily_tss = training_load.daily_tss_series(activities, stream_map, ftp, thr)
        load = training_load.compute_load_metrics(daily_tss)
        legacy_flags, metrics = wellness_scores.compute_wellness_flags(wellness, target)

        if load.get("load_ratio", 0) > 1.3:
            legacy_flags.append("Strava load ratio >1.3 (overreaching risk)")

        activities_7d = self._repo.get_last_7d_activities(target)
        intensity_minutes_df = self._repo.get_intensity_minutes_window(target, days=7)
        last_workout_raw = self._repo.get_last_workout_with_metrics(target)
        last_workout = format_last_workout(last_workout_raw)
        yesterday = build_yesterday(
            activities_7d,
            intensity_minutes_df,
            target,
            stream_map=stream_map,
            ftp=ftp,
            threshold_hr=thr,
        )
        recovery_response = compute_recovery_response(
            activities_7d,
            wellness.get("raw_heart_rate"),
            target,
            yesterday.get("trained", False),
            yesterday.get("intensity_label") in ("hard", "very_hard"),
        )
        last_7d_preview = digest_features.build_last_7d(
            activities_7d, intensity_minutes_df, target
        )
        two_days_ago = target - timedelta(days=2)
        two_day_sess = build_day_session(
            activities_7d,
            intensity_minutes_df,
            two_days_ago,
            stream_map=stream_map,
            ftp=ftp,
            threshold_hr=thr,
        )
        if not daily_tss.empty and two_days_ago in daily_tss.index:
            two_day_tss = float(daily_tss.loc[two_days_ago])
        else:
            two_day_tss = safe_float(two_day_sess.get("tss")) or 0.0
        expected_fatigue = training_load.compute_expected_fatigue(
            yesterday,
            two_day_tss,
            recovery_response,
            last_7d_preview.get("days_since_hard"),
            target,
            two_days_ago_session=two_day_sess,
        )

        fitness_df = wellness.get("raw_fitness", pd.DataFrame())
        partial_legacy = self._garmin_fitness_partial(activities, self._settings.GARMIN_DEVICES)
        scores = wellness_scores.composite_scores(
            wellness,
            target,
            load,
            legacy_flags,
            metrics,
            expected_fatigue=expected_fatigue,
        )

        act_rows = []
        hr_drifts = []
        for _, row in activities.iterrows():
            aid = int(row["strava_activity_id"])
            if pd.notna(row.get("tss_proxy")) and not training_load.activity_needs_tss_recompute(row):
                if row.get("hr_drift") and row["hr_drift"] > 0.05:
                    hr_drifts.append(True)
                continue

            if aid not in stream_map and training_load.activity_needs_tss_recompute(row):
                raw_streams = self._repo.load_streams_by_ids([aid])
                stream_map[aid] = parse_streams_json(raw_streams.get(aid))

            streams_json = to_json(stream_map[aid]) if aid in stream_map else None
            m = stream_metrics.compute_activity_metrics(streams_json, row.get("sport_type"))
            m["strava_activity_id"] = aid
            tss_val, tss_src = training_load.activity_tss_with_source(
                row, stream_map.get(aid), ftp, thr
            )
            m["tss_proxy"] = tss_val
            m["tss_source"] = tss_src
            act_rows.append(m)
            self.stats["metrics_computed"] += 1
            if m.get("hr_drift") and m["hr_drift"] > 0.05:
                hr_drifts.append(True)

        if act_rows:
            self._repo.save_activity_metrics(pd.DataFrame(act_rows))

        sleep_df = wellness.get("raw_sleep", pd.DataFrame())
        poor_sleep = False
        if not sleep_df.empty:
            last = sleep_df.sort_values("date").iloc[-1]
            poor_sleep = (last.get("sleep_minutes") or 999) < 360

        legacy_patterns = correlations.evaluate_patterns(
            legacy_flags,
            scores,
            load,
            None,
            recent_hr_drift_high=bool(hr_drifts),
            poor_sleep=poor_sleep,
        )

        legacy_data_quality = [
            "Garmin training status reflects Edge/vivoactive workouts only; "
            "COROS and indoor cycling load comes from Strava.",
        ]
        if partial_legacy:
            legacy_data_quality.append(
                "Recent activities include non-Garmin sources; Garmin readiness weight reduced."
            )
        if activities.empty:
            legacy_data_quality.append("No recent Strava activities in window.")

        load_cols = {k: load[k] for k in ("atl", "ctl", "load_ratio") if k in load}
        public_scores = {k: v for k, v in scores.items() if not str(k).startswith("_")}
        self._repo.save_daily_aggregate(
            {
                "date": target.isoformat(),
                **public_scores,
                **load_cols,
                "garmin_fitness_partial": partial_legacy,
                "scores_json": to_json(public_scores),
                "flags_json": to_json(legacy_flags),
                "pattern_alerts_json": to_json(legacy_patterns),
                "data_quality_json": to_json(legacy_data_quality),
            }
        )

        return {
            "scores": scores,
            "load": load,
            "flags": legacy_flags,
            "patterns": legacy_patterns,
            "data_quality_notes": legacy_data_quality,
            "garmin_fitness_partial": partial_legacy,
            "expected_fatigue": expected_fatigue,
            "digest_payload": self._payload_builder.build(
                target, wellness, load, scores, expected_fatigue=expected_fatigue
            ),
        }
