import uuid
from datetime import date, datetime, timezone

import pandas as pd

WELLNESS_TABLES = (
    "raw_heart_rate",
    "raw_stress",
    "raw_sleep",
    "raw_body_battery",
    "raw_activity_daily",
    "raw_respiration",
    "raw_fitness",
    "raw_hrv",
)

# Columns on daily_aggregates (extras like training_adjusted live only in scores_json).
DAILY_AGGREGATE_COLUMNS = frozenset(
    {
        "date",
        "recovery_score",
        "burnout_risk_score",
        "illness_probability_score",
        "training_readiness_score",
        "cognitive_readiness_score",
        "atl",
        "ctl",
        "load_ratio",
        "garmin_fitness_partial",
        "scores_json",
        "flags_json",
        "pattern_alerts_json",
        "data_quality_json",
    }
)


class Repository:
    def __init__(self, bq, settings):
        self._bq = bq
        self._settings = settings

    def merge(self, table, df, key_columns):
        if df is None or df.empty:
            return
        staging = f"{table}_staging_{uuid.uuid4().hex[:8]}"
        staging_fq = self._settings.table_id(staging)
        target_fq = self._settings.table_id(table)

        self._bq.save(df, staging, mode="WRITE_TRUNCATE")

        on_clause = " AND ".join(f"T.{c} = S.{c}" for c in key_columns)
        update_cols = [c for c in df.columns if c not in key_columns]
        if update_cols:
            set_clause = ", ".join(f"T.{c} = S.{c}" for c in update_cols)
            update_part = f"UPDATE SET {set_clause}"
        else:
            update_part = "UPDATE SET T.ingested_at = S.ingested_at"

        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join(f"S.{c}" for c in df.columns)
        sql = f"""
        MERGE {target_fq} T
        USING {staging_fq} S
        ON {on_clause}
        WHEN MATCHED THEN {update_part}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self._bq.execute(sql)
        self._bq.client.delete_table(
            f"{self._settings.BQ_PROJECT_ID}.{self._settings.BQ_DATASET_ID}.{staging}",
            not_found_ok=True,
        )

    def append(self, table, df):
        self._bq.save(df, table, mode="WRITE_APPEND")

    def replace_dates(self, table, df, date_col="date"):
        if df is None or df.empty:
            return
        dates = sorted({str(d)[:10] for d in df[date_col].unique()})
        date_list = ", ".join(f"DATE '{d}'" for d in dates)
        self._bq.execute(
            f"DELETE FROM {self._settings.table_id(table)} WHERE {date_col} IN ({date_list})"
        )
        self.append(table, df)

    def get_sync_state(self, key):
        q = (
            f"SELECT state_value FROM {self._settings.table_id('sync_state')} "
            f"WHERE state_key = '{key}' LIMIT 1"
        )
        try:
            df = self._bq.load(q)
            if df.empty:
                return None
            return str(df.iloc[0]["state_value"])
        except Exception:
            return None

    def set_sync_state(self, key, value):
        df = pd.DataFrame(
            [{"state_key": key, "state_value": value, "updated_at": datetime.now(timezone.utc)}]
        )
        self.merge("sync_state", df, ["state_key"])

    def log_pipeline_run(self, run_date, status, error=None):
        run_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        self.append(
            "pipeline_runs",
            pd.DataFrame(
                [
                    {
                        "run_id": run_id,
                        "run_date": run_date.isoformat(),
                        "started_at": now,
                        "finished_at": now if status != "running" else None,
                        "status": status,
                        "error": error,
                    }
                ]
            ),
        )
        return run_id

    def pipeline_success_today(self):
        today = date.today().isoformat()
        q = f"""
        SELECT 1 FROM {self._settings.table_id('pipeline_runs')}
        WHERE run_date = DATE '{today}' AND status = 'success'
        LIMIT 1
        """
        try:
            return not self._bq.load(q).empty
        except Exception:
            return False

    def load_wellness(self, days=90):
        out = {}
        for table in WELLNESS_TABLES:
            q = (
                f"SELECT * FROM {self._settings.table_id(table)} "
                f"WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"
            )
            try:
                out[table] = self._bq.load(q)
            except Exception:
                out[table] = pd.DataFrame()
        return out

    def load_activities(self, days=90):
        return self.load_activities_for_analysis(days)

    def load_activities_for_analysis(self, days=90):
        q = f"""
        SELECT
          a.strava_activity_id,
          a.name,
          a.sport_type,
          a.start_date,
          a.moving_time,
          a.avg_hr,
          a.suffer_score,
          a.weighted_avg_watts,
          a.avg_watts,
          a.device_name,
          a.trainer,
          m.tss_proxy,
          m.tss_source,
          m.hr_drift
        FROM {self._settings.table_id('activities')} a
        LEFT JOIN {self._settings.table_id('activity_derived_metrics')} m
          USING (strava_activity_id)
        WHERE DATE(a.start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY a.start_date DESC
        """
        try:
            return self._bq.load(q)
        except Exception:
            return pd.DataFrame()

    def load_streams_by_ids(self, activity_ids):
        if not activity_ids:
            return {}
        ids = ",".join(str(int(i)) for i in activity_ids)
        q = f"""
        SELECT strava_activity_id, streams_json
        FROM {self._settings.table_id('activity_streams')}
        WHERE strava_activity_id IN ({ids})
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return {}
        return {int(r.strava_activity_id): r.streams_json for _, r in df.iterrows()}

    def get_activity_ids_in_window(self, days=90):
        q = f"""
        SELECT strava_activity_id
        FROM {self._settings.table_id('activities')}
        WHERE DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        """
        try:
            df = self._bq.load(q)
            if df.empty:
                return set()
            return {int(x) for x in df["strava_activity_id"]}
        except Exception:
            return set()

    def get_activity_ids_with_streams(self, days=90):
        q = f"""
        SELECT s.strava_activity_id
        FROM {self._settings.table_id('activity_streams')} s
        JOIN {self._settings.table_id('activities')} a USING (strava_activity_id)
        WHERE DATE(a.start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
          AND s.streams_json IS NOT NULL
        """
        try:
            df = self._bq.load(q)
            if df.empty:
                return set()
            return {int(x) for x in df["strava_activity_id"]}
        except Exception:
            return set()

    def wellness_dates_present(self, start, end):
        q = f"""
        SELECT DISTINCT date
        FROM {self._settings.table_id('raw_heart_rate')}
        WHERE date BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'
        """
        try:
            df = self._bq.load(q)
            if df.empty:
                return set()
            out = set()
            for d in df["date"]:
                if hasattr(d, "isoformat"):
                    out.add(d if not hasattr(d, "hour") else d.date())
                else:
                    out.add(date.fromisoformat(str(d)[:10]))
            return out
        except Exception:
            return set()

    def wellness_dates_complete(self, start, end):
        """Dates with HR and usable sleep — safe to skip re-ingest."""
        q = f"""
        SELECT hr.date
        FROM {self._settings.table_id('raw_heart_rate')} hr
        JOIN {self._settings.table_id('raw_sleep')} sl USING (date)
        WHERE hr.date BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}'
          AND hr.rhr IS NOT NULL
          AND sl.sleep_start IS NOT NULL
          AND sl.sleep_minutes > 0
        """
        try:
            df = self._bq.load(q)
            if df.empty:
                return set()
            out = set()
            for d in df["date"]:
                if hasattr(d, "isoformat"):
                    out.add(d if not hasattr(d, "hour") else d.date())
                else:
                    out.add(date.fromisoformat(str(d)[:10]))
            return out
        except Exception:
            return set()

    def save_daily_aggregate(self, row):
        filtered = {k: v for k, v in row.items() if k in DAILY_AGGREGATE_COLUMNS}
        self.merge("daily_aggregates", pd.DataFrame([filtered]), ["date"])

    def save_activity_metrics(self, df):
        if not df.empty:
            self.merge("activity_derived_metrics", df, ["strava_activity_id"])

    def save_llm_insight(self, row):
        self.merge("llm_insights", pd.DataFrame([row]), ["date"])

    def get_wellness_window(self, target, days=7):
        """Wellness rows from target-`days`..target (inclusive). One row per date."""
        target_iso = target.isoformat()
        q = f"""
        SELECT
          hr.date,
          hr.rhr AS rhr_bpm,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.sleep_minutes, NULL) AS sleep_minutes,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.deep_minutes, NULL) AS deep_sleep_minutes,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.rem_minutes, NULL) AS rem_minutes,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.light_minutes, NULL) AS light_minutes,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.awake_minutes, NULL) AS awake_minutes,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.sleep_score, NULL) AS sleep_score,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, sl.sleep_stress, NULL) AS sleep_stress,
          IF(sl.sleep_start IS NOT NULL AND sl.sleep_minutes > 0, rsp.sleep_rr, NULL) AS sleep_rr,
          st.avg_stress,
          st.high_pct AS high_stress_pct,
          bb.bb_high AS body_battery_high,
          bb.bb_low AS body_battery_low,
          bb.charged AS bb_charged,
          bb.drained AS bb_drained,
          hrv.last_night_avg_ms AS hrv_rmssd_ms,
          hrv.nocturnal_proxy AS hrv_proxy_nocturnal,
          act.steps,
          rsp.waking_rr AS waking_rr_brpm
        FROM {self._settings.table_id('raw_heart_rate')} hr
        LEFT JOIN {self._settings.table_id('raw_sleep')} sl USING(date)
        LEFT JOIN {self._settings.table_id('raw_stress')} st USING(date)
        LEFT JOIN {self._settings.table_id('raw_body_battery')} bb USING(date)
        LEFT JOIN {self._settings.table_id('raw_activity_daily')} act USING(date)
        LEFT JOIN {self._settings.table_id('raw_respiration')} rsp USING(date)
        LEFT JOIN {self._settings.table_id('raw_hrv')} hrv USING(date)
        WHERE hr.date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {days} DAY)
                          AND DATE '{target_iso}'
        ORDER BY hr.date
        """
        try:
            df = self._bq.load(q)
        except Exception as exc:
            if "raw_hrv" not in str(exc) and "Not found" not in str(exc):
                return pd.DataFrame()
            q_fallback = q.replace(
                f"LEFT JOIN {self._settings.table_id('raw_hrv')} hrv USING(date)\n        ",
                "",
            ).replace(
                "          hrv.last_night_avg_ms AS hrv_rmssd_ms,\n          hrv.nocturnal_proxy AS hrv_proxy_nocturnal,\n          ",
                "          CAST(NULL AS FLOAT64) AS hrv_rmssd_ms,\n          CAST(NULL AS FLOAT64) AS hrv_proxy_nocturnal,\n          ",
            )
            try:
                df = self._bq.load(q_fallback)
            except Exception:
                return pd.DataFrame()
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def get_garmin_status_today(self, target, caps=None):
        """Non-null Garmin fitness fields only; forward-fill vo2max."""
        target_iso = target.isoformat()
        caps = caps or {}
        fitness_available = bool(
            caps.get("training_readiness")
            or caps.get("morning_readiness")
            or caps.get("hrv_status")
            or caps.get("training_status")
        )
        if not fitness_available and not caps.get("vo2max", True):
            return {}

        q = f"""
        SELECT
          hrv_status, training_status, readiness_score, morning_readiness,
          vo2max, garmin_only_load, date
        FROM {self._settings.table_id('raw_fitness')}
        WHERE date <= DATE '{target_iso}'
        ORDER BY date DESC
        LIMIT 30
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return {}
        if df.empty:
            return {}

        def _val(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, str) and v.strip().lower() in ("none", ""):
                return None
            return v

        row = df.iloc[0]
        out = {}
        if fitness_available:
            for col in ("hrv_status", "training_status"):
                v = _val(row.get(col))
                if v is not None:
                    out[col] = v
            if pd.notna(row.get("readiness_score")):
                out["readiness_score"] = int(row["readiness_score"])
            if pd.notna(row.get("morning_readiness")):
                out["morning_readiness"] = int(row["morning_readiness"])

        vo2 = _val(row.get("vo2max"))
        if vo2 is None and caps.get("vo2max", True):
            for _, r in df.iterrows():
                v = _val(r.get("vo2max"))
                if v is not None:
                    vo2 = v
                    break
        if vo2 is not None:
            out["vo2max"] = float(vo2)

        if pd.notna(row.get("garmin_only_load")):
            out["garmin_only_load"] = bool(row.get("garmin_only_load"))
        return out

    def get_last_7d_activities(self, target):
        """Activities + derived metrics within target-7..target-1 inclusive."""
        target_iso = target.isoformat()
        q = f"""
        SELECT
          a.strava_activity_id,
          a.name,
          a.sport_type,
          a.start_date_local,
          DATE(a.start_date_local) AS local_date,
          a.moving_time,
          a.suffer_score,
          a.avg_hr,
          a.weighted_avg_watts,
          a.device_name,
          a.trainer,
          adm.tss_proxy,
          adm.tss_source
        FROM {self._settings.table_id('activities')} a
        LEFT JOIN {self._settings.table_id('activity_derived_metrics')} adm
          USING (strava_activity_id)
        WHERE DATE(a.start_date_local)
              BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL 7 DAY)
                  AND DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
        ORDER BY a.start_date_local
        """
        try:
            return self._bq.load(q)
        except Exception:
            return pd.DataFrame()

    def get_intensity_minutes_window(self, target, days=7):
        target_iso = target.isoformat()
        q = f"""
        SELECT date, intensity_minutes
        FROM {self._settings.table_id('raw_activity_daily')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {days} DAY)
                      AND DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
        ORDER BY date
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return pd.DataFrame()
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def get_last_workout_with_metrics(self, target):
        """Most recent activity strictly before target, with derived metrics."""
        target_iso = target.isoformat()
        q = f"""
        SELECT
          a.name,
          a.sport_type AS sport,
          DATE(a.start_date_local) AS date,
          a.moving_time / 60.0 AS minutes,
          a.avg_hr,
          a.weighted_avg_watts,
          a.suffer_score,
          a.device_name AS device,
          adm.tss_proxy AS tss,
          adm.tss_source,
          adm.hr_drift AS hr_drift_pct,
          adm.aerobic_decoupling AS aerobic_decoupling_pct,
          adm.efficiency_factor
        FROM {self._settings.table_id('activities')} a
        LEFT JOIN {self._settings.table_id('activity_derived_metrics')} adm
          USING (strava_activity_id)
        WHERE DATE(a.start_date_local)
              <= DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
        ORDER BY a.start_date_local DESC
        LIMIT 1
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return None
        return df.iloc[0].to_dict() if not df.empty else None

    def get_score_history(self, target, score_name, days=30):
        """Return [(date, value)] for score in daily_aggregates over target-days..target."""
        allowed = {"recovery_score", "illness_probability_score", "cognitive_readiness_score"}
        if score_name not in allowed:
            raise ValueError(f"unsupported score_name: {score_name}")
        target_iso = target.isoformat()
        q = f"""
        SELECT date, {score_name} AS value
        FROM {self._settings.table_id('daily_aggregates')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {int(days)} DAY)
                      AND DATE '{target_iso}'
        ORDER BY date
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return []
        if df.empty:
            return []
        out = []
        for _, r in df.iterrows():
            d = r.get("date")
            v = r.get("value")
            if pd.isna(v):
                continue
            if hasattr(d, "date"):
                d = d.date()
            elif not isinstance(d, date):
                d = date.fromisoformat(str(d)[:10])
            out.append((d, float(v)))
        return out

    def get_sleep_history_window(self, target, days=30):
        target_iso = target.isoformat()
        q = f"""
        SELECT date, sleep_start, sleep_end, sleep_minutes, deep_minutes
        FROM {self._settings.table_id('raw_sleep')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {int(days)} DAY)
                      AND DATE '{target_iso}'
        ORDER BY date
        """
        try:
            return self._bq.load(q)
        except Exception:
            return pd.DataFrame()

    def get_recent_themes_7d(self, target):
        target_iso = target.isoformat()
        q = f"""
        SELECT date, themes
        FROM {self._settings.table_id('digest_themes')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL 7 DAY)
                      AND DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
        ORDER BY date DESC
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return []
        if df.empty:
            return []
        flat = []
        for _, r in df.iterrows():
            themes = r.get("themes")
            if isinstance(themes, list):
                flat.extend([str(t) for t in themes if t])
        return flat

    def save_insight_history(
        self,
        run_date,
        finding_ids,
        categories,
        headline=None,
        lead_finding_category=None,
    ):
        row = {
            "date": run_date.isoformat(),
            "finding_ids": list(finding_ids or []),
            "categories": list(categories or []),
            "headline": headline,
            "lead_finding_category": lead_finding_category,
            "created_at": datetime.now(timezone.utc),
        }
        self.merge("insight_history", pd.DataFrame([row]), ["date"])

    def get_recent_digests(self, target, n=3):
        target_iso = target.isoformat()
        q = f"""
        SELECT date, headline, lead_finding_category
        FROM {self._settings.table_id('insight_history')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL 7 DAY)
                      AND DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
          AND headline IS NOT NULL
        ORDER BY date DESC
        LIMIT {int(n)}
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return []
        if df.empty:
            return []
        out = []
        for _, r in df.iterrows():
            d = r.get("date")
            if hasattr(d, "isoformat"):
                d = d.isoformat()[:10]
            else:
                d = str(d)[:10]
            out.append(
                {
                    "date": d,
                    "headline": r.get("headline"),
                    "lead_finding_category": r.get("lead_finding_category"),
                }
            )
        return out

    def get_recent_finding_categories(self, target, days=2):
        target_iso = target.isoformat()
        q = f"""
        SELECT categories
        FROM {self._settings.table_id('insight_history')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {int(days)} DAY)
                      AND DATE_SUB(DATE '{target_iso}', INTERVAL 1 DAY)
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return []
        flat = []
        for _, r in df.iterrows():
            cats = r.get("categories")
            if isinstance(cats, list):
                flat.extend([str(c) for c in cats if c])
        return flat

    def get_insight_cache(self):
        q = f"""
        SELECT findings_json, computed_on, window_days
        FROM {self._settings.table_id('insight_cache')}
        ORDER BY computed_on DESC
        LIMIT 1
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return []
        if df.empty:
            return []
        import json
        raw = df.iloc[0].get("findings_json")
        if not raw:
            return []
        try:
            return json.loads(raw) if isinstance(raw, str) else list(raw)
        except (json.JSONDecodeError, TypeError):
            return []

    def save_insight_cache(self, computed_on, findings, window_days=90):
        import json
        row = {
            "computed_on": computed_on.isoformat(),
            "findings_json": json.dumps(findings, default=str),
            "window_days": int(window_days),
            "ingested_at": datetime.now(timezone.utc),
        }
        self.merge("insight_cache", pd.DataFrame([row]), ["computed_on"])

    def load_wellness_daily_complete(self, days=90, target=None):
        target = target or date.today()
        target_iso = target.isoformat()
        q = f"""
        SELECT *
        FROM {self._settings.table_id('wellness_daily_complete')}
        WHERE date BETWEEN DATE_SUB(DATE '{target_iso}', INTERVAL {int(days)} DAY)
                      AND DATE '{target_iso}'
        ORDER BY date
        """
        try:
            df = self._bq.load(q)
        except Exception:
            return pd.DataFrame()
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    def save_wellness_daily_complete(self, df):
        self.replace_dates("wellness_daily_complete", df)

    def save_insight_diagnostics(self, computed_on, rows):
        if not rows:
            return
        df = pd.DataFrame(rows)
        df["computed_on"] = computed_on.isoformat()
        self.replace_dates("insight_diagnostics", df, date_col="computed_on")

    def save_digest_themes(self, run_date, themes):
        row = {
            "date": run_date.isoformat(),
            "themes": list(themes or []),
            "created_at": datetime.now(timezone.utc),
        }
        self.merge("digest_themes", pd.DataFrame([row]), ["date"])
