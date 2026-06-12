import re
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

DATE_COLUMNS = frozenset({"date", "run_date", "computed_on"})
TIMESTAMP_COLUMNS = frozenset(
    {
        "sleep_start",
        "sleep_end",
        "sleep_start_local",
        "sleep_end_local",
        "start_date",
        "start_date_local",
        "started_at",
        "finished_at",
        "sent_at",
        "updated_at",
        "fetched_at",
        "enriched_at",
        "computed_at",
        "ingested_at",
    }
)
INT_COLUMNS = frozenset({"steps", "strava_activity_id", "moving_time", "elapsed_time", "steps_fullday", "intensity_minutes_prenoon", "n_pairs"})
FLOAT_COLUMNS = frozenset(
    {
        "rhr",
        "avg_hr",
        "min_hr",
        "max_hr",
        "avg_stress",
        "rest_pct",
        "high_pct",
        "sleep_minutes",
        "deep_minutes",
        "light_minutes",
        "rem_minutes",
        "awake_minutes",
        "sleep_score",
        "sleep_stress",
        "bb_high",
        "bb_low",
        "charged",
        "drained",
        "calories",
        "active_calories",
        "intensity_minutes",
        "sedentary_minutes",
        "waking_rr",
        "sleep_rr",
        "vo2max",
        "readiness_score",
        "morning_readiness",
        "distance",
        "elevation_gain",
        "avg_speed",
        "max_speed",
        "avg_cadence",
        "avg_watts",
        "weighted_avg_watts",
        "kilojoules",
        "suffer_score",
        "elev_high",
        "elev_low",
        "aerobic_te",
        "anaerobic_te",
        "activity_load",
        "value",
        "hr_drift",
        "aerobic_decoupling",
        "efficiency_factor",
        "np_proxy",
        "tss_proxy",
        "recovery_score",
        "burnout_risk_score",
        "illness_probability_score",
        "training_readiness_score",
        "cognitive_readiness_score",
        "atl",
        "ctl",
        "load_ratio",
        "last_night_avg_ms",
        "weekly_avg_ms",
        "baseline_low_ms",
        "baseline_high_ms",
        "nocturnal_proxy",
        "sleep_quality_index",
        "stress_band_morning",
        "stress_band_work",
        "stress_band_evening",
        "stress_peak_value",
        "high_stress_minutes",
        "avg_stress_fullday",
        "high_stress_pct_fullday",
        "rest_pct_fullday",
        "bb_recharge_efficiency",
        "steps_fullday",
        "best_r",
        "tss",
    }
)
BOOL_COLUMNS = frozenset({"trainer", "garmin_only_load", "garmin_fitness_partial", "is_hard_day", "passed"})


class BigQueryClient:
    def __init__(self, settings):
        self._settings = settings
        self._client = None

    @property
    def client(self):
        if self._client is None:
            creds = service_account.Credentials.from_service_account_file(
                str(self._settings.SERVICE_ACCOUNT_PATH)
            )
            self._client = bigquery.Client(
                credentials=creds, project=self._settings.BQ_PROJECT_ID
            )
        return self._client

    @staticmethod
    def prepare_dataframe(df):
        if df.empty:
            return df
        out = df.copy()
        for col in out.columns:
            if col in DATE_COLUMNS:
                out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
            elif col in TIMESTAMP_COLUMNS:
                out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
            elif col in INT_COLUMNS:
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif col in FLOAT_COLUMNS:
                out[col] = pd.to_numeric(out[col], errors="coerce")
            elif col in BOOL_COLUMNS:
                out[col] = out[col].astype("boolean")
        return out

    def load(self, query):
        job = self.client.query(query, location=self._settings.BQ_LOCATION)
        return job.result().to_dataframe(create_bqstorage_client=True)

    def execute(self, sql):
        return self.client.query(sql, location=self._settings.BQ_LOCATION).result()

    def save(self, df, table, mode="WRITE_APPEND"):
        if df is None or df.empty:
            return None
        df = self.prepare_dataframe(df)
        destination = f"{self._settings.BQ_PROJECT_ID}.{self._settings.BQ_DATASET_ID}.{table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=mode,
            autodetect=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        if mode == "WRITE_APPEND":
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        job = self.client.load_table_from_dataframe(
            df,
            destination,
            job_config=job_config,
            location=self._settings.BQ_LOCATION,
        )
        result = job.result()
        if job.error_result or job.errors:
            raise RuntimeError(f"BigQuery load failed: {job.error_result or job.errors}")
        return result

    def init_schema(self):
        schema_path = Path(__file__).parent / "bq_schema.sql"
        sql = schema_path.read_text()
        sql = sql.replace("{BQ_PROJECT_ID}", self._settings.BQ_PROJECT_ID)
        sql = sql.replace("{BQ_DATASET_ID}", self._settings.BQ_DATASET_ID)
        sql = re.sub(
            r"location\s*=\s*'[^']+'",
            f"location = '{self._settings.BQ_LOCATION}'",
            sql,
            count=1,
        )
        for stmt in (s.strip() for s in sql.split(";") if s.strip()):
            self.execute(stmt)
