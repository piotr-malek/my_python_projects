import time
from datetime import timedelta

import pandas as pd

from garmin import transforms
from garmin.capabilities import get_capabilities
from garmin.hrv import fetch_hrv_day, nocturnal_proxy_index
from garmin.client import GarminClient
from util.json_util import to_json


class WellnessFetcher:
    TABLES = (
        "raw_heart_rate",
        "raw_stress",
        "raw_sleep",
        "raw_body_battery",
        "raw_activity_daily",
        "raw_respiration",
        "raw_fitness",
        "raw_hrv",
    )

    def __init__(self, settings, garmin=None):
        self._settings = settings
        self._garmin = garmin or GarminClient.get(settings)
        self.stats = {"fetched_days": 0, "skipped_days": 0}
        cache_path = settings.LOCAL_STATE_DIR / "garmin_capabilities.json"
        self._caps = get_capabilities(self._garmin, cache_path)

    def fetch_day(self, d):
        try:
            return self._fetch_day(d)
        except Exception as e:
            print(f"Garmin fetch failed for {d}: {e}")
            return {}

    def _fetch_day(self, d):
        cdate = d.isoformat()
        api = self._garmin.api
        call = self._garmin.call

        stats = call(api.get_stats, cdate) or {}
        hr = call(api.get_heart_rates, cdate) or {}
        stress = call(api.get_stress_data, cdate) or {}
        sleep_raw = call(api.get_sleep_data, cdate) or {}
        bb_list = call(api.get_body_battery, cdate, cdate) or []
        resp = call(api.get_respiration_data, cdate) or {}
        training = call(api.get_training_status, cdate) or {}
        readiness = {}
        if self._caps.get("training_readiness"):
            readiness = call(api.get_training_readiness, cdate) or {}
        morning = {}
        if self._caps.get("morning_readiness"):
            morning = call(api.get_morning_training_readiness, cdate) or {}
        hrv = {}
        if self._caps.get("nightly_hrv"):
            hrv = call(api.get_hrv_data, cdate) or {}

        sleep = sleep_raw.get("dailySleepDTO") or {}
        hr_values = hr.get("heartRateValues") or []
        samples = [
            {"t": v[0], "hr": v[1]}
            for v in hr_values
            if isinstance(v, (list, tuple)) and len(v) >= 2
        ]
        bb_day = bb_list[0] if isinstance(bb_list, list) and bb_list else {}

        profile_id = transforms.extract_profile_id(training)
        load_balance = {}
        if profile_id:
            m = training.get("mostRecentTrainingLoadBalance") or {}
            load_balance = (m.get("metricsTrainingLoadBalanceDTOMap") or {}).get(profile_id) or {}

        vo2 = (training.get("mostRecentVO2Max") or {}).get("generic") or {}

        hr_row = {
            "date": cdate,
            "rhr": stats.get("restingHeartRate"),
            "avg_hr": stats.get("averageHeartRate"),
            "min_hr": stats.get("minHeartRate"),
            "max_hr": stats.get("maxHeartRate"),
            "samples_json": to_json(samples),
        }
        sleep_row = {
            "date": cdate,
            "sleep_start": transforms.ms_local_to_iso(sleep.get("sleepStartTimestampLocal")),
            "sleep_end": transforms.ms_local_to_iso(sleep.get("sleepEndTimestampLocal")),
            "sleep_minutes": (sleep.get("sleepTimeSeconds") or 0) / 60,
            "deep_minutes": (sleep.get("deepSleepSeconds") or 0) / 60,
            "light_minutes": (sleep.get("lightSleepSeconds") or 0) / 60,
            "rem_minutes": (sleep.get("remSleepSeconds") or 0) / 60,
            "awake_minutes": (sleep.get("awakeSleepSeconds") or 0) / 60,
            "sleep_score": (sleep.get("sleepScores") or {}).get("overall", {}).get("value"),
            "sleep_stress": sleep.get("avgSleepStress"),
            "raw_json": to_json(sleep),
        }
        if self._caps.get("nightly_hrv"):
            hrv_row = fetch_hrv_day(self._garmin, d)
        else:
            hrv_row = {"date": cdate, "raw_json": "{}"}
        if hrv_row.get("last_night_avg_ms") is None and self._caps.get("nocturnal_proxy", True):
            proxy = nocturnal_proxy_index(pd.Series(hr_row), pd.Series(sleep_row))
            if proxy is not None:
                hrv_row["nocturnal_proxy"] = proxy

        return {
            "raw_heart_rate": hr_row,
            "raw_stress": {
                "date": cdate,
                "avg_stress": stats.get("averageStressLevel"),
                "rest_pct": stats.get("restStressPercentage"),
                "high_pct": (stats.get("highStressPercentage") or 0)
                + (stats.get("veryHighStressPercentage") or 0),
                "samples_json": to_json(stress),
            },
            "raw_sleep": sleep_row,
            "raw_body_battery": {
                "date": cdate,
                "bb_high": stats.get("bodyBatteryHighestValue") or bb_day.get("bodyBatteryHighestValue"),
                "bb_low": stats.get("bodyBatteryLowestValue") or bb_day.get("bodyBatteryLowestValue"),
                "charged": stats.get("bodyBatteryChargedValue"),
                "drained": stats.get("bodyBatteryDrainedValue"),
                "timeline_json": to_json(bb_list),
            },
            "raw_activity_daily": {
                "date": cdate,
                "steps": stats.get("totalSteps"),
                "calories": stats.get("totalKilocalories"),
                "active_calories": stats.get("activeKilocalories"),
                "intensity_minutes": (stats.get("moderateIntensityMinutes") or 0)
                + (stats.get("vigorousIntensityMinutes") or 0),
                "sedentary_minutes": (stats.get("sedentarySeconds") or 0) / 60,
            },
            "raw_respiration": {
                "date": cdate,
                "waking_rr": stats.get("avgWakingRespirationValue")
                or resp.get("avgWakingRespirationValue"),
                "sleep_rr": sleep.get("averageRespirationValue"),
            },
            "raw_fitness": {
                "date": cdate,
                "vo2max": vo2.get("vo2MaxPreciseValue") or vo2.get("vo2MaxValue"),
                "readiness_score": readiness.get("score") if isinstance(readiness, dict) else None,
                "morning_readiness": morning.get("score") if isinstance(morning, dict) else None,
                "hrv_status": (hrv or {}).get("status") if isinstance(hrv, dict) else None,
                "training_status": training.get("trainingStatusName"),
                "garmin_only_load": True,
                "load_balance_json": to_json(load_balance),
                "raw_json": to_json(
                    {"training_status": training, "readiness": readiness, "morning": morning, "hrv": hrv}
                ),
            },
            "raw_hrv": hrv_row,
        }

    def fetch_range(self, start, end, skip_dates=None):
        skip_dates = skip_dates or set()
        tables = {name: [] for name in self.TABLES}
        d = start
        while d <= end:
            if d in skip_dates:
                self.stats["skipped_days"] += 1
            else:
                day_data = self.fetch_day(d)
                self.stats["fetched_days"] += 1
                for table_name in self.TABLES:
                    if table_name in day_data and day_data[table_name]:
                        tables[table_name].append(day_data[table_name])
                time.sleep(0.5)
            d += timedelta(days=1)
        return {k: pd.DataFrame(v) for k, v in tables.items() if v}
