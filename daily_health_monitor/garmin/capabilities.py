"""Probe Garmin Connect endpoints once and cache what this account/device supports."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict

CACHE_VERSION = 1


def _has_nightly_hrv(raw: Any) -> bool:
    if not isinstance(raw, dict) or not raw:
        return False
    for key in ("lastNightAvg", "lastNightAverage", "weeklyAvg", "weeklyAverage"):
        if raw.get(key) is not None:
            return True
    return False


def _has_sleep_score(sleep_raw: Any) -> bool:
    if not isinstance(sleep_raw, dict):
        return False
    sleep = sleep_raw.get("dailySleepDTO") or {}
    scores = sleep.get("sleepScores") or {}
    overall = scores.get("overall") or {}
    return overall.get("value") is not None


def _readiness_score(raw: Any):
    if isinstance(raw, dict):
        return raw.get("score")
    return None


def probe_garmin_capabilities(garmin_client, sample_days: int = 3) -> Dict[str, Any]:
    """Call the endpoints the pipeline relies on for recent dates."""
    api = garmin_client.api
    call = garmin_client.call
    nightly_hrv = False
    sleep_score = False
    training_readiness = False
    morning_readiness = False
    training_status = False
    vo2max = False
    hr_sample_interval_sec = None

    for i in range(sample_days):
        d = (date.today() - timedelta(days=i)).isoformat()
        hrv_raw = call(api.get_hrv_data, d) or {}
        sleep_raw = call(api.get_sleep_data, d) or {}
        readiness = call(api.get_training_readiness, d)
        morning = call(api.get_morning_training_readiness, d) or {}
        training = call(api.get_training_status, d) or {}

        nightly_hrv = nightly_hrv or _has_nightly_hrv(hrv_raw)
        sleep_score = sleep_score or _has_sleep_score(sleep_raw)
        training_readiness = training_readiness or _readiness_score(readiness) is not None
        morning_readiness = morning_readiness or _readiness_score(morning) is not None
        training_status = training_status or bool(
            training.get("trainingStatusName")
            or (training.get("mostRecentTrainingStatus") or {}).get("trainingStatusName")
        )
        vo2 = (training.get("mostRecentVO2Max") or {}).get("generic") or {}
        vo2max = vo2max or bool(vo2.get("vo2MaxPreciseValue") or vo2.get("vo2MaxValue"))

        if hr_sample_interval_sec is None and i == 1:
            hr = call(api.get_heart_rates, d) or {}
            vals = hr.get("heartRateValues") or []
            if len(vals) >= 2:
                hr_sample_interval_sec = int((vals[1][0] - vals[0][0]) / 1000)

    return {
        "version": CACHE_VERSION,
        "nightly_hrv": nightly_hrv,
        "sleep_score": sleep_score,
        "sleep_stress": sleep_score,  # same API object as sleep_score on Connect
        "training_readiness": training_readiness,
        "morning_readiness": morning_readiness,
        "training_status": training_status,
        "vo2max": vo2max,
        "hrv_status": nightly_hrv,
        "hr_sample_interval_sec": hr_sample_interval_sec,
        "nocturnal_proxy": hr_sample_interval_sec is not None,
    }


def load_capabilities(cache_path: Path) -> Dict[str, Any] | None:
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text())
    except (json.JSONDecodeError, OSError):
        return None
    if data.get("version") != CACHE_VERSION:
        return None
    return data


def save_capabilities(cache_path: Path, caps: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(caps, indent=2, sort_keys=True))


def get_capabilities(garmin_client, cache_path: Path, refresh: bool = False) -> Dict[str, Any]:
    if not refresh:
        cached = load_capabilities(cache_path)
        if cached is not None:
            return cached
    caps = probe_garmin_capabilities(garmin_client)
    save_capabilities(cache_path, caps)
    return caps
