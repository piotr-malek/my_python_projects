import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=_ROOT / ".env", override=False)
except ImportError:
    pass

PROJECT_ID = os.getenv("PROJECT_ID")
RISK_DATASET = "risk_assessment"
REGIONS_DATASET = "google_earth"
REGIONS_TABLE = "regions_info"
INGESTION_DATASET = "daily_ingestion"

HAZARDS = ("flood", "fire", "drought", "landslide")
HAZARD_TIE_ORDER = ("flood", "fire", "drought", "landslide")

CACHE_TTL_SEC = int(os.getenv("RISK_MONITOR_CACHE_TTL", "300"))

# Spec: gray / yellow / orange / red — no green
RISK_HEX = {
    0: "#9e9e9e",
    1: "#fdd835",
    2: "#fb8c00",
    3: "#c62828",
}

METRIC_LABELS = {
    "precipitation_7d_sum_mm": "Precipitation (7d sum)",
    "temperature_7d_max_C": "Temperature (7d max)",
    "sm1_mean": "Soil moisture (layer 1)",
    "precipitation_30d_sum_mm": "Precipitation (30d sum)",
    "sm1_mean_14d": "Soil moisture (14d mean)",
    "river_discharge_7d_sum_m3s": "River discharge (7d sum)",
}
