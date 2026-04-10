"""
Configuration file for ML model training.

This file contains all constants, table names, date ranges, and hyperparameters
used throughout the ML training pipeline. Centralizing these makes it easy to
update settings without hunting through code.
"""

from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env", override=False)

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# BigQuery Configuration
PROJECT_ID = os.getenv("PROJECT_ID")
CLIMATOLOGY_DATASET = "climatology"
REGIONS_DATASET = "google_earth"
REGIONS_TABLE = "regions_info"  # Base table for initial training

# STRING descriptor columns used in ML features.
# v1.1.1 drops basin_type (currently empty) and keeps soil texture via one-hot columns.
REGION_DESCRIPTOR_STRING_COLUMNS = ("soil_texture_class",)

# Table Names (all regions merged into main climatology tables)
ERA5_TABLE = "era5"
MODIS_TABLE = "modis"
VIIRS_TABLE = "viirs"
TERRAIN_TABLE = "terrain_static"
GLC_TABLE = "glc"
GLOBAL_FLOOD_DB_TABLE = "global_flood_db"
WORLDFLOODS_TABLE = "worldfloods"

# Date Ranges by Dataset
ERA5_START = "1981-01-01"
ERA5_END = "2024-12-31"

MODIS_START = "2000-02-18"
MODIS_END = "2024-08-31"  # MODIS discontinued

VIIRS_START = "2012-01-01"
VIIRS_END = "2024-12-31"

GLC_START = "1970-01-01"
GLC_END = "2019-12-31"

# Training Date Ranges by Disaster Type
FIRE_TRAIN_START = "2012-01-01"  # VIIRS validation range
FIRE_TRAIN_END = "2024-12-31"

DROUGHT_TRAIN_START = "1981-01-01"  # Full ERA5 range
DROUGHT_TRAIN_END = "2024-12-31"

FLOOD_TRAIN_START = "1981-01-01"  # Full ERA5 range
FLOOD_TRAIN_END = "2024-12-31"

LANDSLIDE_TRAIN_START_ORIGINAL = "1981-01-01"  # Original regions
LANDSLIDE_TRAIN_END_ORIGINAL = "2019-12-31"

LANDSLIDE_TRAIN_START_NEW = "2014-01-01"  # New regions
LANDSLIDE_TRAIN_END_NEW = "2024-12-31"

# Model Configuration
HAZARD_MODEL_VERSIONS = {
    'fire': '1.3',
    'drought': '1.3',
    'landslide': '1.3',
    'flood': '1.2'
}
MODELS_DIR = PROJECT_ROOT / "models"
REGION_DESCRIPTOR_ENCODINGS_FILENAME = "region_descriptor_encodings_v{version}.json"

# Random Forest Hyperparameters (v1 - MVP)
RF_N_ESTIMATORS = 200
RF_MAX_DEPTH = 20
RF_MIN_SAMPLES_SPLIT = 10
RF_MIN_SAMPLES_LEAF = 4
RF_CLASS_WEIGHT = "balanced"  # Handles class imbalance automatically
RF_RANDOM_STATE = 42

# Feature Engineering Configuration
TEMPORAL_WINDOWS = {
    "precip_3d": 3,
    "precip_7d": 7,
    "precip_30d": 30,
    "temp_7d": 7,
    "sm_14d": 14,
    "river_discharge_7d": 7,
    "ndvi_30d": 30,
    "hotspot_7d": 7,
    "frp_7d": 7,
}

# River discharge sources (flood / landslide); ERA5 runoff is not used as an ML feature.
DISCHARGE_CLIMATOLOGY_TABLE = "copernicus_glofas"
DISCHARGE_DAILY_DATASET = "daily_ingestion"
DISCHARGE_DAILY_TABLE = "openmeteo_weather"

# Climatology Percentiles to Compute
CLIMATOLOGY_PERCENTILES = [20, 80, 95]  # p20, p80, p95

# Missing Data Configuration
MISSING_DATA_THRESHOLD = 0.5  # Drop rows with >50% missing required features
MODIS_FORWARD_FILL_WINDOW = 16  # Maximum days to forward-fill MODIS data

# Feature Correlation Threshold
CORRELATION_THRESHOLD = 0.90  # v1.3: tighter drop of redundant features for hydrology signal

# Train/Test Split
TRAIN_TEST_SPLIT = 0.8  # 80% training, 20% test (chronological split)

# Evaluation Metrics
EVALUATION_METRICS = [
    "precision",
    "recall",
    "f1_score",
    "weighted_f1",
    "roc_auc",  # Binary: risk (1-3) vs no-risk (0)
    "confusion_matrix",
]

# Required Features (cannot be missing)
REQUIRED_FEATURES = [
    "date",
    "region",
    "temp_2m_mean_C",
    "precipitation_sum_mm",
    "sm1_mean",
    "sm2_mean",
]

# Optional Features (can have missing values)
OPTIONAL_FEATURES = {
    "fire": ["ndvi_mean", "burned_area_pct", "hotspot_count", "frp_mean"],
    "drought": ["ndvi_mean"],
    "flood": ["river_discharge"],
    "landslide": ["river_discharge"],
}
