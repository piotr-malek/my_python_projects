import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from utils.bq_utils import load_from_bigquery, save_to_bigquery
from ml_training.data_preparation.feature_engineering import compute_climatology
from ml_training.data_preparation.load_training_data import (
    load_era5_data,
    load_modis_data,
    load_viirs_data,
    merge_datasets,
    load_terrain_data,
    load_region_descriptors,
    load_river_discharge_data,
)
from ml_training.config import (
    PROJECT_ID,
    CLIMATOLOGY_DATASET,
    ERA5_START,
    ERA5_END,
    MODIS_START,
    MODIS_END,
    VIIRS_START,
    VIIRS_END,
)

def load_climatology_from_bq() -> pd.DataFrame:
    """Load pre-computed climatology from BigQuery if it exists."""
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{CLIMATOLOGY_DATASET}.climatology_monthly`
    ORDER BY region, month
    """
    
    try:
        df = load_from_bigquery(query, project_id=PROJECT_ID)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    
    return pd.DataFrame()


def compute_and_save_climatology():
    """Compute climatology from all historical data and save to BigQuery."""
    print("Loading all historical data for climatology computation...")
    
    era5_all = load_era5_data(start_date=ERA5_START, end_date=ERA5_END)
    modis_all = load_modis_data(start_date=MODIS_START, end_date=MODIS_END)
    viirs_all = load_viirs_data(start_date=VIIRS_START, end_date=VIIRS_END)
    terrain_all = load_terrain_data()
    descriptors_all = load_region_descriptors()
    
    print("Merging datasets...")
    merged_all = merge_datasets(
        era5_all, modis_all, viirs_all, terrain_all, descriptors_all
    )

    discharge_start = "1984-01-01"
    discharge_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(
        f"Loading river discharge (GloFAS + daily_ingestion, {discharge_start} → {discharge_end})..."
    )
    discharge_all = load_river_discharge_data(
        regions=None,
        start_date=discharge_start,
        end_date=discharge_end,
    )
    if not discharge_all.empty:
        merged_all["date"] = pd.to_datetime(merged_all["date"]).dt.normalize()
        discharge_all = discharge_all.copy()
        discharge_all["date"] = pd.to_datetime(discharge_all["date"]).dt.normalize()
        discharge_all = discharge_all.drop_duplicates(subset=["date", "region"], keep="last")
        merged_all = merged_all.merge(
            discharge_all[["date", "region", "river_discharge"]],
            on=["date", "region"],
            how="left",
        )
        print(
            f"  Merged river_discharge: {discharge_all['region'].nunique()} regions, "
            f"{len(discharge_all)} rows"
        )

    print("Computing climatology (all metrics including river_discharge)...")
    climatology = compute_climatology(merged_all)

    print(f"Saving climatology to BigQuery ({len(climatology)} rows)...")
    save_to_bigquery(
        climatology,
        project_id=PROJECT_ID,
        dataset_id=CLIMATOLOGY_DATASET,
        table_id="climatology_monthly",
        mode="WRITE_TRUNCATE"
    )
    
    print("✓ Climatology saved successfully")
    return climatology


if __name__ == "__main__":
    compute_and_save_climatology()
