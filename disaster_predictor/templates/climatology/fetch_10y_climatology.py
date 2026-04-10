#!/usr/bin/env python3
"""
Fetch climatology data for 10k km² subregions - SHORT PERIOD VERSION (10 years).

This version fetches only the last 10 years of data for faster onboarding of new regions.
Use this for:
- New training regions (faster validation)
- Regions where short-period climatology is sufficient
- Initial data collection before deciding on full historical fetch

Fetches ERA5 (daily), MODIS (16-day), VIIRS (daily), and terrain (static).
Appends to BigQuery per subregion; skips regions already in each table. Verification report and gap refetch at end.
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env", override=False)

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.earth_engine_utils import init_ee, KEY_PATH, get_subregions_from_bq
from utils.bq_utils import load_from_bigquery
from risk_assessment.constants import MODIS_CUTOFF_DATE

from utils.incremental_save_utils import (
    process_with_per_subregion_save,
    get_existing_regions,
    SLEEP_BETWEEN_DATASETS_DAILY,
    CLIMATOLOGY_ERA5_MAX_WORKERS,
    CLIMATOLOGY_MODIS_MAX_WORKERS,
    CLIMATOLOGY_VIIRS_MAX_WORKERS,
    CLIMATOLOGY_TERRAIN_MAX_WORKERS,
    CLIMATOLOGY_SLEEP_S,
)

from utils.datasets.era5_utils import fetch_era5_daily
from utils.datasets.viirs_utils import fetch_viirs_daily
from utils.datasets.modis_utils import modis_16day_combined_df
from utils.datasets.terrain_utils import fetch_terrain_data

from gap_refetch import run_gap_detection_and_refetch

DATASET_ID = "climatology"
TABLES = ["era5", "modis", "viirs", "terrain_static"]
YEARS_BACK = 10


def _verify_and_report(
    project_id: str,
    dataset_id: str,
    tables: list[str],
    expected_regions: set[str],
) -> None:
    all_ok = True
    print()
    print("=" * 80)
    print("VERIFICATION REPORT")
    print("=" * 80)
    print(f"Expected regions: {len(expected_regions)}")
    print()
    for table_id in tables:
        try:
            q_r = f"SELECT DISTINCT region FROM `{project_id}.{dataset_id}.{table_id}`"
            q_c = f"SELECT COUNT(*) as n FROM `{project_id}.{dataset_id}.{table_id}`"
            df_r = load_from_bigquery(q_r, project_id=project_id)
            df_c = load_from_bigquery(q_c, project_id=project_id)
            actual = set(df_r["region"].unique()) if df_r is not None and not df_r.empty else set()
            total = int(df_c["n"].iloc[0]) if df_c is not None and not df_c.empty else 0
        except Exception as e:
            print(f"  {table_id}: ERROR - {e}")
            actual, total = set(), 0
            all_ok = False
            continue
        missing = expected_regions - actual
        extra = actual - expected_regions
        print(f"  {table_id}: regions {len(actual)}, rows {total}, missing {len(missing)}")
        if missing:
            all_ok = False
            print(f"    {sorted(missing)[:20]}{'...' if len(missing) > 20 else ''}")
        if extra:
            print(f"    extra (not in regions_info): {len(extra)}")
        print()
    print("  All tables OK." if all_ok else "  Some tables missing regions; re-run to fill gaps.")
    print("=" * 80)
    print()


def _calculate_10y_dates():
    """Calculate 10-year date ranges based on dataset end dates.
    
    Simply subtract 10 years from the end date, keeping the same month and day.
    E.g., 2024-12-31 → 2014-12-31, 2024-08-31 → 2014-08-31
    """
    # End dates (same as fetch_all_climatology.py)
    era5_end = datetime(2024, 12, 31).date()
    viirs_end = datetime(2024, 12, 31).date()
    modis_end = pd.Timestamp(MODIS_CUTOFF_DATE).date()
    
    # Calculate start dates by subtracting 10 years, keeping same month and day
    era5_start = datetime(era5_end.year - YEARS_BACK, era5_end.month, era5_end.day).date()
    viirs_start = datetime(viirs_end.year - YEARS_BACK, viirs_end.month, viirs_end.day).date()
    modis_start = datetime(modis_end.year - YEARS_BACK, modis_end.month, modis_end.day).date()
    
    # VIIRS starts in 2012, so adjust if needed
    viirs_start = max(viirs_start, datetime(2012, 1, 1).date())
    
    return {
        "era5": (era5_start.strftime("%Y-%m-%d"), era5_end.strftime("%Y-%m-%d")),
        "modis": (modis_start.strftime("%Y-%m-%d"), modis_end.strftime("%Y-%m-%d")),
        "viirs": (viirs_start.strftime("%Y-%m-%d"), viirs_end.strftime("%Y-%m-%d")),
    }


def main():
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise RuntimeError("PROJECT_ID not set")

    # Calculate 10-year date ranges
    dates = _calculate_10y_dates()
    modis_end = dates["modis"][1]
    
    steps = [
        ("ERA5 (daily)", fetch_era5_daily, "era5", dates["era5"][0], dates["era5"][1], {}, CLIMATOLOGY_ERA5_MAX_WORKERS, None, True),
        ("MODIS (16-day)", modis_16day_combined_df, "modis", dates["modis"][0], dates["modis"][1], {}, CLIMATOLOGY_MODIS_MAX_WORKERS, None, True),
        ("VIIRS (daily)", fetch_viirs_daily, "viirs", dates["viirs"][0], dates["viirs"][1], {}, CLIMATOLOGY_VIIRS_MAX_WORKERS, CLIMATOLOGY_SLEEP_S, True),
        ("Terrain (static)", fetch_terrain_data, "terrain_static", "2000-01-01", "2000-01-01", {}, CLIMATOLOGY_TERRAIN_MAX_WORKERS, 3, False),
    ]

    print("=" * 80)
    print("CLIMATOLOGY DATA COLLECTION FOR 10k km² SUBREGIONS (10-YEAR PERIOD)")
    print("=" * 80)
    print("⚠️  SHORT PERIOD VERSION - Last 10 years only")
    print(f"   ERA5: {dates['era5'][0]} to {dates['era5'][1]}")
    print(f"   MODIS: {dates['modis'][0]} to {dates['modis'][1]}")
    print(f"   VIIRS: {dates['viirs'][0]} to {dates['viirs'][1]}")
    print()
    print("ERA5 daily | MODIS 16-day | VIIRS daily | Terrain static. Append per subregion; skip existing.")
    print("Workers: ERA5=%s, MODIS=%s, VIIRS=%s, Terrain=%s" % (CLIMATOLOGY_ERA5_MAX_WORKERS, CLIMATOLOGY_MODIS_MAX_WORKERS, CLIMATOLOGY_VIIRS_MAX_WORKERS, CLIMATOLOGY_TERRAIN_MAX_WORKERS))
    print()

    init_ee(KEY_PATH)
    subregions = get_subregions_from_bq()
    expected_regions = set(subregions.keys())
    print(f"Subregions loaded: {len(subregions)}")
    print()

    for i, (label, fetch_fn, table_id, start_date, end_date, fetch_kwargs, max_workers, sleep_s, normalize_date) in enumerate(steps, 1):
        print("=" * 80)
        print(f"STEP {i}/4: {label}")
        print("=" * 80)
        
        # Skip regions that already exist in this table
        existing_regions = get_existing_regions(project_id, DATASET_ID, table_id)
        
        # Filter out regions that already exist
        remaining_subregions = {
            rid: geom for rid, geom in subregions.items()
            if rid not in existing_regions
        }
        
        if not remaining_subregions:
            continue
        
        kwargs = dict(
            subregions=remaining_subregions,
            fetch_function=fetch_fn,
            project_id=project_id,
            dataset_id=DATASET_ID,
            table_id=table_id,
            start_date=start_date,
            end_date=end_date,
            fetch_kwargs=fetch_kwargs,
            skip_existing=False,  # Already filtered above
            max_workers=max_workers,
            normalize_date=normalize_date,
        )
        if sleep_s is not None:
            kwargs["sleep_s"] = sleep_s
        _, had_work = process_with_per_subregion_save(**kwargs)
        print()
        if had_work:
            time.sleep(SLEEP_BETWEEN_DATASETS_DAILY)
        print()

    _verify_and_report(project_id, DATASET_ID, TABLES, expected_regions)

    run_gap_detection_and_refetch(
        subregions=subregions,
        project_id=project_id,
        dataset_id=DATASET_ID,
        era5_start=dates["era5"][0],
        era5_end=dates["era5"][1],
        modis_start=dates["modis"][0],
        modis_end=modis_end,
        viirs_start=dates["viirs"][0],
        viirs_end=dates["viirs"][1],
    )

    _verify_and_report(project_id, DATASET_ID, TABLES, expected_regions)


if __name__ == "__main__":
    main()
