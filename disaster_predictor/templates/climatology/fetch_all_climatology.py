#!/usr/bin/env python3
"""
Fetch climatology data for all 10k km² subregions: ERA5 (daily), MODIS (16-day), VIIRS (daily), terrain (static).
Appends to BigQuery per subregion; skips regions already in each table. Verification report and gap refetch at end.
"""

import os
import sys
import time
from pathlib import Path

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


def main():
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise RuntimeError("PROJECT_ID not set")

    modis_end = str(pd.Timestamp(MODIS_CUTOFF_DATE).date())
    steps = [
        ("ERA5 (daily)", fetch_era5_daily, "era5", "1981-01-01", "2024-12-31", {}, CLIMATOLOGY_ERA5_MAX_WORKERS, None, True),
        ("MODIS (16-day)", modis_16day_combined_df, "modis", "2000-02-18", modis_end, {}, CLIMATOLOGY_MODIS_MAX_WORKERS, None, True),
        ("VIIRS (daily)", fetch_viirs_daily, "viirs", "2012-01-01", "2024-12-31", {}, CLIMATOLOGY_VIIRS_MAX_WORKERS, CLIMATOLOGY_SLEEP_S, True),
        ("Terrain (static)", fetch_terrain_data, "terrain_static", "2000-01-01", "2000-01-01", {}, CLIMATOLOGY_TERRAIN_MAX_WORKERS, 3, False),
    ]

    print("=" * 80)
    print("CLIMATOLOGY DATA COLLECTION FOR 10k km² SUBREGIONS")
    print("=" * 80)
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
        era5_start="1981-01-01",
        era5_end="2024-12-31",
        modis_start="2000-02-18",
        modis_end=modis_end,
        viirs_start="2012-01-01",
        viirs_end="2024-12-31",
    )

    _verify_and_report(project_id, DATASET_ID, TABLES, expected_regions)


if __name__ == "__main__":
    main()
