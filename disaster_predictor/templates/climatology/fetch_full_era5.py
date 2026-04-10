#!/usr/bin/env python3
"""
Fetch full historical ERA5 climatology for 8 key regions.

- ERA5 only (daily), no MODIS/VIIRS/terrain.
- Looks up the earliest date already present in BigQuery for each target region
  in `climatology.era5` (expansion data was merged into era5).
- Uses the same ERA5 start date as `fetch_all_climatology.py` (1981-01-01).
- For each region, fetches from 1981-01-01 up to one day before that region's
  earliest existing date, so we fetch the full history without re-fetching what
  is already in BQ. After merge (action 2), the 8 key regions already have
  10 years (e.g. 2014-12-31 → 2024-12-30) in era5; this script fetches only
  1981 → 2014-12-30 and appends, so the existing 10 years are never re-fetched.
- Uses the same workers / sleep / incremental-save settings as
  `templates/climatology/fetch_10y_climatology.py` for ERA5.

Intended usage:
- Run after ERA5 has some coverage for these regions (e.g. 10y subset),
  so this script backfills the earlier years.
- Appends directly to `climatology.era5`.
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dotenv import load_dotenv


# Load .env from project root (same pattern as other templates)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env", override=False)

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.earth_engine_utils import init_ee, KEY_PATH, get_subregions_from_bq  # type: ignore
from utils.bq_utils import load_from_bigquery  # type: ignore

from utils.incremental_save_utils import (  # type: ignore
    process_with_per_subregion_save,
    CLIMATOLOGY_ERA5_MAX_WORKERS,
    CLIMATOLOGY_SLEEP_S,
    SLEEP_BETWEEN_DATASETS_DAILY,
)

from utils.datasets.era5_utils import fetch_era5_daily  # type: ignore


DATASET_ID = "climatology"
TABLE_ID = "era5"

# Same ERA5 date range convention as in fetch_all_climatology.py
ERA5_START_DATE = "1981-01-01"
ERA5_END_DATE = "2024-12-31"

# One-off full ERA5 for exactly these 8 regions (see docs/expansion_implementation_plan.md §3)
TARGET_REGIONS = [
    "Himalayan_Foothills_03",
    "Himalayan_Foothills_11",
    "Himalayan_Foothills_15",
    "Himalayan_Foothills_18",
    "Himalayan_Foothills_20",
    "Manipur_Hills_02",
    "Central_Assam_01",
    "Bhutan_04",
]


def _get_region_min_existing_date(project_id: str, region: str) -> Optional[datetime]:
    """
    Get the earliest existing ERA5 date in BQ for a single region from climatology.era5.
    """
    query = f"""
    SELECT MIN(date) AS min_date
    FROM `{project_id}.{DATASET_ID}.{TABLE_ID}`
    WHERE region = '{region}'
    """
    try:
        df = load_from_bigquery(query, project_id=project_id)
    except Exception as e:
        print(f"⚠ Could not load existing ERA5 dates from {DATASET_ID}.{TABLE_ID}: {e}")
        return None

    if df is None or df.empty or "min_date" not in df.columns:
        return None

    min_val = df["min_date"].iloc[0]
    if pd.isna(min_val):
        return None

    return pd.to_datetime(min_val).to_pydatetime()


def main() -> None:
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise RuntimeError("PROJECT_ID not set")

    # Initialize Earth Engine
    init_ee(KEY_PATH)

    # Load all subregions from BQ and filter to our 8 regions
    subregions_all = get_subregions_from_bq()
    subregions = {
        rid: geom for rid, geom in subregions_all.items()
        if rid in TARGET_REGIONS
    }

    if not subregions:
        raise RuntimeError(
            f"None of the target regions found in BigQuery regions tables. "
            f"Expected one or more of: {', '.join(TARGET_REGIONS)}"
        )

    print(f"Subregions to process: {len(subregions)} (filtered to target regions)")
    print()

    print("=" * 80)
    print("CLIMATOLOGY ERA5 BACKFILL FOR 8 KEY REGIONS")
    print("=" * 80)
    print(f"Regions ({len(TARGET_REGIONS)}): {', '.join(TARGET_REGIONS)}")
    print(f"Global ERA5 range available: {ERA5_START_DATE} to {ERA5_END_DATE}")
    print()
    print(f"ERA5 daily only. Append to {DATASET_ID}.{TABLE_ID}; no MODIS/VIIRS/terrain.")
    print(f"Workers: ERA5={CLIMATOLOGY_ERA5_MAX_WORKERS}")
    print()

    # Process each region separately so that we can respect per-region
    # existing coverage (different MIN(date) per region if needed).
    for idx, (region_id, geom) in enumerate(subregions.items(), 1):
        print("=" * 80)
        print(f"[{idx}/{len(subregions)}] Region: {region_id}")
        print("=" * 80)

        earliest = _get_region_min_existing_date(project_id, region_id)

        if earliest is None:
            # No existing rows for this region in either table:
            # fetch the full history range.
            start_date = ERA5_START_DATE
            end_date = ERA5_END_DATE
            print(f"  No existing ERA5 rows for {region_id} in {DATASET_ID}.{TABLE_ID}.")
            print(f"  Will fetch full range {start_date} to {end_date}.")
        else:
            cutoff = earliest - timedelta(days=1)
            start_dt = datetime.strptime(ERA5_START_DATE, "%Y-%m-%d")
            if cutoff < start_dt:
                print(f"  Existing ERA5 data for {region_id} already covers from or before {ERA5_START_DATE}.")
                print(f"  Earliest existing date: {earliest.date()}")
                print("  Nothing to backfill for this region; skipping.")
                print()
                continue
            start_date = ERA5_START_DATE
            end_date = cutoff.strftime("%Y-%m-%d")
            print(f"  Existing ERA5 data detected for {region_id}.")
            print(f"  Earliest existing date in {TABLE_ID}: {earliest.date()}")
            print(f"  Backfill range for this region: {start_date} to {end_date}")

        # Single-region subregion dict for this call
        region_subregions = {region_id: geom}

        kwargs = dict(
            subregions=region_subregions,
            fetch_function=fetch_era5_daily,
            project_id=project_id,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            start_date=start_date,
            end_date=end_date,
            fetch_kwargs={},
            skip_existing=False,  # we avoid overlap via the per-region date range
            max_workers=CLIMATOLOGY_ERA5_MAX_WORKERS,
            normalize_date=True,
        )

        _, had_work = process_with_per_subregion_save(**kwargs)
        print()
        if had_work:
            # Keep same dataset-to-dataset sleep convention as other templates,
            # even though we're looping per region here.
            print(f"Sleeping {SLEEP_BETWEEN_DATASETS_DAILY}s before next region...")
            time.sleep(SLEEP_BETWEEN_DATASETS_DAILY)
        print()

    print("=" * 80)
    print("ERA5 backfill for key regions completed.")
    print(f"Data appended to {project_id}.{DATASET_ID}.{TABLE_ID}.")
    print("=" * 80)


if __name__ == "__main__":
    main()

