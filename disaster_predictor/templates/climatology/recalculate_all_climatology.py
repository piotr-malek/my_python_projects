#!/usr/bin/env python3
"""
Recalculate all climatology for all regions (including new expansion regions).

climatology_monthly: from ERA5, MODIS, VIIRS, terrain_static, copernicus_glofas.
  Same pattern as ERA5: raw daily data → monthly percentiles (temp_p90, river_discharge_p20/p80/p95, etc.).
  Use for fire, drought, flood/landslide training and modifiers.

Run from project root. Ensure climatology.copernicus_glofas has river_discharge history.
"""
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
# Airflow include for ml_training modules
include = project_root / "airflow" / "include"
if include.exists():
    sys.path.insert(0, str(include))

from dotenv import load_dotenv
load_dotenv(project_root / ".env", override=False)


def main():
    print("=== Recomputing climatology_monthly ===")
    print("Using tables: climatology.era5, climatology.modis, climatology.viirs,")
    print("             climatology.terrain_static, climatology.copernicus_glofas")
    try:
        from ml_training.data_preparation.climatology_utils import compute_and_save_climatology
        clim = compute_and_save_climatology()
        regions_count = clim["region"].nunique() if clim is not None and not clim.empty else 0
        print(f"  climatology_monthly: {len(clim)} rows, {regions_count} regions")
    except Exception as e:
        print(f"  Failed: {e}")
        return 1

    print("\n✓ Climatology recalculation done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
