#!/usr/bin/env python3
"""
Create subregions for a list of regions (subdivision + optional BQ append).

For adding new regions from a JSON spec, use the new workflow:
  python utils/regions_creation/validate_regions_json.py <input_json>
  python utils/regions_creation/add_regions_from_verified_json.py <verified_json>
  
See utils/regions_creation/README.md for full documentation.
"""

import os
import sys
from pathlib import Path
from typing import Union, List, Dict
from dotenv import load_dotenv
import ee

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)
sys.path.insert(0, str(ROOT))

from utils.earth_engine_utils import (
    init_ee,
    KEY_PATH,
    get_region_geometry_actual,
    subdivide_region_to_target_size,
    regions_ee,
)


def create_subregions(
    regions: Union[List[str], Dict[str, ee.Geometry]],
    target_size_km2: float = 10_000,
    save_to_bq: bool = False,
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
) -> Dict[str, ee.Geometry]:
    """
    Create subregions for a list of regions using the same subdivision logic.

    Args:
        regions: Either:
            - List of region names (strings) that exist in regions_ee() or get_region_geometry_actual()
            - Dict mapping region_name -> ee.Geometry (for custom regions)
        target_size_km2: Target size for each subregion (default: 10,000 km²)
        save_to_bq: If True, save subregion info to BigQuery
        project_id: BigQuery project ID (uses env var if None)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID

    Returns:
        Dict mapping subregion_id -> ee.Geometry

    Example:
        # Using existing region names
        subregions = create_subregions(["Kruger_NP", "Etosha_NP"])

        # Using custom geometries
        custom_regions = {
            "New_Region": ee.Geometry.Rectangle([20.0, -30.0, 25.0, -25.0]),
            "Another_Region": ee.Geometry.Point([22.0, -27.0]).buffer(50_000)
        }
        subregions = create_subregions(custom_regions)
    """
    try:
        ee.Number(1).getInfo()
    except Exception:
        init_ee(KEY_PATH)

    if isinstance(regions, list):
        region_dict = {}
        for region_name in regions:
            try:
                geom = get_region_geometry_actual(region_name)
            except Exception:
                geom = regions_ee().get(region_name)
                if geom is None:
                    raise ValueError(
                        f"Region '{region_name}' not found in regions_ee() or get_region_geometry_actual()"
                    )
            region_dict[region_name] = geom
    elif isinstance(regions, dict):
        region_dict = {}
        for region_name, geom in regions.items():
            if geom is None:
                try:
                    geom = get_region_geometry_actual(region_name)
                except Exception:
                    geom = regions_ee().get(region_name)
                    if geom is None:
                        raise ValueError(
                            f"Region '{region_name}' not found in regions_ee() or get_region_geometry_actual()"
                        )
            elif not isinstance(geom, ee.Geometry):
                raise TypeError(
                    f"Geometry for '{region_name}' must be an ee.Geometry object"
                )
            region_dict[region_name] = geom
    else:
        raise TypeError(
            "regions must be either a list of strings or a dict mapping region_name -> ee.Geometry (or None to look up)"
        )

    all_subregions = {}
    print(f"Creating subregions for {len(region_dict)} region(s)...")
    print(f"Target size: {target_size_km2:,} km²")
    print()

    for region_name, region_geom in region_dict.items():
        print(f"Processing {region_name}...")
        subregions = subdivide_region_to_target_size(
            region_name,
            region_geom,
            target_size_km2=target_size_km2,
        )
        all_subregions.update(subregions)
        print(f"  Created {len(subregions)} subregion(s)")

    print()
    print(f"Total subregions created: {len(all_subregions)}")

    if save_to_bq:
        from templates.regions_creation.region_storage import save_subregions_to_bq
        save_subregions_to_bq(
            all_subregions,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

    return all_subregions




if __name__ == "__main__":
    print(
        "Use the JSON-based workflow to add new regions (verify + create + append to BQ):"
    )
    print("  python utils/regions_creation/validate_regions_json.py <input_json>")
    print("  python utils/regions_creation/add_regions_from_verified_json.py <verified_json>")
    print()
    print(
        "Or: from templates.regions_creation import create_subregions"
    )
    sys.exit(0)
