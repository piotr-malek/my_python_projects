#!/usr/bin/env python3
"""
Script 2: Add Regions from Verified JSON

Takes a verified JSON file (from validate_regions_json.py) and adds all regions
to BigQuery with proper subdivision logic.

Features:
- Uses specified subregions as base units when found in GAUL
- Subdivides subregions > 12k km² into ~10k chunks
- Falls back to parent subdivision if no subregions found
- Simplified naming (e.g., "Alto_Alegre" instead of "Amazon_Basin_Roraima_Brazil_Alto_Alegre_Municipality")
- Comprehensive final report

Usage:
    python utils/regions_creation/add_regions_from_verified_json.py <verified_json> [--table-id <table>] [--dry-run]
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
import unicodedata

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / ".env", override=False)

import ee
from utils.earth_engine_utils import init_ee, KEY_PATH, get_info_with_timeout
from utils.regions_creation.gaul_utils import (
    normalize as _normalize,
    simplify_region_name as _simplify_region_name,
    gaul_union_for_region as _gaul_union_for_region,
    bbox_geometry as _bbox_geometry,
    geom_info as _geom_info,
    GAUL_ASSET,
    GEE_TIMEOUT
)
from templates.regions_creation.region_storage import save_subregions_to_bq

# Import subdivision function
from utils.earth_engine_utils import subdivide_region_to_target_size


def _process_specified_subregions(
    parent_region: str,
    parent_geom: ee.Geometry,
    region_cfg: dict,
    specified_subregions: list,
    target_size_km2: float,
    get_info_fn: Any,
) -> dict:
    """
    Process specified subregions: find each in GAUL, check area, subdivide if needed.
    
    Returns dict mapping subregion_id -> ee.Geometry
    """
    all_subregions = {}
    country = region_cfg.get("country", "")
    bbox = region_cfg.get("bbox", [])
    overrides_global = region_cfg.get("gaul_overrides") or {}
    
    # Handle nested structure (e.g., {"parent": "West Kalimantan", "areas": [...]})
    # or flat structure from verified JSON
    flat_subregions = []
    for sub in specified_subregions:
        if isinstance(sub, dict):
            if "parent" in sub:
                # Nested structure
                areas = sub.get("areas", [])
                for area in areas:
                    area_name = area.get("name", "")
                    if area_name:
                        flat_subregions.append({"name": area_name, "parent_group": sub.get("parent")})
            elif "name" in sub:
                # Flat structure from verified JSON
                flat_subregions.append({"name": sub["name"]})
        else:
            # String
            flat_subregions.append({"name": str(sub)})
    
    print(f"  Found {len(flat_subregions)} subregion(s) to process")
    
    for i, sub_spec in enumerate(flat_subregions, 1):
        sub_name = sub_spec["name"]
        print(f"  [{i}/{len(flat_subregions)}] Processing: {sub_name}")
        
        # Try to find in GAUL
        sub_geom = None
        if country and bbox:
            overrides = overrides_global.get(sub_name) or {}
            union, matched, missing = _gaul_union_for_region(
                country, bbox, [sub_name], overrides, get_info_fn
            )
            if union is not None:
                sub_geom = union
                print(f"    ✓ Found in GAUL")
            else:
                print(f"    ⚠ Not found in GAUL")
        
        # If not found in GAUL, skip this subregion
        if sub_geom is None:
            print(f"    ⚠ Skipping (not in GAUL)")
            continue
        
        # Check area
        try:
            area_info = _geom_info(sub_geom, get_info_fn)
            area_km2 = area_info["area_km2"]
            print(f"    Area: {area_km2:,.0f} km²")
            
            # Simplify the subregion name for use as parent
            simple_name = _simplify_region_name(sub_name)
            
            # Only subdivide if area > ~12k km² (to allow 2+ subregions)
            if area_km2 > target_size_km2 * 1.2:  # > 12k km²
                print(f"    → Subdividing (area > {target_size_km2 * 1.2:,.0f} km²)")
                subdivided = subdivide_region_to_target_size(
                    simple_name,
                    sub_geom,
                    target_size_km2=target_size_km2
                )
                all_subregions.update(subdivided)
                print(f"      Created {len(subdivided)} subregion(s)")
            else:
                # Keep as single subregion - use simple_name_01 format for consistency
                subregion_id = f"{simple_name}_01"
                all_subregions[subregion_id] = sub_geom
                print(f"    → Keeping as single subregion")
        except Exception as e:
            print(f"    ✗ Error processing: {e}")
            continue
    
    return all_subregions


def build_region_geometries(
    cfg: dict[str, Any],
    use_gaul: bool = True,
    get_info_fn: Any = None,
) -> dict[str, tuple[ee.Geometry, str]]:
    """
    Build region geometries from config.
    Returns dict mapping region_name -> (geometry, source)
    """
    out = {}
    regions = cfg.get("regions", [])
    all_overrides = cfg.get("gaul_overrides", {})
    
    for r in regions:
        name = r.get("region", "")
        if not name:
            continue
        
        country = r.get("country", "")
        components = r.get("components", [])
        bbox = r.get("bbox", [])
        overrides = all_overrides.get(name, {})
        
        geom = None
        source = "unknown"
        
        # Try GAUL if components provided
        if use_gaul and components and country and bbox:
            union, matched, missing = _gaul_union_for_region(
                country, bbox, components, overrides, get_info_fn
            )
            if union is not None:
                geom = union
                source = "gaul"
        
        # Fallback to bbox
        if geom is None and bbox and len(bbox) == 4:
            geom = _bbox_geometry(bbox)
            source = "bbox"
        
        if geom:
            out[name] = (geom, source)
    
    return out


def add_regions_from_verified_json(
    verified_json_path: Path,
    *,
    target_size_km2: float = 10_000,
    save_to_bq: bool = True,
    project_id: str = None,
    dataset_id: str = "google_earth",
    table_id: str = "regions_info",
    dry_run: bool = False,
) -> Dict[str, ee.Geometry]:
    """
    Main function to add regions from verified JSON.
    """
    if not verified_json_path.is_file():
        raise FileNotFoundError(f"Verified JSON not found: {verified_json_path}")
    
    # Load verified JSON
    with open(verified_json_path, encoding="utf-8") as f:
        cfg = json.load(f)
    
    # Initialize GEE
    try:
        ee.Number(1).getInfo()
    except Exception:
        init_ee(KEY_PATH)
    
    def get_info_fn(obj):
        return get_info_with_timeout(obj, timeout_seconds=GEE_TIMEOUT)
    
    print("=" * 80)
    print("ADDING REGIONS FROM VERIFIED JSON")
    print("=" * 80)
    print(f"Config: {verified_json_path}")
    if dry_run:
        print("⚠ DRY RUN MODE - No changes will be saved to BigQuery")
    print()
    
    # Build region geometries
    print("Building region geometries...")
    built = build_region_geometries(cfg, use_gaul=True, get_info_fn=get_info_fn)
    print(f"✓ Built {len(built)} region(s)")
    print()
    
    # Process each region
    all_subregions = {}
    regions_cfg = {r["region"]: r for r in cfg.get("regions") or []}
    
    print("=" * 80)
    print("CREATING SUBREGIONS")
    print("=" * 80)
    print()
    
    for region_name, (region_geom, source) in built.items():
        region_cfg = regions_cfg.get(region_name, {})
        specified_subregions = region_cfg.get("subregions", [])
        
        print(f"Processing: {region_name} (source: {source})")
        
        if specified_subregions:
            # Use specified subregions as base units
            print(f"  Using {len(specified_subregions)} specified subregion(s)...")
            region_subregions = _process_specified_subregions(
                region_name, region_geom, region_cfg, specified_subregions,
                target_size_km2, get_info_fn
            )
            # If no subregions were found/created, fall back to subdividing parent
            if not region_subregions:
                print(f"  No specified subregions found in GAUL - falling back to subdividing parent region")
                simple_parent_name = _simplify_region_name(region_name)
                region_subregions = subdivide_region_to_target_size(
                    simple_parent_name, region_geom, target_size_km2=target_size_km2
                )
                print(f"  Created {len(region_subregions)} subregion(s) from parent subdivision")
            else:
                print(f"  Created {len(region_subregions)} subregion(s) from specified units")
            all_subregions.update(region_subregions)
        else:
            # No specified subregions - subdivide entire parent region
            print(f"  No specified subregions - subdividing parent region...")
            simple_parent_name = _simplify_region_name(region_name)
            region_subregions = subdivide_region_to_target_size(
                simple_parent_name, region_geom, target_size_km2=target_size_km2
            )
            all_subregions.update(region_subregions)
            print(f"  Created {len(region_subregions)} subregion(s)")
        
        print()
    
    # Save to BigQuery
    if save_to_bq and not dry_run:
        print("=" * 80)
        print("SAVING TO BIGQUERY")
        print("=" * 80)
        save_subregions_to_bq(
            all_subregions,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
    elif dry_run:
        print("=" * 80)
        print("DRY RUN - SKIPPING BIGQUERY SAVE")
        print("=" * 80)
        print(f"Would save {len(all_subregions)} subregion(s) to {dataset_id}.{table_id}")
    
    # Final summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total subregions created: {len(all_subregions)}")
    print()
    
    # Group by parent region
    from utils.earth_engine_utils import get_parent_region
    parent_counts = {}
    for subregion_id in all_subregions.keys():
        parent = get_parent_region(subregion_id)
        parent_counts[parent] = parent_counts.get(parent, 0) + 1
    
    print("Subregions by parent region:")
    for parent, count in sorted(parent_counts.items()):
        print(f"  {parent}: {count} subregion(s)")
    
    print()
    print("=" * 80)
    
    return all_subregions


def main():
    parser = argparse.ArgumentParser(description="Add regions from verified JSON to BigQuery")
    parser.add_argument("verified_json", type=Path, help="Verified JSON file from validate_regions_json.py")
    parser.add_argument("--table-id", default="regions_info", help="BigQuery table ID (default: regions_info)")
    parser.add_argument("--target-size", type=float, default=10000, help="Target subregion size in km² (default: 10000)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - don't save to BigQuery")
    
    args = parser.parse_args()
    
    if not args.verified_json.exists():
        print(f"Error: Verified JSON file not found: {args.verified_json}")
        sys.exit(1)
    
    try:
        import os
        project_id = os.getenv("PROJECT_ID") or "disaster-predictor-470812"
        
        add_regions_from_verified_json(
            args.verified_json,
            target_size_km2=args.target_size,
            save_to_bq=not args.dry_run,
            project_id=project_id,
            table_id=args.table_id,
            dry_run=args.dry_run,
        )
        
        if not args.dry_run:
            print()
            print("✓ Regions successfully added to BigQuery!")
            print(f"  Table: {project_id}.google_earth.{args.table_id}")
        else:
            print()
            print("✓ Dry run completed successfully!")
            print("  Run without --dry-run to actually save to BigQuery")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
