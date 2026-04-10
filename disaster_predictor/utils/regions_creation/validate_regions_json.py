#!/usr/bin/env python3
"""
Script 1: Validate Regions JSON

Takes a raw JSON file with region definitions and validates them against GEE/GAUL.
Outputs:
1. A verified JSON file ready for production
2. A detailed report with issues, suggestions, and decisions needed

Usage:
    python utils/regions_creation/validate_regions_json.py <input_json> [--output <verified_json>] [--report <report_file>]
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
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
    GAUL_ASSET,
    GEE_TIMEOUT,
    gaul_union_for_region as _gaul_union_for_region,
    geom_info as _geom_info
)


@dataclass
class ValidationIssue:
    """Represents a validation issue or suggestion."""
    severity: str  # "error", "warning", "info", "suggestion"
    region: str
    field: Optional[str] = None
    message: str = ""
    suggested_fix: Optional[str] = None
    actual_value: Optional[Any] = None
    expected_value: Optional[Any] = None


@dataclass
class RegionValidation:
    """Validation results for a single region."""
    region_name: str
    is_valid: bool
    issues: List[ValidationIssue]
    verified_config: Dict[str, Any]
    subregions_validation: List[Dict[str, Any]] = None


def _check_gaul_match(
    country: str,
    bbox: List[float],
    name: str,
    overrides: Dict[str, str],
    get_info_fn
) -> tuple[bool, Optional[str], Optional[float]]:
    """
    Check if a name matches in GAUL.
    Returns: (found, matched_name, area_km2)
    """
    try:
        overrides_dict = overrides.get(name) or {}
        union, matched, missing = _gaul_union_for_region(
            country, bbox, [name], overrides_dict, get_info_fn
        )
        
        if union is not None:
            # Calculate area
            area_info = _geom_info(union, get_info_fn)
            matched_name = matched[0] if matched else name
            return True, matched_name, area_info["area_km2"]
        
        return False, None, None
    except Exception as e:
        return False, None, None


def _validate_region(
    region_config: Dict[str, Any],
    get_info_fn,
    all_overrides: Dict[str, Dict[str, str]]
) -> RegionValidation:
    """
    Validate a single region configuration.
    """
    region_name = region_config.get("region", "")
    issues = []
    verified_config = region_config.copy()
    
    # Validate required fields
    if not region_name:
        issues.append(ValidationIssue(
            severity="error",
            region="",
            field="region",
            message="Region name is required"
        ))
        return RegionValidation(region_name, False, issues, verified_config)
    
    if "bbox" not in region_config or len(region_config["bbox"]) != 4:
        issues.append(ValidationIssue(
            severity="error",
            region=region_name,
            field="bbox",
            message="Bbox is required and must have 4 elements [lon_min, lat_min, lon_max, lat_max]"
        ))
        return RegionValidation(region_name, False, issues, verified_config)
    
    bbox = region_config["bbox"]
    country = region_config.get("country", "")
    components = region_config.get("components", [])
    subregions = region_config.get("subregions", [])
    expected_area = region_config.get("area_km2")
    overrides = all_overrides.get(region_name, {})
    
    # Validate bbox geometry
    try:
        rect = ee.Geometry.Rectangle(bbox)
        bbox_area_m2 = rect.area(maxError=1000)
        bbox_area_km2 = get_info_fn(bbox_area_m2) / 1_000_000
        
        if expected_area:
            diff_pct = abs(bbox_area_km2 - expected_area) / expected_area * 100
            if diff_pct > 25:
                issues.append(ValidationIssue(
                    severity="warning",
                    region=region_name,
                    field="area_km2",
                    message=f"Bbox area differs significantly from expected",
                    actual_value=bbox_area_km2,
                    expected_value=expected_area,
                    suggested_fix=f"Update area_km2 to {bbox_area_km2:,.0f} or adjust bbox"
                ))
    except Exception as e:
        issues.append(ValidationIssue(
            severity="error",
            region=region_name,
            field="bbox",
            message=f"Invalid bbox: {e}"
        ))
        return RegionValidation(region_name, False, issues, verified_config)
    
    # Validate components if provided
    if components and country:
        verified_components = []
        for comp in components:
            found, matched_name, area = _check_gaul_match(
                country, bbox, comp, overrides, get_info_fn
            )
            if found:
                verified_components.append(matched_name)
            else:
                issues.append(ValidationIssue(
                    severity="warning",
                    region=region_name,
                    field="components",
                    message=f"Component '{comp}' not found in GAUL",
                    suggested_fix=f"Check spelling or add to gaul_overrides"
                ))
        
        if verified_components:
            verified_config["components"] = verified_components
        elif components:
            issues.append(ValidationIssue(
                severity="warning",
                region=region_name,
                field="components",
                message="No components found in GAUL - will use bbox instead",
                suggested_fix="Consider removing components or using bbox-only approach"
            ))
    
    # Validate subregions if provided
    subregions_validation = []
    if subregions:
        flat_subregions = []
        for sub in subregions:
            if isinstance(sub, dict) and "parent" in sub:
                areas = sub.get("areas", [])
                for area in areas:
                    area_name = area.get("name", "")
                    if area_name:
                        flat_subregions.append({"name": area_name, "parent_group": sub.get("parent")})
            else:
                sub_name = sub.get("name", "") if isinstance(sub, dict) else str(sub)
                if sub_name:
                    flat_subregions.append({"name": sub_name})
        
        verified_subregions = []
        for sub_spec in flat_subregions:
            sub_name = sub_spec["name"]
            if country and bbox:
                found, matched_name, area = _check_gaul_match(
                    country, bbox, sub_name, overrides, get_info_fn
                )
                subregions_validation.append({
                    "name": sub_name,
                    "found_in_gaul": found,
                    "matched_name": matched_name,
                    "area_km2": area,
                    "will_subdivide": area > 12000 if area else False
                })
                
                if found:
                    verified_subregions.append({
                        "name": matched_name,
                        "area_km2": area,
                        "will_subdivide": area > 12000
                    })
                else:
                    issues.append(ValidationIssue(
                        severity="warning",
                        region=region_name,
                        field="subregions",
                        message=f"Subregion '{sub_name}' not found in GAUL",
                        suggested_fix=f"Will be skipped - parent region will be subdivided instead"
                    ))
            else:
                issues.append(ValidationIssue(
                    severity="warning",
                    region=region_name,
                    field="subregions",
                    message=f"Cannot validate subregion '{sub_name}' - country or bbox missing"
                ))
        
        if verified_subregions:
            verified_config["subregions"] = verified_subregions
        else:
            issues.append(ValidationIssue(
                severity="info",
                region=region_name,
                field="subregions",
                message="No subregions found in GAUL - parent region will be subdivided",
                suggested_fix="This is acceptable - the system will handle it automatically"
            ))
    
    is_valid = all(issue.severity != "error" for issue in issues)
    
    return RegionValidation(
        region_name=region_name,
        is_valid=is_valid,
        issues=issues,
        verified_config=verified_config,
        subregions_validation=subregions_validation
    )


def validate_regions_json(
    input_path: Path,
    output_path: Optional[Path] = None,
    report_path: Optional[Path] = None
) -> tuple[Dict[str, Any], List[RegionValidation]]:
    """
    Main validation function.
    Returns: (verified_json_dict, validation_results)
    """
    # Load input JSON
    with open(input_path, encoding="utf-8") as f:
        input_data = json.load(f)
    
    # Initialize GEE
    try:
        ee.Number(1).getInfo()
    except Exception:
        init_ee(KEY_PATH)
    
    def get_info_fn(obj):
        return get_info_with_timeout(obj, timeout_seconds=GEE_TIMEOUT)
    
    # Extract global overrides
    all_overrides = input_data.get("gaul_overrides", {})
    
    # Validate each region
    regions = input_data.get("regions", [])
    validation_results = []
    verified_regions = []
    
    print("=" * 80)
    print("VALIDATING REGIONS")
    print("=" * 80)
    print(f"Input file: {input_path}")
    print(f"Regions to validate: {len(regions)}")
    print()
    
    for i, region_config in enumerate(regions, 1):
        print(f"[{i}/{len(regions)}] Validating: {region_config.get('region', 'UNNAMED')}")
        result = _validate_region(region_config, get_info_fn, all_overrides)
        validation_results.append(result)
        
        if result.is_valid:
            verified_regions.append(result.verified_config)
            print(f"  ✓ Valid")
        else:
            print(f"  ✗ Has errors")
        
        if result.issues:
            for issue in result.issues:
                print(f"    [{issue.severity.upper()}] {issue.message}")
        print()
    
    # Build verified JSON
    verified_json = {
        "regions": verified_regions,
        "gaul_overrides": all_overrides,
        "_metadata": {
            "source_file": str(input_path),
            "validated_at": str(Path(__file__).stat().st_mtime),
            "total_regions": len(regions),
            "valid_regions": len(verified_regions)
        }
    }
    
    # Save verified JSON
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(verified_json, f, indent=2, ensure_ascii=False)
        print(f"✓ Verified JSON saved to: {output_path}")
    
    # Generate report
    if report_path:
        _generate_report(validation_results, report_path)
        print(f"✓ Report saved to: {report_path}")
    
    return verified_json, validation_results


def _generate_report(validation_results: List[RegionValidation], report_path: Path):
    """Generate a detailed validation report."""
    lines = []
    lines.append("# Regions Validation Report\n")
    lines.append(f"Generated: {Path(__file__).stat().st_mtime}\n")
    lines.append("=" * 80)
    lines.append("\n")
    
    # Summary
    total = len(validation_results)
    valid = sum(1 for r in validation_results if r.is_valid)
    errors = sum(1 for r in validation_results for i in r.issues if i.severity == "error")
    warnings = sum(1 for r in validation_results for i in r.issues if i.severity == "warning")
    
    lines.append("## Summary\n")
    lines.append(f"- Total regions: {total}\n")
    lines.append(f"- Valid regions: {valid}\n")
    lines.append(f"- Regions with errors: {errors}\n")
    lines.append(f"- Regions with warnings: {warnings}\n")
    lines.append("\n")
    
    # Detailed results
    for result in validation_results:
        lines.append(f"## {result.region_name}\n")
        lines.append(f"**Status:** {'✓ Valid' if result.is_valid else '✗ Has Issues'}\n")
        lines.append("\n")
        
        if result.issues:
            lines.append("### Issues\n")
            for issue in result.issues:
                lines.append(f"- **[{issue.severity.upper()}]** {issue.message}\n")
                if issue.field:
                    lines.append(f"  - Field: `{issue.field}`\n")
                if issue.actual_value is not None:
                    lines.append(f"  - Actual: `{issue.actual_value}`\n")
                if issue.expected_value is not None:
                    lines.append(f"  - Expected: `{issue.expected_value}`\n")
                if issue.suggested_fix:
                    lines.append(f"  - Suggestion: {issue.suggested_fix}\n")
                lines.append("\n")
        
        if result.subregions_validation:
            lines.append("### Subregions Validation\n")
            lines.append("| Name | Found in GAUL | Matched Name | Area (km²) | Will Subdivide |\n")
            lines.append("|------|---------------|--------------|------------|----------------|\n")
            for sub in result.subregions_validation:
                found = "✓" if sub["found_in_gaul"] else "✗"
                matched = sub.get("matched_name", "-") or "-"
                area = f"{sub.get('area_km2', 0):,.0f}" if sub.get("area_km2") else "-"
                subdivide = "Yes" if sub.get("will_subdivide") else "No"
                lines.append(f"| {sub['name']} | {found} | {matched} | {area} | {subdivide} |\n")
            lines.append("\n")
        
        lines.append("---\n\n")
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def main():
    parser = argparse.ArgumentParser(description="Validate regions JSON against GEE/GAUL")
    parser.add_argument("input_json", type=Path, help="Input JSON file with region definitions")
    parser.add_argument("--output", "-o", type=Path, help="Output path for verified JSON (default: <input>_verified.json)")
    parser.add_argument("--report", "-r", type=Path, help="Output path for validation report (default: <input>_report.md)")
    
    args = parser.parse_args()
    
    if not args.input_json.exists():
        print(f"Error: Input file not found: {args.input_json}")
        sys.exit(1)
    
    output_path = args.output or args.input_json.parent / f"{args.input_json.stem}_verified.json"
    report_path = args.report or args.input_json.parent / f"{args.input_json.stem}_report.md"
    
    try:
        verified_json, results = validate_regions_json(args.input_json, output_path, report_path)
        
        # Print summary
        print()
        print("=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        total = len(results)
        valid = sum(1 for r in results if r.is_valid)
        print(f"Total regions: {total}")
        print(f"Valid: {valid}")
        print(f"Issues found: {sum(len(r.issues) for r in results)}")
        print()
        print(f"✓ Verified JSON: {output_path}")
        print(f"✓ Report: {report_path}")
        print()
        print("Next step: Review the report and verified JSON, then run:")
        print(f"  python utils/regions_creation/add_regions_from_verified_json.py {output_path}")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
