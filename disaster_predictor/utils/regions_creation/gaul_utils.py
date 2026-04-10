"""
Shared GAUL utility functions for region creation.
Extracted from templates/regions_creation/add_regions_from_json.py
"""

import unicodedata
from typing import Any, Dict, List, Optional, Tuple, Union
import ee

GAUL_ASSET = "FAO/GAUL/2015/level2"
GEE_TIMEOUT = 120


def normalize(s: str) -> str:
    """Normalize string for GAUL matching."""
    s = (s or "").strip().lower().replace("_", " ")
    # Remove common administrative suffixes for better matching
    suffixes = [" regency", " county", " department", " cercle", " municipality", " ward"]
    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[:-len(suffix)].strip()
    nfd = unicodedata.normalize("NFD", s)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def simplify_region_name(name: str) -> str:
    """
    Simplify region name for use as parent_region.
    Examples:
    - "Alto Alegre Municipality" -> "Alto_Alegre"
    - "Butte County" -> "Butte_County"
    - "Bhutan_Complete" -> "Bhutan"
    - "Central_Valley_California_USA_Butte_County" -> "Butte_County"
    - "Bandiagara Cercle" -> "Bandiagara_Cercle"
    - "Amazon_Basin_Roraima_Brazil_Alto_Alegre_Municipality" -> "Alto_Alegre"
    """
    # Remove "_Complete" suffix first
    if name.endswith("_Complete"):
        name = name[:-9]
    
    # For complex names with many underscores, extract the meaningful part
    if "_" in name and name.count("_") > 2:
        parts = name.split("_")
        descriptors = {"County", "Municipality", "Department", "Cercle", "Regency", "Ward"}
        skip_words = {"USA", "US", "Complete", "California", "Northern", "Territory", "Brazil", "Mali", "Niger", 
                     "Amazon", "Basin", "Roraima", "Central", "Valley", "Archipelago", "West", "Kalimantan", "Jambi",
                     "Outback", "Australian"}
        
        # Check if it's a reasonable name that shouldn't be simplified
        # (e.g., "Himalayan_Foothills_Nepal_Bhutan" is already reasonable)
        meaningful_parts = [p for p in parts if p not in skip_words]
        if len(meaningful_parts) <= 4 and not any(p in descriptors for p in parts):  # Already reasonable
            name = "_".join(meaningful_parts) if meaningful_parts else name
        else:
            # If last part is a descriptor, extract what's before it
            if len(parts) >= 2 and parts[-1] in descriptors:
                # For "Municipality", "Regency", "Ward" - remove them and take what's before
                if parts[-1] in {"Municipality", "Regency", "Ward"}:
                    # Take the part(s) before the descriptor (could be multi-word like "Alto_Alegre")
                    # Look backwards to find where the meaningful name starts
                    result_parts = []
                    i = len(parts) - 2  # Start from second-to-last
                    while i >= 0 and parts[i] not in skip_words:
                        result_parts.insert(0, parts[i])
                        i -= 1
                    if result_parts:
                        name = "_".join(result_parts)
                    else:
                        name = parts[-2] if len(parts) >= 2 else name
                else:
                    # For "County", "Department", "Cercle" - keep them
                    name = f"{parts[-2]}_{parts[-1]}"
            else:
                # Look backwards for meaningful parts
                for i in range(len(parts) - 1, -1, -1):
                    if parts[i] not in skip_words and len(parts[i]) > 2:
                        if i > 0 and parts[i-1] in descriptors:
                            name = f"{parts[i-1]}_{parts[i]}"
                        else:
                            name = parts[i]
                        break
    
    # Handle space-separated names
    if " " in name:
        words = name.split()
        # Remove "Municipality", "Regency", "Ward" but keep "County", "Department", "Cercle"
        remove_words = {"Municipality", "Regency", "Ward"}
        keep_words = {"County", "Department", "Cercle"}
        
        filtered = []
        for word in words:
            if word not in remove_words:
                filtered.append(word)
            elif word in keep_words:
                # Keep it if it's a keep_word
                filtered.append(word)
        
        if filtered:
            name = "_".join(filtered)
        else:
            name = "_".join(words)
    
    # Normalize: replace remaining spaces with underscores, remove accents
    name = name.replace(" ", "_")
    nfd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    
    return name


def bbox_geometry(bbox: List[float]) -> ee.Geometry:
    """Create a rectangle geometry from bbox."""
    return ee.Geometry.Rectangle(bbox)


def gaul_union_for_region(
    country: str,
    bbox: List[float],
    components: List[str],
    overrides: Optional[Dict[str, str]],
    get_info_fn: Any,
) -> Tuple[Optional[ee.Geometry], List[str], List[str]]:
    """
    Get GAUL union for a region.
    Returns: (union_geometry, matched_names, missing_names)
    """
    fc = ee.FeatureCollection(GAUL_ASSET)
    rect = ee.Geometry.Rectangle(bbox)
    filtered = fc.filterBounds(rect).filter(ee.Filter.eq("ADM0_NAME", country))
    try:
        features = get_info_fn(filtered)
    except Exception:
        return None, [], list(components)

    if not features or "features" not in features:
        return None, [], list(components)

    overrides = overrides or {}
    want = {normalize(overrides.get(c, c)) for c in components}
    matched: List[Tuple[str, Any]] = []
    for f in features["features"]:
        props = f.get("properties") or {}
        # GAUL varies: some countries use NAME_2, Spain uses ADM2_NAME for provinces
        name2 = (props.get("NAME_2") or props.get("ADM2_NAME") or "").strip()
        if not name2:
            continue
        if normalize(name2) in want:
            matched.append((name2, f.get("geometry")))

    missing = [
        c for c in components
        if normalize(overrides.get(c, c)) not in {normalize(m[0]) for m in matched}
    ]
    if not matched:
        return None, [], list(components)

    geoms = [ee.Geometry(geo) for _, geo in matched if geo]
    if not geoms:
        return None, [], list(components)

    union = geoms[0]
    for g in geoms[1:]:
        union = union.union(g, maxError=1000)
    return union, [m[0] for m in matched], missing


def geom_info(geom: ee.Geometry, get_info_fn: Any) -> Dict[str, Any]:
    """
    Get geometry information (area, bbox, centroid).
    """
    area_m2 = geom.area(maxError=1000)
    area_km2 = get_info_fn(area_m2) / 1_000_000
    
    bounds = geom.bounds(maxError=1000)
    bounds_info = get_info_fn(bounds)
    coords = bounds_info.get("coordinates", [[[]]])[0][0]
    lon_min = min(c[0] for c in coords)
    lon_max = max(c[0] for c in coords)
    lat_min = min(c[1] for c in coords)
    lat_max = max(c[1] for c in coords)
    
    centroid = geom.centroid(maxError=1000)
    centroid_info = get_info_fn(centroid)
    centroid_coords = centroid_info.get("coordinates", [0, 0])
    centroid_lon = centroid_coords[0]
    centroid_lat = centroid_coords[1]
    
    return {
        "area_km2": area_km2,
        "bbox": [lon_min, lat_min, lon_max, lat_max],
        "centroid_lon": centroid_lon,
        "centroid_lat": centroid_lat,
    }
