# Region Creation and Descriptor Computation

This module provides tools for creating new regions and computing region descriptors for the universal ML model.

## Overview

**Two independent workflows** (region creation does NOT require descriptor computation):

1. **Create new regions**: Use the new templatized workflow in `utils/regions_creation/`:
   - **Script 1**: `validate_regions_json.py` - Validates JSON against GEE/GAUL
   - **Script 2**: `add_regions_from_verified_json.py` - Adds regions to BigQuery
   - See `utils/regions_creation/README.md` for full documentation

2. **Compute region descriptors** (`compute_region_descriptors.py`): Load subregions from BigQuery → compute descriptors from climatology + GEE → update table. **Requires climatology data** (terrain_static, era5, modis).

---

## Creating New Regions

**⚠️ Note**: The old `add_regions_from_json.py` script has been removed. Please use the new workflow:

```bash
# Step 1: Validate your JSON
python utils/regions_creation/validate_regions_json.py your_regions.json

# Step 2: Add to BigQuery
python utils/regions_creation/add_regions_from_verified_json.py your_regions_verified.json
```

See `utils/regions_creation/README.md` for complete documentation.

### Legacy Method (Deprecated)

Create a JSON file following the schema described in `config/regions_json_schema_for_llm.json`:

```json
{
  "regions": [
    {
      "region": "Example_Region_A",
      "country": "Peru",
      "components": ["Lima", "Callao"],
      "bbox": [-77.2, -12.2, -76.7, -11.8],
      "area_km2": 15000,
      "gee_notes": "Union of admin2 units (GAUL)"
    },
    {
      "region": "Example_Region_B",
      "bbox": [-68.5, -17.5, -67.5, -16.5],
      "area_km2": 8000
    }
  ],
  "gaul_overrides": {}
}
```

**Required fields per region:**
- `region`: Unique region name (string)
- `bbox`: Bounding box `[lon_min, lat_min, lon_max, lat_max]` (array of 4 floats)

**Optional fields per region:**
- `country`: Required only when using GAUL components (filters FAO GAUL by ADM0_NAME)
- `components`: List of admin2 names for GAUL union; if omitted → uses bbox as geometry
- `area_km2`: Used only for verification (flags if actual vs expected area differs >25%)
- `gee_notes`: Printed in report only (helpful for context)

**Top-level:**
- `gaul_overrides`: Optional `{"RegionName": {"component": "GAUL NAME_2"}}` for GAUL name matching

**Run:**
```bash
# Step 1: Validate your JSON
python utils/regions_creation/validate_regions_json.py path/to/regions.json

# Step 2: Add to BigQuery
python utils/regions_creation/add_regions_from_verified_json.py path/to/regions_verified.json

# Save to a different table (e.g. regions_expansion)
python utils/regions_creation/add_regions_from_verified_json.py path/to/regions_verified.json --table-id regions_expansion

# Dry run (test without saving)
python utils/regions_creation/add_regions_from_verified_json.py path/to/regions_verified.json --dry-run
```

**What it does:**
1. **Script 1 (validate)**: Validates JSON against GEE/GAUL, generates verified JSON + report
2. **Script 2 (add)**: Creates subregions (~10k km² each) from parent region or specified subregions, saves to BigQuery

**Note:** Does NOT compute descriptors. Run `compute_region_descriptors.py` separately after fetching climatology data.

### Method 2: Programmatic (Python)

```python
from templates.regions_creation.subregions import create_subregions
import ee

# Option A: Use existing region names
subregions = create_subregions(
    regions=["Kruger_NP", "Etosha_NP"],
    target_size_km2=10_000,
    save_to_bq=True
)

# Option B: Use custom geometries
custom_regions = {
    "New_Region": ee.Geometry.Rectangle([lon_min, lat_min, lon_max, lat_max])
}
subregions = create_subregions(
    regions=custom_regions,
    target_size_km2=10_000,
    save_to_bq=True
)
```

---

## Computing Region Descriptors

Region descriptors are static features that characterize each region (geography, climate, hydrology, landcover, soil). These enable the universal ML model to generalize across regions.

**Prerequisites:** Climatology data must exist (`climatology.terrain_static`, `climatology.era5`, `climatology.modis`, `climatology.viirs` for fire return interval). Run `add_regions_from_json.py` and climatology fetch scripts first.

### Usage

```bash
# Default: regions_info table
python templates/regions_creation/compute_region_descriptors.py

# For a different table (e.g. regions_expansion)
python templates/regions_creation/compute_region_descriptors.py --table-id regions_expansion
```

**Options:**
- `--dataset-id`: BigQuery dataset (default: google_earth)
- `--table-id`: BigQuery table (default: regions_info)
- `--test N`: Test mode — process only N regions, print results, skip BigQuery update

**What it does:**
1. Loads all subregions from the specified BigQuery table
2. Computes descriptors from:
   - **BigQuery:** `regions_info` (lat/lon), `climatology.terrain_static` (elevation, slope), `climatology.era5` (precip, temp), `climatology.modis` (NDVI), `climatology.viirs` (fire return interval)
   - **Google Earth Engine:** ESA WorldCover (landcover), ISRIC SoilGrids (AWC, effective rooting depth), OpenLandMap (soil texture), JRC GSW (distance to water, seasonal water), coastlines
3. Updates the specified table with computed descriptors

### Descriptor Categories

**Geographic (6):** `mean_latitude_deg`, `mean_longitude_deg`, `elevation_mean_m`, `elevation_std_m`, `slope_mean_deg`, `slope_gt15_pct`

**Climate (6):** `mean_annual_precip_mm`, `precip_seasonality_index`, `mean_annual_temp_c`, `temperature_seasonality_c`, `aridity_index` (Thornthwaite PET/P), `frost_days_per_year`

**Hydrology (8):** `distance_to_coast_km`, `distance_to_major_waterbody_km`, `basin_type`, `drainage_area_km2`, `stream_order_max`, `permanent_water_fraction_pct`, `wetland_fraction_pct`, `seasonal_water_range_pct`

**Landcover (8):** `landcover_diversity_index`, `urban_fraction_pct`, `crop_fraction_pct`, `natural_vegetation_fraction_pct`, `ndvi_mean`, `ndvi_std`, `tree_cover_pct`, `grass_cover_pct`

**Soil (5):** `soil_texture_class` (USDA), `soil_depth_cm`, `soil_organic_carbon_pct`, `available_water_capacity_mm` (0–200cm), `effective_rooting_depth_cm` (AWC-derived, depth where 95% of plant-available water is contained)

**Historical (3):** `flood_events_per_decade`, `fire_return_interval_years` (from MODIS burned area + VIIRS hotspots), `landslide_events_per_decade`

**Total: 36 descriptors** (counted dynamically from schema)

### Implementation Status

**✅ Fully Implemented:**
- **Geographic:** lat/lon, elevation, slope, `slope_gt15_pct` (estimated from mean)
- **Climate:** precip, temp, seasonality, frost days, `aridity_index` (Thornthwaite PET from ERA5)
- **NDVI:** from MODIS
- **Landcover:** ESA WorldCover (tree/grass/crop/urban/water fractions, diversity index)
- **Hydrology:** `distance_to_coast_km`, `distance_to_major_waterbody_km` (JRC GSW fastDistanceTransform), `permanent_water_fraction_pct`, `wetland_fraction_pct`, `seasonal_water_range_pct` (JRC GSW seasonality)
- **Soil:** `soil_texture_class` (OpenLandMap USDA), `available_water_capacity_mm` (SoilGrids 0–200cm), `effective_rooting_depth_cm` (AWC-derived, global)
- **Historical:** `fire_return_interval_years` (MODIS + VIIRS from BigQuery)

**⚠️ Placeholders (NULL):**
- `basin_type`, `drainage_area_km2`, `stream_order_max` — HydroSHEDS Basin asset deprecated
- `soil_depth_cm`, `soil_organic_carbon_pct` — would need different soil datasets
- `flood_events_per_decade`, `landslide_events_per_decade` — would need event databases

### Notes

- Schema is automatically extended if new columns don't exist
- Missing values are stored as NULL in BigQuery
- Script uses incremental updates via MERGE statements
- GEE rate limits apply; includes sleep delays between subregions

---

## Scripts Reference

### Region Creation Workflow

**⚠️ The old `add_regions_from_json.py` has been removed. Use the new workflow:**

**CLI:**
```bash
# Step 1: Validate
python utils/regions_creation/validate_regions_json.py <input_json>

# Step 2: Add to BigQuery
python utils/regions_creation/add_regions_from_verified_json.py <verified_json> --table-id regions_expansion
```

**See `utils/regions_creation/README.md` for complete documentation.**

add_regions_from_json("config/my_regions.json")
add_regions_from_json("config/my_regions.json", table_id="regions_expansion")
```

### `subregions.py`
Creates subregions for existing region names or custom geometries.

**Python:**
```python
from templates.regions_creation.subregions import create_subregions
subregions = create_subregions(regions=["Region_Name"], save_to_bq=True)
```

### `compute_region_descriptors.py`
Computes all region descriptors for existing subregions. Requires climatology data. Updates the specified table.

**CLI:**
```bash
python templates/regions_creation/compute_region_descriptors.py
python templates/regions_creation/compute_region_descriptors.py --table-id regions_expansion
python templates/regions_creation/compute_region_descriptors.py --test 5   # Test 5 regions, no BQ update
```

---

## Workflow: Adding a New Region

1. **Create JSON file** with region specification (see `config/regions_json_schema_for_llm.json`)
2. **Run `add_regions_from_json.py`** to create subregions and save to BigQuery (no climatology needed)
3. **Fetch climatology data** (see `templates/climatology/`)
4. **Run `compute_region_descriptors.py`** to compute descriptors (requires climatology)
5. **Region is ready** for ML model training/inference

Region creation and descriptor computation are independent. You can create regions first and compute descriptors later when climatology is available.

---

## BigQuery Tables

### `google_earth.regions_info`
Stores subregion information and descriptors:
- Basic info: `region`, `parent_region`, `area_km2`, `lon_min`, `lat_min`, `lon_max`, `lat_max`, `centroid_lon`, `centroid_lat`
- All 36 region descriptors (see above)

### `climatology.*`
Stores historical weather/climate data (see `templates/climatology/` README)

---

## See Also

- **Climatology fetch:** `templates/climatology/` — Scripts for fetching historical weather data
