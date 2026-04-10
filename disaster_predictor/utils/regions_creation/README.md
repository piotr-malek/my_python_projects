# Regions Creation Workflow

A templatized, two-script workflow for adding regions to BigQuery with minimal manual intervention.

## Quick Start

1. **Create your JSON file** with region definitions (see [JSON Schema](#json-schema) below)
2. **Validate** your JSON:
   ```bash
   python utils/regions_creation/validate_regions_json.py your_regions.json
   ```
3. **Review** the generated report (`your_regions_report.md`) and verified JSON (`your_regions_verified.json`)
4. **Add regions** to BigQuery:
   ```bash
   python utils/regions_creation/add_regions_from_verified_json.py your_regions_verified.json
   ```

That's it! The system handles:
- GAUL name matching and validation
- Automatic subdivision of large regions (>12k km²)
- Fallback to parent subdivision if subregions not found
- Simplified naming (e.g., "Alto_Alegre" instead of "Amazon_Basin_Roraima_Brazil_Alto_Alegre_Municipality")
- Comprehensive reporting

## Workflow Details

### Script 1: `validate_regions_json.py`

**Purpose**: Validates raw JSON against GEE/GAUL and prepares production-ready verified JSON.

**What it does**:
- Checks if regions/subregions exist in GAUL
- Validates area estimates against actual GEE areas
- Suggests fixes for naming mismatches
- Outputs a verified JSON ready for production
- Generates a detailed markdown report

**Outputs**:
- `<input>_verified.json`: Production-ready JSON with validated names
- `<input>_report.md`: Detailed validation report with issues and suggestions

**Example**:
```bash
python utils/regions_creation/validate_regions_json.py config/new_regions.json
```

### Script 2: `add_regions_from_verified_json.py`

**Purpose**: Takes verified JSON and adds all regions to BigQuery with proper subdivision.

**What it does**:
- Uses specified subregions as base units when found in GAUL
- Subdivides subregions > 12k km² into ~10k chunks
- Falls back to parent subdivision if no subregions found
- Applies simplified naming conventions
- Saves to BigQuery `regions_expansion` table

**Options**:
- `--table-id`: Specify BigQuery table (default: `regions_expansion`)
- `--target-size`: Target subregion size in km² (default: 10000)
- `--dry-run`: Test without saving to BigQuery

**Example**:
```bash
# Dry run first
python utils/regions_creation/add_regions_from_verified_json.py config/new_regions_verified.json --dry-run

# Actually add to BigQuery
python utils/regions_creation/add_regions_from_verified_json.py config/new_regions_verified.json
```

## JSON Schema

Your input JSON can be flexible, but should follow this structure:

```json
{
  "regions": [
    {
      "region": "Unique_Region_Name",
      "country": "Country Name (for GAUL matching)",
      "bbox": [lon_min, lat_min, lon_max, lat_max],
      "area_km2": 123456,
      "components": ["Component1", "Component2"],
      "subregions": [
        {"name": "Subregion1"},
        {"name": "Subregion2"},
        {
          "parent": "Parent Group",
          "areas": [
            {"name": "Area1"},
            {"name": "Area2"}
          ]
        }
      ],
      "gee_notes": "Optional notes"
    }
  ],
  "gaul_overrides": {
    "Region_Name": {
      "ComponentName": "Exact GAUL NAME_2"
    }
  }
}
```

### Field Descriptions

**Required**:
- `region`: Unique identifier for the region
- `bbox`: Bounding box `[lon_min, lat_min, lon_max, lat_max]`

**Optional**:
- `country`: Country name for GAUL filtering (required if using `components` or `subregions`)
- `components`: List of admin2 names to union from GAUL (if omitted, uses bbox)
- `subregions`: List of subregions to use as base units (can be flat or nested)
- `area_km2`: Expected area (used for validation warnings)
- `gee_notes`: Free-form notes (for documentation only)
- `gaul_overrides`: Per-region overrides for GAUL name matching

### Subregions Format

Subregions can be specified in two formats:

**Flat format**:
```json
"subregions": [
  {"name": "Subregion1"},
  {"name": "Subregion2"}
]
```

**Nested format** (for grouped subregions):
```json
"subregions": [
  {
    "parent": "Group Name",
    "areas": [
      {"name": "Area1"},
      {"name": "Area2"}
    ]
  }
]
```

## How It Works

### Validation Phase (Script 1)

1. **Loads your JSON** and validates structure
2. **Checks each region**:
   - Validates bbox geometry
   - Checks if components exist in GAUL
   - Validates subregions against GAUL
   - Compares expected vs actual areas
3. **Generates verified JSON** with:
   - Matched GAUL names
   - Validated subregions
   - Area corrections
4. **Creates report** with:
   - Issues found (errors, warnings, suggestions)
   - Subregion validation results
   - Suggested fixes

### Addition Phase (Script 2)

1. **Loads verified JSON** (from Script 1)
2. **For each region**:
   - Builds geometry (GAUL union or bbox)
   - Processes specified subregions:
     - Finds each in GAUL
     - If > 12k km²: subdivides into ~10k chunks
     - If < 12k km²: keeps as single subregion
   - If no subregions found: subdivides parent region
3. **Applies simplified naming**:
   - "Alto Alegre Municipality" → "Alto_Alegre"
   - "Butte County" → "Butte_County"
   - "Bhutan_Complete" → "Bhutan"
4. **Saves to BigQuery** with all metadata

## Examples

### Example 1: Simple Region (No Subregions)

```json
{
  "regions": [
    {
      "region": "Bhutan_Complete",
      "bbox": [88.75, 26.7, 92.0, 28.25],
      "area_km2": 55000
    }
  ]
}
```

Result: Bhutan will be subdivided into ~5-6 subregions of ~10k km² each.

### Example 2: Region with Subregions

```json
{
  "regions": [
    {
      "region": "Amazon_Basin_Roraima_Brazil",
      "country": "Brazil",
      "bbox": [-64.5, 0.5, -59.5, 5.5],
      "subregions": [
        {"name": "Caracaraí Municipality"},
        {"name": "Alto Alegre Municipality"}
      ]
    }
  ]
}
```

Result: Each municipality will be:
- Found in GAUL
- If > 12k km²: subdivided into multiple subregions
- If < 12k km²: kept as single subregion

### Example 3: Complex Region with Nested Subregions

```json
{
  "regions": [
    {
      "region": "Indonesian_Archipelago",
      "country": "Indonesia",
      "bbox": [108.8, -1.8, 114.2, 1.2],
      "subregions": [
        {
          "parent": "West Kalimantan",
          "areas": [
            {"name": "Pontianak Regency"},
            {"name": "Sambas Regency"}
          ]
        },
        {
          "parent": "Jambi",
          "areas": [
            {"name": "Kerinci Regency"}
          ]
        }
      ]
    }
  ]
}
```

## Troubleshooting

### "Component not found in GAUL"

**Solution**: Add to `gaul_overrides`:
```json
"gaul_overrides": {
  "Region_Name": {
    "YourComponentName": "Exact GAUL NAME_2"
  }
}
```

### "Area differs significantly"

**Solution**: Either:
1. Update `area_km2` to match actual bbox area
2. Adjust `bbox` to match expected area

### "Subregion not found in GAUL"

**Solution**: This is OK! The system will:
1. Skip that subregion
2. Subdivide the parent region instead
3. Report this in the validation report

### Validation takes too long

**Solution**: The script validates each region/subregion against GEE, which can take time. This is normal for the first run. The verified JSON can be reused.

## Best Practices

1. **Always validate first**: Run Script 1 before Script 2
2. **Review the report**: Check for warnings and suggestions
3. **Use dry-run**: Test Script 2 with `--dry-run` before actually adding
4. **Keep verified JSON**: Reuse it if you need to re-add regions
5. **Check naming**: The system simplifies names automatically, but verify they make sense

## Advanced Usage

### Custom Target Size

```bash
python utils/regions_creation/add_regions_from_verified_json.py verified.json --target-size 15000
```

### Different BigQuery Table

```bash
python utils/regions_creation/add_regions_from_verified_json.py verified.json --table-id my_custom_table
```

### Programmatic Usage

```python
from utils.regions_creation.validate_regions_json import validate_regions_json
from utils.regions_creation.add_regions_from_verified_json import add_regions_from_verified_json
from pathlib import Path

# Validate
verified_json, results = validate_regions_json(
    Path("config/new_regions.json"),
    output_path=Path("config/new_regions_verified.json"),
    report_path=Path("config/new_regions_report.md")
)

# Add to BigQuery
subregions = add_regions_from_verified_json(
    Path("config/new_regions_verified.json"),
    target_size_km2=10000,
    save_to_bq=True
)
```

## Migration from Old System

If you were using `templates/regions_creation/add_regions_from_json.py`:

1. Your existing JSON should work as-is
2. Run it through Script 1 first to get verified JSON
3. Use Script 2 to add regions (same logic, cleaner interface)

The old script is still available but the new two-script workflow is recommended for better validation and reporting.
