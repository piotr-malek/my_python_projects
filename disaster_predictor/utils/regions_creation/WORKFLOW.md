# Regions Creation Workflow - Quick Reference

## The Process

```
Your JSON → [Script 1: Validate] → Verified JSON + Report → [Script 2: Add] → BigQuery
```

## Step-by-Step

### 1. Prepare Your JSON

Create a JSON file with your regions. See `config/regions_json_schema_for_llm.json` for field definitions.

**Minimum required**:
- `region`: Unique name
- `bbox`: `[lon_min, lat_min, lon_max, lat_max]`

**Optional but recommended**:
- `country`: For GAUL matching
- `subregions`: List of subregions to use as base units
- `area_km2`: Expected area (for validation)

### 2. Validate

```bash
python utils/regions_creation/validate_regions_json.py your_regions.json
```

**Outputs**:
- `your_regions_verified.json` - Production-ready JSON
- `your_regions_report.md` - Detailed validation report

**What to check in the report**:
- ✅ All regions valid?
- ⚠️ Any warnings about area mismatches?
- ⚠️ Any subregions not found in GAUL? (This is OK - system handles it)
- 💡 Any suggestions for improvements?

### 3. Review & Decide

**If there are issues**:
- Fix them in your original JSON
- Re-run validation
- Or use `gaul_overrides` for name mismatches

**If everything looks good**:
- Proceed to Step 4

### 4. Add to BigQuery

**Dry run first** (recommended):
```bash
python utils/regions_creation/add_regions_from_verified_json.py your_regions_verified.json --dry-run
```

**Actually add**:
```bash
python utils/regions_creation/add_regions_from_verified_json.py your_regions_verified.json
```

**Output**:
- Summary of subregions created
- Breakdown by parent region
- Confirmation of BigQuery save

## Common Scenarios

### Scenario 1: Simple Region (No Subregions)

**Input**:
```json
{
  "region": "Bhutan",
  "bbox": [88.75, 26.7, 92.0, 28.25],
  "area_km2": 55000
}
```

**Result**: Bhutan subdivided into ~5-6 subregions of ~10k km² each.

### Scenario 2: Region with Subregions (All Found)

**Input**:
```json
{
  "region": "Roraima",
  "country": "Brazil",
  "bbox": [-64.5, 0.5, -59.5, 5.5],
  "subregions": [
    {"name": "Caracaraí Municipality"},
    {"name": "Alto Alegre Municipality"}
  ]
}
```

**Result**: 
- Each municipality found in GAUL
- Large ones (>12k km²) subdivided
- Small ones kept as single subregions

### Scenario 3: Region with Subregions (Some Not Found)

**Input**:
```json
{
  "region": "Australia",
  "country": "Australia",
  "bbox": [131.0, -21.5, 138.5, -17.0],
  "subregions": [
    {"name": "Ward A"},
    {"name": "Ward B"}
  ]
}
```

**Result**:
- Wards not found in GAUL
- System automatically subdivides parent region instead
- No manual intervention needed

### Scenario 4: Name Mismatch

**Input**:
```json
{
  "region": "My_Region",
  "country": "Country",
  "subregions": [
    {"name": "My Component Name"}
  ]
}
```

**Validation finds**: "My Component Name" not in GAUL

**Solution**: Add to `gaul_overrides`:
```json
"gaul_overrides": {
  "My_Region": {
    "My Component Name": "Exact GAUL NAME_2"
  }
}
```

## Tips

1. **Always validate first** - Catches issues before adding to BigQuery
2. **Use dry-run** - Test the addition process without saving
3. **Keep verified JSON** - Reuse it if you need to re-add
4. **Check the report** - It tells you exactly what will happen
5. **Don't worry about missing subregions** - System handles it automatically

## What Gets Created

For each region, the system creates:

- **Parent region geometry**: From GAUL union or bbox
- **Subregions**: 
  - From specified subregions (if found in GAUL)
  - Or from parent subdivision (if not found)
- **Naming**: Simplified (e.g., "Alto_Alegre" not "Amazon_Basin_Roraima_Brazil_Alto_Alegre_Municipality")

All saved to BigQuery `regions_expansion` table with:
- `region`: Subregion ID (e.g., "Alto_Alegre_01")
- `parent_region`: Parent name (e.g., "Alto_Alegre")
- `area_km2`: Area in km²
- `bbox`: Bounding box
- `centroid`: Centroid coordinates

## Troubleshooting

**"Component not found in GAUL"**
→ Add to `gaul_overrides` or remove from `components`

**"Area differs significantly"**
→ Update `area_km2` or adjust `bbox`

**"Subregion not found in GAUL"**
→ This is OK! System will subdivide parent instead

**Validation is slow**
→ Normal - validates against GEE. Verified JSON can be reused.
