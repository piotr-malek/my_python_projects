# Region creation utilities

Tools for adding monitoring subregions and computing ML descriptors. Region geometry and `display_name` live in BigQuery (`google_earth.regions_info`).

## Add new regions

Use the workflow in `utils/regions_creation/`:

```bash
python utils/regions_creation/validate_regions_json.py your_regions.json
python utils/regions_creation/add_regions_from_verified_json.py your_regions_verified.json
```

See `utils/regions_creation/README.md` for the full JSON schema and GAUL workflow.

## Compute descriptors (after climatology exists)

```bash
python templates/regions_creation/compute_region_descriptors.py
python templates/regions_creation/compute_region_descriptors.py --table-id regions_expansion
```

## Assign display names for new subregions

Only runs for rows where `display_name` is NULL (unless `--force`):

```bash
python templates/regions_creation/build_region_display_names.py research
python templates/regions_creation/build_region_display_names.py generate
python templates/regions_creation/build_region_display_names.py validate
python templates/regions_creation/build_region_display_names.py append-bq
```

Or in one step:

```bash
python templates/regions_creation/build_region_display_names.py all
```

`region_display_names.pending.json` is a short-lived working file (gitignored) created during that pipeline and removed after `append-bq`.

## Programmatic subdivision (optional)

```python
from templates.regions_creation import create_subregions

subregions = create_subregions(["New_Region"], save_to_bq=True)
```

## Files in this folder

| File | Purpose |
|------|---------|
| `compute_region_descriptors.py` | Compute static ML descriptors → merge into `regions_info` |
| `subregions.py` | Subdivide parent geometries (~10k km²) |
| `region_storage.py` | Append subregion rows to BigQuery |
| `build_region_display_names.py` | Geocode + name new subregions |
| `region_display_geocoding.py` | Nominatim reverse-geocoding helper |
