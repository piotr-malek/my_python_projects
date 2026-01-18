# Birding dbt Project

This dbt project transforms bird migration and weather data for analysis. It replaces the previous Python-based ETL pipeline with a structured dbt workflow, including API integrations with eBird and OpenMeteo.

## Project Structure

### Staging Models
Transform raw data into clean, standardized formats:

#### API Integrations
```
birding_dbt/
├── models/            # dbt models (to be added later)
├── seeds/             # Seed data (migration locations)
│   └── migration_locations.csv
├── .gitignore
├── README.md
└── dbt_project.yml    # dbt project configuration
```

## Data Sources

- **eBird API**: Bird sighting data
- **OpenMeteo API**: Historical weather data
- **Migration Locations**: Migration routes and timing data

## Project Status

🚧 Data collection in progress - dbt transformations will be added later

### eBird API
- Used to fetch bird observation data and first arrival dates
- Requires an API key from [eBird](https://ebird.org/api/keygen)
- Data is cached for 24 hours to minimize API calls
### OpenMeteo API
- Used to fetch historical weather data for migration routes
- Free tier available with rate limits
- Data is cached to minimize API calls

## Data Flow

1. **Data Ingestion**:
   - eBird API → `stg_ebird_observations` and `stg_ebird_first_arrivals`
   - OpenMeteo API → `stg_openmeteo_weather`
   - CSV seeds → `stg_migration_locations`

2. **Data Transformation**:
   - Intermediate models process and join the raw data
   - Weather requests are generated based on migration paths and arrival dates

3. **Analysis**:
   - Final marts combine all data for analysis
   - Includes calculated fields for weather patterns and migration conditions

## Migrating from Old Project

The following Python scripts have been replaced by dbt models:
- `ebird.py` → `stg_ebird_observations` and `stg_ebird_first_arrivals` models
- `openmeteo.py` → `stg_openmeteo_weather` model
- `bigquery.py` → dbt's built-in BigQuery adapter and custom macros
- `main.py` → `fct_bird_migration_analysis` model and dbt run commands
dbt docs serve
```

## Data Flow

1. Raw data is loaded into BigQuery tables (`raw_pl_ma_first_sightings`, `raw_pl_ma_spring_weather_data`)
2. Staging models clean and standardize the raw data
3. Intermediate models transform and enrich the data
4. Core models create the final analytical datasets

## Migration from Python

This dbt project replaces the previous Python-based ETL pipeline that used scripts like `ebird.py` and `openmeteo.py`. The transformation logic has been migrated to SQL-based dbt models, making it more maintainable and easier to understand.
