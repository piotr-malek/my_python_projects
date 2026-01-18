# Data Collection Scripts Documentation

This document provides detailed information about the data collection scripts used in the bird migration tracking system.

## 1. `openmeteo.py` - Weather Data Collection

### Purpose
Collects historical weather data for bird migration tracking locations.

### Data Collection Process
1. **Location Date Expansion**
   - Each location has tracking start and end dates relative to bird arrival dates
   - Example: If a bird arrives on March 15 and has weather_check_start of 14 days and weather_check_end of 7 days:
     - Tracking period: March 1 to March 8
   - Historical data collection spans 10 years before the earliest recorded arrival
   - Weather data is fetched for each year's tracking period

2. **Weather Parameters Collected**
   - Temperature
   - Precipitation
   - Wind speed and direction
   - Cloud cover
   - Pressure
   - Humidity

### Storage
Data is stored in BigQuery in the `birding_raw` dataset under tables named with the format `{bird_species}_weather_data`

## 2. `ebird.py` - Bird Observation Data Collection

### Purpose
Collects bird observation data from eBird API for tracking migration patterns.

### Data Collection Process
1. **First Arrivals Tracking**
   - Collects first sightings of each species in specified regions
   - Searches across multiple years (typically January-June)

2. **Data Points Collected**
   - Species observations
   - Observation dates and times
   - Geographical coordinates
   - Region information
   - Observer metadata

### Storage
Data is stored in BigQuery in the `birding_raw` dataset under tables named with the format `{region}_first_sightings`

## 3. `bigquery.py` - Data Storage Utilities

### Purpose
Provides utility functions for interacting with BigQuery.

### Functions
1. `save_to_bigquery`
   - Saves pandas DataFrames to BigQuery tables
   - Supports different write modes (truncate, append, etc.)
   - Automatic schema detection

2. `load_from_bigquery`
   - Executes BigQuery queries and returns results as pandas DataFrames
   - Handles error management
   - Configurable project ID and credentials

### Security
- Uses service account credentials for authentication
- Credentials are stored in a secure location
- Environment variables are used for configuration

## Data Flow Overview

1. Raw Data Collection
   - Weather data → `birding_raw.weather_data_*`
   - Bird sightings → `birding_raw.first_sightings_*`

2. Data Processing
   - DBT models transform raw data
   - Staging models clean and normalize data
   - Core models perform final transformations

3. Final Storage
   - Cleaned data → `birding_dbt_core.*`
   - Intermediate results → `birding_dbt_staging.*`

## Best Practices

1. **Data Validation**
   - All scripts include error handling
   - Data validation before storage
   - Logging of errors and warnings

2. **Rate Limiting**
   - API calls are cached where possible
   - Respect API rate limits
   - Retry mechanisms for failed requests

3. **Documentation**
   - Each script includes docstrings
   - Parameters and return values are documented
   - Error conditions are documented
