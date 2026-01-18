import openmeteo_requests
from pandas.util.version import PrePostDevType
import requests_cache
import pandas as pd
import time
import random
from timezonefinder import TimezoneFinder

from bigquery import load_from_bigquery, save_to_bigquery

import os
from dotenv import load_dotenv

load_dotenv()

def get_weather_data(lat, lon, start_date, end_date, max_retries=5, initial_delay=1):
    """
    Fetch weather data from OpenMeteo API with retry mechanism and rate limiting.
    
    Args:
        lat: Latitude of the location
        lon: Longitude of the location
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds (will increase exponentially)
        
    Returns:
        DataFrame with weather data
    """
    
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                 "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
                 "sunshine_duration", "sunrise", "sunset", "wind_speed_10m_max",
                 "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum",
                 "et0_fao_evapotranspiration"],
        "timezone": "auto"
    }
    
    delay = initial_delay

    def get_timezone(lat, lon):
        tf = TimezoneFinder()
        tz_name = tf.timezone_at(lat=lat, lng=lon)
        if tz_name is None:
            raise ValueError(f"Could not determine timezone for coordinates: {lat}, {lon}")
        return tz_name

    for attempt in range(max_retries):
        try:
            time.sleep(6 + random.uniform(0.5, 1.5))

            responses = openmeteo.weather_api(url, params=params, timeout=30)
            response = responses[0]

            daily = response.Daily()
            
            tz_name = get_timezone(lat, lon)

            date_range = pd.date_range(
                start=start_date,
                end=end_date,  
                freq="D",
                inclusive="both",
                tz=tz_name
            )
            
            daily_data = {"date": date_range}

            weather_fields = [
                "weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
                "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
                "sunshine_duration", "sunrise", "sunset", "wind_speed_10m_max",
                "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum",
                "et0_fao_evapotranspiration"
            ]

            for i, name in enumerate(weather_fields):
                values = pd.Series(daily.Variables(i).ValuesAsNumpy(), dtype='float')
                values = values.reindex(range(len(date_range))).to_numpy()
                daily_data[name] = values

            df = pd.DataFrame(daily_data)

            if df[weather_fields].isna().all().all():
                raise ValueError(f"Missing weather data for lat={lat}, lon={lon}, {start_date} to {end_date}.")

            df['date'] = df['date'].dt.date

            return df

        except Exception as e:
            if attempt == max_retries - 1:
                raise

            error_msg = str(e).lower()

            if 'minutely api request limit exceeded' in error_msg:
                wait_time = min(60, delay * (2 ** attempt))
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                wait_time = min(30, delay * (2 ** attempt) + random.uniform(0, 1))
                print(f"Error: {str(e)}. Retrying in {wait_time:.1f} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)

    raise Exception(f"Failed to fetch weather data after {max_retries} attempts")

def get_migration_dates(project_id):
    """Load migration dates from BQ"""
    
    migration_dates = load_from_bigquery(
        f"""
        SELECT DISTINCT
            w.bird,
            w.weather_date,
            w.arrival_year,
            w.location_name,
            l.lat,
            l.lon,
            w.arrival_year as effective_year,
            0 as offset
        FROM `{project_id}.dbt_staging.stg_arrival_weather_periods` w
        JOIN `{project_id}.dbt_staging.stg_migration_locations` l USING(location_name)
        """
    )

    if migration_dates is None or migration_dates.empty:
        raise ValueError("No weather dates data returned from BigQuery.")
    
    migration_dates["weather_date"] = pd.to_datetime(migration_dates["weather_date"]).dt.strftime("%Y-%m-%d")

    return migration_dates

def get_historical_dates(migration_dates):
    """Get 10 years of historical dates for each migration date"""
    historical_dates = []
    
    for row in migration_dates.itertuples(index=False):
        for offset in range(1, 11):
            historical_dates.append({
                'bird': row.bird,
                'location_name': row.location_name,
                'lat': row.lat,
                'lon': row.lon,
                'weather_date': (pd.to_datetime(row.weather_date) - pd.DateOffset(years=offset)).strftime('%Y-%m-%d'),
                'arrival_year': row.arrival_year,
                'year_offset': offset,
                'effective_year': row.arrival_year - offset
            })

    return pd.DataFrame(historical_dates)

def combine_weather_dates(historical_dates, arrival_dates):
    """Combine historical and arrival dates"""

    combined_dates = pd.concat([historical_dates, arrival_dates], ignore_index=True)
    combined_dates = combined_dates.drop_duplicates()

    return combined_dates

def get_weather_data_for_all_dates(combined_dates):
    """Fetch weather data for each (bird, year, location) group"""
    all_weather_data = []

    grouped_dates = combined_dates.groupby(['bird', 'effective_year', 'location_name', 'lat', 'lon'])

    for (bird, effective_year, location_name, lat, lon), group in grouped_dates:
        start_date = group['weather_date'].min()
        end_date = group['weather_date'].max()

        print(f"Fetching weather data for {bird}, {location_name}, {effective_year} from {start_date} to {end_date}")

        weather_data = get_weather_data(lat, lon, start_date, end_date)

        weather_data["bird"] = bird
        weather_data["location_name"] = location_name
        weather_data["arrival_year"] = group['arrival_year'].iloc[0]
        weather_data["effective_year"] = effective_year
        weather_data["year_offset"] = group['year_offset'].iloc[0]

        all_weather_data.append(weather_data)

    return pd.concat(all_weather_data, ignore_index=True)
        
def get_missing_weather_data(project_id):
    """Get weather data for missing dates in case there are some gaps after an earlier fetch"""

    all_weather_data = []

    missing_dates = load_from_bigquery(
        f"""
        SELECT DISTINCT
            p.weather_date,
            p.location_name,
            l.lat,
            l.lon,
            p.bird,
            p.arrival_year,
            p.arrival_year as effective_year,
            0 as offset
        FROM `{project_id}.dbt_staging.stg_arrival_weather_periods` p
        LEFT JOIN `{project_id}.birding_raw.pl_ma_spring_weather_data_new` w
            ON p.bird = w.bird
            AND p.location_name = w.location_name
            AND p.weather_date = DATE(w.date)
        JOIN `{project_id}.dbt_staging.stg_migration_locations` l
            ON p.location_name = l.location_name
        WHERE w.date IS NULL

        UNION ALL

        SELECT DISTINCT
            p.weather_date,
            p.location_name,
            l.lat,
            l.lon,
            p.bird,
            p.arrival_year,
            p.effective_year,
            p.offset
        FROM `{project_id}.dbt_staging.stg_historical_weather_periods` p
        LEFT JOIN `{project_id}.birding_raw.pl_ma_spring_weather_data_new` w
            ON p.bird = w.bird
            AND p.location_name = w.location_name
            AND p.weather_date = DATE(w.date)
        JOIN `{project_id}.dbt_staging.stg_migration_locations` l
            ON p.location_name = l.location_name
        WHERE w.date IS NULL
        """ 
    ) 

    if missing_dates is None or missing_dates.empty:
        return pd.DataFrame()

    all_weather_data = []
    
    for row in missing_dates.itertuples(index=False):
        weather_data = get_weather_data(row.lat, row.lon, row.weather_date, row.weather_date)
        
        weather_data['bird'] = row.bird
        weather_data['location_name'] = row.location_name
        weather_data['arrival_year'] = row.arrival_year
        weather_data['effective_year'] = row.effective_year
        weather_data['year_offset'] = row.offset
        
        all_weather_data.append(weather_data)

    df = pd.concat(all_weather_data, ignore_index=True)

    print(f"Fetched weather data for {len(all_weather_data)} missing date(s).")
    
    return df

def main():
    project_id = os.getenv("BQ_PROJECT_ID")
    
    migration_dates = get_migration_dates(project_id)
    historical_dates = get_historical_dates(migration_dates)
    combined_dates = combine_weather_dates(historical_dates, migration_dates) 
    weather_data = get_weather_data_for_all_dates(combined_dates)
    save_to_bigquery(weather_data, "birding_raw", "pl_ma_spring_weather_data", mode='WRITE_TRUNCATE')

    missing_weather_data = get_missing_weather_data(project_id)
    
    if not missing_weather_data.empty:
        save_to_bigquery(missing_weather_data, "birding_raw", "pl_ma_spring_weather_data", mode='WRITE_APPEND')
    else:
        print("No missing weather data found, finishing.")

if __name__ == "__main__":
    main()