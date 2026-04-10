import requests
import requests_cache
import openmeteo_requests
import pandas as pd
import time
import random
from timezonefinder import TimezoneFinder
from datetime import date, timedelta, datetime
from typing import Optional, List, Dict
from ..earth_engine_utils import regions_openmeteo

# Hourly Forecast API: soil moisture and soil temperature are only available via hourly, not daily.
# Parameter names for the 5 soil moisture depth layers (m³/m³).
HOURLY_SOIL_MOISTURE_VARS = [
    "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm",
    "soil_moisture_3_to_9cm",
    "soil_moisture_9_to_27cm",
    "soil_moisture_27_to_81cm",
]
# Soil temperature at 4 depths (°C) for optional request.
HOURLY_SOIL_TEMPERATURE_VARS = [
    "soil_temperature_0cm",
    "soil_temperature_6cm",
    "soil_temperature_18cm",
    "soil_temperature_54cm",
]

def get_timezone(lat, lon):
    tf = TimezoneFinder()
    tz_name = tf.timezone_at(lat=lat, lng=lon)
    if tz_name is None:
        raise ValueError(f"Could not determine timezone for coordinates: {lat}, {lon}")
    return tz_name

def fetch_openmeteo_historical_batch(lats: List[float], lons: List[float], start_date: str, end_date: str, max_retries: int = 5) -> List[pd.DataFrame]:
    """Fetch historical weather data from OpenMeteo for multiple locations in one call."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lats,
        "longitude": lons,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m",
                  "wind_gusts_10m", "wind_direction_10m", "shortwave_radiation",
                  "et0_fao_evapotranspiration", "relative_humidity_2m",
                  "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"],
        "timezone": "auto"
    }
    
    for attempt in range(max_retries):
        try:
            time.sleep(1 + random.uniform(0.5, 1.5))
            responses = openmeteo.weather_api(url, params=params, timeout=60)
            
            all_dfs = []
            for i, response in enumerate(responses):
                lat, lon = lats[i], lons[i]
                hourly = response.Hourly()
                tz_name = get_timezone(lat, lon)

                # Reconstruct hourly index
                h_start = pd.Timestamp(start_date, tz=tz_name)
                h_end = pd.Timestamp(end_date, tz=tz_name) + pd.Timedelta(hours=23)
                hourly_index = pd.date_range(start=h_start, end=h_end, freq="h", inclusive="both")

                hourly_fields = [
                    "temperature_2m", "precipitation", "wind_speed_10m",
                    "wind_gusts_10m", "wind_direction_10m", "shortwave_radiation",
                    "et0_fao_evapotranspiration", "relative_humidity_2m",
                    "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"
                ]

                hourly_df = pd.DataFrame({"datetime": hourly_index})
                for j, field_name in enumerate(hourly_fields):
                    values = pd.Series(hourly.Variables(j).ValuesAsNumpy(), dtype='float')
                    values = values.reindex(range(len(hourly_index))).to_numpy()
                    hourly_df[field_name] = values

                hourly_df["date"] = hourly_df["datetime"].dt.date

                # Aggregate to daily
                agg_rules = {
                    "temperature_2m": ["min", "max", "mean"],
                    "precipitation": "sum",
                    "wind_speed_10m": "max",
                    "wind_gusts_10m": "max",
                    "wind_direction_10m": "mean",
                    "shortwave_radiation": "sum",
                    "et0_fao_evapotranspiration": "sum",
                    "relative_humidity_2m": "mean",
                    "soil_moisture_0_to_7cm": "mean",
                    "soil_moisture_7_to_28cm": "mean"
                }

                daily_agg = {}
                for field, method in agg_rules.items():
                    if isinstance(method, list):
                        for m in method:
                            daily_agg[f"{field}_{m}"] = hourly_df.groupby("date")[field].agg(m)
                    else:
                        daily_agg[f"{field}_{method if method != 'mean' else 'mean'}"] = hourly_df.groupby("date")[field].agg(method)

                df = pd.DataFrame(daily_agg).reset_index()
                df = df.rename(columns={
                    "precipitation_sum": "precipitation_sum",
                    "wind_direction_10m_mean": "wind_direction_10m_dominant",
                    "shortwave_radiation_sum": "shortwave_radiation_sum",
                    "et0_fao_evapotranspiration_sum": "evapotranspiration_sum",
                    "soil_moisture_0_to_7cm_mean": "soil_moisture_0_to_7cm_mean",
                    "soil_moisture_7_to_28cm_mean": "soil_moisture_7_to_28cm_mean"
                })
                all_dfs.append(df)
            return all_dfs

        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1: raise
            time.sleep(min(30, 2 ** attempt))

    return [pd.DataFrame()] * len(lats)

def fetch_openmeteo_forecast_batch(lats: List[float], lons: List[float], days_ahead: int = 7, max_retries: int = 5) -> List[pd.DataFrame]:
    """Fetch weather forecast from OpenMeteo for multiple locations in one call."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lats,
        "longitude": lons,
        "daily": ["temperature_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
                  "wind_speed_10m_max", "precipitation_sum", "et0_fao_evapotranspiration"],
        "forecast_days": days_ahead,
        "timezone": "auto"
    }
    
    for attempt in range(max_retries):
        try:
            time.sleep(1 + random.uniform(0.5, 1.5))
            responses = openmeteo.weather_api(url, params=params, timeout=60)
            
            all_dfs = []
            for i, response in enumerate(responses):
                lat, lon = lats[i], lons[i]
                daily = response.Daily()
                tz_name = get_timezone(lat, lon)

                d_start = pd.to_datetime(daily.Time(), unit="s", utc=True).tz_convert(tz_name)
                date_range = pd.date_range(start=d_start, periods=days_ahead, freq="D")
                
                daily_data = {"date": date_range.date}
                daily_fields = ["temperature_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
                                "wind_speed_10m_max", "precipitation_sum", "et0_fao_evapotranspiration"]

                for j, name in enumerate(daily_fields):
                    vals = daily.Variables(j).ValuesAsNumpy()
                    daily_data[name] = vals[:len(date_range)]

                df = pd.DataFrame(daily_data)
                df['evapotranspiration_sum'] = df['et0_fao_evapotranspiration']
                all_dfs.append(df)
            return all_dfs
        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1: raise
            time.sleep(min(30, 2 ** attempt))
    return [pd.DataFrame()] * len(lats)

def fetch_openmeteo_flood_discharge_batch(
    lats: List[float],
    lons: List[float],
    past_days: int = 30,
    forecast_days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_retries: int = 5,
) -> List[pd.DataFrame]:
    """Fetch river discharge data from OpenMeteo Flood API for multiple locations."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://flood-api.open-meteo.com/v1/flood"
    params = {
        "latitude": lats,
        "longitude": lons,
        "daily": [
            "river_discharge",
            "river_discharge_mean",
            "river_discharge_median",
            "river_discharge_max",
            "river_discharge_min",
            "river_discharge_p25",
            "river_discharge_p75"
        ],
    }
    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"] = end_date
    else:
        params["past_days"] = past_days
        params["forecast_days"] = forecast_days
    
    for attempt in range(max_retries):
        try:
            time.sleep(1 + random.uniform(0.5, 1.5))
            responses = openmeteo.weather_api(url, params=params, timeout=60)
            
            all_dfs = []
            for i, response in enumerate(responses):
                lat, lon = lats[i], lons[i]
                daily = response.Daily()
                tz_name = get_timezone(lat, lon)
                
                d_start = pd.to_datetime(daily.Time(), unit="s", utc=True).tz_convert(tz_name)
                discharge = daily.Variables(0).ValuesAsNumpy()
                num_days = len(discharge)
                date_range = pd.date_range(start=d_start, periods=num_days, freq="D")
                
                daily_fields = [
                    "river_discharge", "river_discharge_mean", "river_discharge_median",
                    "river_discharge_max", "river_discharge_min", "river_discharge_p25",
                    "river_discharge_p75"
                ]
                
                daily_data = {"date": date_range.date}
                num_variables = daily.VariablesLength()
                
                for j, name in enumerate(daily_fields):
                    if j < num_variables:
                        try:
                            vals = daily.Variables(j).ValuesAsNumpy()
                            daily_data[name] = vals[:len(date_range)]
                        except Exception:
                            daily_data[name] = [None] * len(date_range)
                    else:
                        daily_data[name] = [None] * len(date_range)
                
                all_dfs.append(pd.DataFrame(daily_data))
            return all_dfs
            
        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "minutely" in err_str or "rate" in err_str or "limit" in err_str or "429" in err_str or "too many requests" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt < max_retries - 1:
                time.sleep(min(5, 2 ** attempt))
                continue
            raise
    
    return [pd.DataFrame()] * len(lats)

def fetch_openmeteo_forecast_hourly_batch(
    lats: List[float],
    lons: List[float],
    past_days: int = 30,
    forecast_days: int = 7,
    include_soil_temperature: bool = True,
    include_sm1_sm2_equivalent: bool = True,
    max_retries: int = 5,
) -> List[pd.DataFrame]:
    """Fetch past + forecast hourly data from Open-Meteo Forecast API for multiple locations."""
    url = "https://api.open-meteo.com/v1/forecast"
    hourly_vars = list(HOURLY_SOIL_MOISTURE_VARS)
    if include_soil_temperature:
        hourly_vars.extend(HOURLY_SOIL_TEMPERATURE_VARS)
    params = {
        "latitude": lats,
        "longitude": lons,
        "hourly": ",".join(hourly_vars),
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    
    data = None
    for attempt in range(max_retries):
        try:
            time.sleep(1 + random.uniform(0.5, 1.5))
            r = requests.get(url, params=params, timeout=60)
            if r.status_code != 200:
                try:
                    err_data = r.json()
                    if err_data.get("error"):
                        raise ValueError(err_data.get("reason", f"HTTP {r.status_code}"))
                except ValueError:
                    pass
            r.raise_for_status()
            data = r.json()
            break
        except (requests.RequestException, ValueError) as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1:
                raise
            time.sleep(min(30, 2 ** attempt))
            
    if data is None:
        return [pd.DataFrame()] * len(lats)

    # If only one location was requested, OpenMeteo returns a dict. 
    # If multiple, it returns a list of dicts.
    if isinstance(data, dict):
        data_list = [data]
    else:
        data_list = data

    all_dfs = []
    for loc_data in data_list:
        hourly = loc_data.get("hourly", {})
        if not hourly or "time" not in hourly:
            all_dfs.append(pd.DataFrame())
            continue

        times = pd.to_datetime(hourly["time"])
        df = pd.DataFrame({"datetime": times})
        for key, values in hourly.items():
            if key != "time":
                df[key] = values
        df["date"] = df["datetime"].dt.date

        agg = {}
        for col in df.columns:
            if col in ("datetime", "date"): continue
            agg[col] = df.groupby("date")[col].mean()
        daily = pd.DataFrame(agg).reset_index()

        if include_sm1_sm2_equivalent and all(c in daily.columns for c in HOURLY_SOIL_MOISTURE_VARS):
            a, b, c, d, e = [daily[v] for v in HOURLY_SOIL_MOISTURE_VARS]
            daily["sm1_mean"] = (1.0 * a + 2.0 * b + 4.0 * c) / 7.0
            daily["sm2_mean"] = (2.0 * c + 18.0 * d + 1.0 * e) / 21.0
        
        all_dfs.append(daily)
        
    return all_dfs

def fetch_openmeteo_historical(lat: float, lon: float, start_date: str, end_date: str, max_retries: int = 5) -> pd.DataFrame:
    """Fetch historical weather data from OpenMeteo."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m",
                  "wind_gusts_10m", "wind_direction_10m", "shortwave_radiation",
                  "et0_fao_evapotranspiration", "relative_humidity_2m",
                  "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"],
        "timezone": "auto"
    }
    
    for attempt in range(max_retries):
        try:
            time.sleep(2 + random.uniform(0.5, 1.5)) # Slightly reduced wait but still cautious
            responses = openmeteo.weather_api(url, params=params, timeout=30)
            response = responses[0]
            hourly = response.Hourly()
            tz_name = get_timezone(lat, lon)

            # Reconstruct hourly index
            h_start = pd.Timestamp(start_date, tz=tz_name)
            h_end = pd.Timestamp(end_date, tz=tz_name) + pd.Timedelta(hours=23)
            hourly_index = pd.date_range(start=h_start, end=h_end, freq="h", inclusive="both")

            hourly_fields = [
                "temperature_2m", "precipitation", "wind_speed_10m",
                "wind_gusts_10m", "wind_direction_10m", "shortwave_radiation",
                "et0_fao_evapotranspiration", "relative_humidity_2m",
                "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm"
            ]

            hourly_df = pd.DataFrame({"datetime": hourly_index})
            for i, field_name in enumerate(hourly_fields):
                values = pd.Series(hourly.Variables(i).ValuesAsNumpy(), dtype='float')
                values = values.reindex(range(len(hourly_index))).to_numpy()
                hourly_df[field_name] = values

            hourly_df["date"] = hourly_df["datetime"].dt.date

            # Aggregate to daily
            agg_rules = {
                "temperature_2m": ["min", "max", "mean"],
                "precipitation": "sum",
                "wind_speed_10m": "max",
                "wind_gusts_10m": "max",
                "wind_direction_10m": "mean",
                "shortwave_radiation": "sum",
                "et0_fao_evapotranspiration": "sum",
                "relative_humidity_2m": "mean",
                "soil_moisture_0_to_7cm": "mean",
                "soil_moisture_7_to_28cm": "mean"
            }

            daily_agg = {}
            for field, method in agg_rules.items():
                if isinstance(method, list):
                    for m in method:
                        daily_agg[f"{field}_{m}"] = hourly_df.groupby("date")[field].agg(m)
                else:
                    daily_agg[f"{field}_{method if method != 'mean' else 'mean'}"] = hourly_df.groupby("date")[field].agg(method)

            df = pd.DataFrame(daily_agg).reset_index()
            df = df.rename(columns={
                "precipitation_sum": "precipitation_sum",
                "wind_direction_10m_mean": "wind_direction_10m_dominant",
                "shortwave_radiation_sum": "shortwave_radiation_sum",
                "et0_fao_evapotranspiration_sum": "evapotranspiration_sum",
                "soil_moisture_0_to_7cm_mean": "soil_moisture_0_to_7cm_mean",
                "soil_moisture_7_to_28cm_mean": "soil_moisture_7_to_28cm_mean"
            })
            return df

        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1: raise
            time.sleep(min(5, 2 ** attempt))

    return pd.DataFrame()

def fetch_openmeteo_forecast(lat: float, lon: float, days_ahead: int = 7, max_retries: int = 5) -> pd.DataFrame:
    """Fetch weather forecast from OpenMeteo."""
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": ["temperature_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
                  "wind_speed_10m_max", "precipitation_sum", "et0_fao_evapotranspiration"],
        "forecast_days": days_ahead,
        "timezone": "auto"
    }
    
    for attempt in range(max_retries):
        try:
            time.sleep(1 + random.uniform(0.5, 1.5))
            responses = openmeteo.weather_api(url, params=params, timeout=30)
            response = responses[0]
            daily = response.Daily()
            tz_name = get_timezone(lat, lon)

            d_start = pd.to_datetime(daily.Time(), unit="s", utc=True).tz_convert(tz_name)
            date_range = pd.date_range(start=d_start, periods=days_ahead, freq="D")
            
            daily_data = {"date": date_range.date}
            daily_fields = ["temperature_2m_max", "relative_humidity_2m_min", "relative_humidity_2m_mean",
                            "wind_speed_10m_max", "precipitation_sum", "et0_fao_evapotranspiration"]

            for i, name in enumerate(daily_fields):
                vals = daily.Variables(i).ValuesAsNumpy()
                daily_data[name] = vals[:len(date_range)]

            df = pd.DataFrame(daily_data)
            df['evapotranspiration_sum'] = df['et0_fao_evapotranspiration']
            return df
        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1: raise
            time.sleep(min(30, 2 ** attempt))
    return pd.DataFrame()

def fetch_openmeteo_flood_discharge(
    lat: float,
    lon: float,
    past_days: int = 30,
    forecast_days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    max_retries: int = 5,
) -> pd.DataFrame:
    """
    Fetch river discharge data from OpenMeteo Flood API (GloFAS).
    
    Either (past_days + forecast_days) or (start_date, end_date) can be used.
    Historical range: 1984-01-01 to 2022-07-31 (GloFAS reanalysis).
    
    Args:
        lat: Latitude
        lon: Longitude
        past_days: Number of past days (used only if start_date/end_date not set)
        forecast_days: Number of forecast days (used only if start_date/end_date not set)
        start_date: Start date YYYY-MM-DD for historical range (mutually exclusive with past_days)
        end_date: End date YYYY-MM-DD for historical range
        max_retries: Maximum number of retry attempts
        
    Returns:
        DataFrame with columns: date, river_discharge, ...
    """
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    openmeteo = openmeteo_requests.Client(session=cache_session)
    
    url = "https://flood-api.open-meteo.com/v1/flood"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "river_discharge",
            "river_discharge_mean",
            "river_discharge_median",
            "river_discharge_max",
            "river_discharge_min",
            "river_discharge_p25",
            "river_discharge_p75"
        ],
    }
    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"] = end_date
    else:
        params["past_days"] = past_days
        params["forecast_days"] = forecast_days
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(0.5 + random.uniform(0.5, 1.5))
            else:
                time.sleep(1 + random.uniform(0.5, 1.5))
            
            responses = openmeteo.weather_api(url, params=params, timeout=30)
            response = responses[0]
            daily = response.Daily()
            tz_name = get_timezone(lat, lon)
            
            # Get time data - similar to forecast function
            d_start = pd.to_datetime(daily.Time(), unit="s", utc=True).tz_convert(tz_name)
            
            # Get number of days from the first variable
            discharge = daily.Variables(0).ValuesAsNumpy()  # river_discharge
            num_days = len(discharge)
            date_range = pd.date_range(start=d_start, periods=num_days, freq="D")
            
            # Get all discharge variables
            # Note: ensemble statistics (mean, median, max, min, p25, p75) are only available for forecasts
            # Historical data may only have river_discharge
            daily_fields = [
                "river_discharge",
                "river_discharge_mean",
                "river_discharge_median",
                "river_discharge_max",
                "river_discharge_min",
                "river_discharge_p25",
                "river_discharge_p75"
            ]
            
            daily_data = {"date": date_range.date}
            num_variables = daily.VariablesLength()
            
            # Only extract variables that are available
            for i, name in enumerate(daily_fields):
                if i < num_variables:
                    try:
                        vals = daily.Variables(i).ValuesAsNumpy()
                        daily_data[name] = vals[:len(date_range)]
                    except Exception as e:
                        # If a variable is not available, fill with NaN
                        print(f"Warning: {name} not available, filling with NaN")
                        daily_data[name] = [None] * len(date_range)
                else:
                    # If we've run out of variables, fill remaining with NaN
                    daily_data[name] = [None] * len(date_range)
            
            df = pd.DataFrame(daily_data)
            return df
            
        except Exception as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "minutely" in err_str or "rate" in err_str or "limit" in err_str or "429" in err_str or "too many requests" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            # Other transient errors: exponential backoff up to max_retries
            if attempt < max_retries - 1:
                time.sleep(min(5, 2 ** attempt))
                continue
            print(f"Error fetching flood discharge data: {e}")
            raise
    
    return pd.DataFrame()


def fetch_openmeteo_forecast_hourly_with_soil_moisture(
    lat: float,
    lon: float,
    past_days: int = 30,
    forecast_days: int = 7,
    include_soil_temperature: bool = True,
    include_sm1_sm2_equivalent: bool = True,
    max_retries: int = 5,
) -> pd.DataFrame:
    """
    Fetch past + forecast hourly data from Open-Meteo Forecast API, including soil moisture
    at all five depths and optionally soil temperature. Aggregates to daily and returns
    a DataFrame suitable for merging with existing daily forecast (e.g. by date).

    The only additional data of value vs the existing daily forecast is soil moisture
    and soil temperature; temp and precip are already in the daily forecast.

    Aggregation for comparability with past data (archive/ERA5):
    - Past data uses Open-Meteo archive "soil_moisture_0_to_7cm" → sm1_mean (0–7 cm)
      and "soil_moisture_7_to_28cm" → sm2_mean (7–28 cm). This function has five
      forecast layers (0–1, 1–3, 3–9, 9–27, 27–81 cm). We add depth-weighted columns
      sm1_mean and sm2_mean so the LLM and outlook can compare the same metrics:
    - sm1_mean (0–7 cm): (1×0_to_1cm + 2×1_to_3cm + 4×3_to_9cm) / 7.
    - sm2_mean (7–28 cm): (2×3_to_9cm + 18×9_to_27cm + 1×27_to_81cm) / 21.
    All are daily means of hourly values (same as archive daily aggregation).

    Args:
        lat: Latitude (WGS84).
        lon: Longitude (WGS84).
        past_days: Number of past days (0–92). Default 30.
        forecast_days: Number of forecast days (0–16). Default 7.
        include_soil_temperature: If True, request and aggregate soil_temperature at 0, 6, 18, 54 cm.
        include_sm1_sm2_equivalent: If True, add sm1_mean and sm2_mean (depth-weighted to match
            archive 0–7 cm and 7–28 cm) for direct comparability with past metrics.
        max_retries: Retries on failure.

    Returns:
        DataFrame with columns: date, raw soil moisture layers (daily mean), optional
        soil_temperature_* (daily mean), and if requested sm1_mean, sm2_mean.
        Empty DataFrame on error.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    hourly_vars = list(HOURLY_SOIL_MOISTURE_VARS)
    if include_soil_temperature:
        hourly_vars.extend(HOURLY_SOIL_TEMPERATURE_VARS)
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(hourly_vars),
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    data = None
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                time.sleep(0.5 + random.uniform(0.5, 1.5))
            else:
                time.sleep(1 + random.uniform(0.5, 1.0))
            r = requests.get(url, params=params, timeout=30)
            if r.status_code != 200:
                try:
                    err_data = r.json()
                    if err_data.get("error"):
                        raise ValueError(err_data.get("reason", f"HTTP {r.status_code}"))
                except ValueError:
                    pass
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                raise ValueError(data.get("reason", "API returned error"))
            break
        except (requests.RequestException, ValueError) as e:
            err_str = str(e).lower()
            if "hourly" in err_str or "daily" in err_str:
                print(f"  [Rate Limit] Hourly/Daily limit reached. Failing task.", flush=True)
                raise
            if "429" in err_str or "too many requests" in err_str or "rate" in err_str or "limit" in err_str:
                wait = 65
                print(f"  [Rate Limit] Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
                continue
            if attempt == max_retries - 1:
                raise
            time.sleep(min(30, 2 ** attempt))
    if data is None:
        return pd.DataFrame()

    hourly = data.get("hourly", {})
    if not hourly or "time" not in hourly:
        return pd.DataFrame()

    times = pd.to_datetime(hourly["time"])
    df = pd.DataFrame({"datetime": times})
    for key, values in hourly.items():
        if key != "time":
            df[key] = values
    df["date"] = df["datetime"].dt.date

    # Daily mean for all requested variables
    agg = {}
    for col in df.columns:
        if col in ("datetime", "date"):
            continue
        agg[col] = df.groupby("date")[col].mean()
    daily = pd.DataFrame(agg).reset_index()

    # Depth-weighted equivalents of archive sm1 (0–7 cm) and sm2 (7–28 cm) for direct
    # comparability: same metric names (sm1_mean, sm2_mean) so LLM/outlook can interpret
    # "soil moisture layer 1" recent vs forecast without schema change.
    if include_sm1_sm2_equivalent and all(
        c in daily.columns
        for c in [
            "soil_moisture_0_to_1cm",
            "soil_moisture_1_to_3cm",
            "soil_moisture_3_to_9cm",
            "soil_moisture_9_to_27cm",
            "soil_moisture_27_to_81cm",
        ]
    ):
        a = daily["soil_moisture_0_to_1cm"]
        b = daily["soil_moisture_1_to_3cm"]
        c = daily["soil_moisture_3_to_9cm"]
        d = daily["soil_moisture_9_to_27cm"]
        e = daily["soil_moisture_27_to_81cm"]
        # sm1: 0–7 cm = 1 cm (0–1) + 2 cm (1–3) + 4 cm (3–7 from 3–9 layer)
        daily["sm1_mean"] = (1.0 * a + 2.0 * b + 4.0 * c) / 7.0
        # sm2: 7–28 cm = 2 cm (7–9 from 3–9) + 18 cm (9–27) + 1 cm (27–28 from 27–81)
        daily["sm2_mean"] = (2.0 * c + 18.0 * d + 1.0 * e) / 21.0

    return daily


def merge_glofas_river_discharge_onto_openmeteo_daily(
    weather_daily: pd.DataFrame,
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Left-join GloFAS river discharge onto archive daily weather rows.

    Uses Open-Meteo Flood API daily variable ``river_discharge`` (discharge in the last 24 hours),
    comparable to Copernicus ``river_discharge_in_the_last_24_hours`` — not ``river_discharge_mean``.
    """
    if weather_daily.empty:
        return weather_daily
    out = weather_daily.copy()
    if "river_discharge" in out.columns:
        out = out.drop(columns=["river_discharge"])
    try:
        flood = fetch_openmeteo_flood_discharge(
            lat, lon, start_date=start_date, end_date=end_date
        )
    except Exception as e:
        print(f"  Warning: GloFAS river_discharge fetch failed: {e}")
        out["river_discharge"] = pd.NA
        return out
    if flood is None or flood.empty or "river_discharge" not in flood.columns:
        out["river_discharge"] = pd.NA
        return out
    f = flood[["date", "river_discharge"]].copy()
    out["date_norm"] = pd.to_datetime(out["date"]).dt.normalize()
    f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
    out = out.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left")
    out = out.drop(columns=["date_norm"])
    return out


def merge_glofas_river_discharge_onto_openmeteo_forecast(
    forecast_daily: pd.DataFrame,
    lat: float,
    lon: float,
    *,
    past_days: int = 0,
    forecast_days: int = 7,
) -> pd.DataFrame:
    """
    Left-join GloFAS river discharge onto daily rows from ``fetch_openmeteo_forecast``.

    Fetches via ``fetch_openmeteo_flood_discharge`` with the same ``past_days`` / ``forecast_days``
    window (default: forecast only, 7 days — matching ``fetch_openmeteo_forecast(..., days_ahead=7)``).
    Uses daily ``river_discharge``, not ``river_discharge_mean``.
    """
    if forecast_daily.empty:
        return forecast_daily
    out = forecast_daily.copy()
    if "river_discharge" in out.columns:
        out = out.drop(columns=["river_discharge"])
    try:
        flood = fetch_openmeteo_flood_discharge(
            lat, lon, past_days=past_days, forecast_days=forecast_days
        )
    except Exception as e:
        print(f"  Warning: GloFAS river_discharge (forecast) failed: {e}")
        out["river_discharge"] = pd.NA
        return out
    if flood is None or flood.empty or "river_discharge" not in flood.columns:
        out["river_discharge"] = pd.NA
        return out
    f = flood[["date", "river_discharge"]].copy()
    out["date_norm"] = pd.to_datetime(out["date"]).dt.normalize()
    f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
    out = out.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left")
    out = out.drop(columns=["date_norm"])
    return out


def sync_openmeteo_all_regions(project_id: str, dataset_id: str, historical_start: str, historical_end: str):
    """Sync historical archive (weather + GloFAS river_discharge) and forecast (weather + GloFAS) for all regions using batch fetching."""
    from ..bq_utils import save_to_bigquery
    
    regions = regions_openmeteo()
    region_names = list(regions.keys())
    lats = [regions[n]['lat'] for n in region_names]
    lons = [regions[n]['lon'] for n in region_names]
    
    # Process in batches of 50 (OpenMeteo limit)
    batch_size = 50
    hist_all, fore_all = [], []
    
    for i in range(0, len(region_names), batch_size):
        batch_names = region_names[i:i+batch_size]
        batch_lats = lats[i:i+batch_size]
        batch_lons = lons[i:i+batch_size]
        n_batch = len(batch_names)
        
        print(f"Processing batch {i//batch_size + 1}: {n_batch} regions")
        
        # 1. Historical
        print(f"  Fetching historical data...")
        h_weather_list = fetch_openmeteo_historical_batch(batch_lats, batch_lons, historical_start, historical_end)
        h_flood_list = fetch_openmeteo_flood_discharge_batch(batch_lats, batch_lons, start_date=historical_start, end_date=historical_end)
        
        for j, name in enumerate(batch_names):
            h_weather = h_weather_list[j]
            h_flood = h_flood_list[j]
            if not h_weather.empty:
                if not h_flood.empty and "river_discharge" in h_flood.columns:
                    f = h_flood[["date", "river_discharge"]].copy()
                    h_weather["date_norm"] = pd.to_datetime(h_weather["date"]).dt.normalize()
                    f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
                    h_weather = h_weather.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left").drop(columns=["date_norm"])
                else:
                    h_weather["river_discharge"] = pd.NA
                h_weather['region_name'] = name
                hist_all.append(h_weather)

        # 2. Forecast
        print(f"  Fetching forecast data...")
        f_weather_list = fetch_openmeteo_forecast_batch(batch_lats, batch_lons)
        f_sm_list = fetch_openmeteo_forecast_hourly_batch(batch_lats, batch_lons, past_days=0, forecast_days=7, include_soil_temperature=False, include_sm1_sm2_equivalent=True)
        f_flood_list = fetch_openmeteo_flood_discharge_batch(batch_lats, batch_lons, past_days=0, forecast_days=7)
        
        for j, name in enumerate(batch_names):
            f_weather = f_weather_list[j]
            f_sm = f_sm_list[j]
            f_flood = f_flood_list[j]
            if not f_weather.empty:
                if not f_sm.empty and 'date' in f_sm.columns and 'sm1_mean' in f_sm.columns and 'sm2_mean' in f_sm.columns:
                    f_weather = f_weather.merge(f_sm[['date', 'sm1_mean', 'sm2_mean']], on='date', how='left')
                if not f_flood.empty and "river_discharge" in f_flood.columns:
                    f = f_flood[["date", "river_discharge"]].copy()
                    f_weather["date_norm"] = pd.to_datetime(f_weather["date"]).dt.normalize()
                    f["date_norm"] = pd.to_datetime(f["date"]).dt.normalize()
                    f_weather = f_weather.merge(f[["date_norm", "river_discharge"]], on="date_norm", how="left").drop(columns=["date_norm"])
                else:
                    f_weather["river_discharge"] = pd.NA
                f_weather['region_name'] = name
                fore_all.append(f_weather)

    if hist_all:
        df_hist = pd.concat(hist_all, ignore_index=True)
        save_to_bigquery(df_hist, project_id, dataset_id, "openmeteo_weather", mode="WRITE_APPEND")
        print(f"✓ Saved {len(df_hist)} historical weather records")
    
    if fore_all:
        df_fore = pd.concat(fore_all, ignore_index=True)
        save_to_bigquery(df_fore, project_id, dataset_id, "openmeteo_forecast", mode="WRITE_TRUNCATE")
        print(f"✓ Saved {len(df_fore)} forecast records")
