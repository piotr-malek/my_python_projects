import requests
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

from bigquery import load_from_bigquery, save_to_bigquery
import requests_cache

requests_cache.install_cache('ebird_cache', backend='sqlite', expire_after=86400) 

load_dotenv()

headers = {
    'x-ebirdapitoken': os.getenv('EBIRD_TOKEN')
}

def get_sightings(region_code, species_dict, start_date, end_date, stop_at_first=True):
    """
    Get bird observations for specified species within a date range.
    
    Parameters:
    - region_code: eBird region code
    - species_dict: Dictionary mapping common names to species codes
    - start_date: Start date (datetime-like)
    - end_date: End date (datetime-like)
    - stop_at_first: If True, stop after finding first observation for a species
    
    Returns:
    - DataFrame of observations
    """
    all_observations = pd.DataFrame()
    
    for common_name, species_code in species_dict.items():
        df = pd.DataFrame()
        found_first_observation = False
        
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        print(f"Collecting data for {common_name} in date range {start_date} to {end_date}")
        
        for dt in pd.date_range(start_dt, end_dt):
            if stop_at_first and found_first_observation:
                break
                
            y, m, d = dt.year, dt.month, dt.day
            
            url = f"https://api.ebird.org/v2/data/obs/{region_code}/historic/{y}/{m}/{d}"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                
                if data:
                    data = [
                        {
                            "species_code": obs.get("speciesCode"),
                            "common_name": obs.get("comName"),
                            "scientific_name": obs.get("sciName"),
                            "location_id": obs.get("locId"),
                            "location_name": obs.get("locName"),
                            "observation_date": obs.get("obsDt"),
                            "count": obs.get("howMany"),
                            "latitude": obs.get("lat"),
                            "longitude": obs.get("lng"),
                            "is_valid": obs.get("obsValid"),
                            "is_reviewed": obs.get("obsReviewed"),
                            "is_location_private": obs.get("locationPrivate"),
                            "submission_id": obs.get("subId")
                        }
                        for obs in data
                        if obs.get('speciesCode') == species_code
                    ]
                    
                    if data:
                        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)
                        if stop_at_first:
                            found_first_observation = True
        
        if df.empty:
            print(f"No data for species: {common_name} in date range {start_date} to {end_date}")
        else:
            print(f"Collected {len(df)} sighting(s) for {common_name} in date range {start_date} to {end_date}")

        if not df.empty:
            try:
                df['observation_date'] = pd.to_datetime(df['observation_date'])
            except (ValueError, TypeError):
                df.loc[df['observation_date'].isnull(), 'observation_date'] = None
                
            df['count'] = pd.to_numeric(df['count'], errors='coerce')
            df['count'] = df['count'].fillna(0).astype('Int64')
            
            all_observations = pd.concat([all_observations, df], ignore_index=True)

    print(f"Total observations collected: {len(all_observations)}")
    return all_observations

def get_first_arrivals_by_year(region_code, species_dict, year_start, year_end, 
                              month_start=1, month_end=6, day_start=1, day_end=30):
    """
    Get first arrivals for each species across multiple years.
    
    Parameters:
    - region_code: eBird region code
    - species_dict: Dictionary mapping common names to species codes
    - year_start, year_end: Range of years to check
    - month_start, month_end: Limit search to these months (default: Jan-Jun)
    - day_start, day_end: Start/end days for the range
    
    Returns:
    - DataFrame of first observations for each species in each year
    """
    all_first_arrivals = pd.DataFrame()
    
    for year in range(year_start, year_end + 1):
        start_date = pd.Timestamp(year=year, month=month_start, day=day_start)
        end_date = pd.Timestamp(year=year, month=month_end, day=day_end)
        
        print(f"Searching for first arrivals in {year} from {start_date.strftime('%b %d')} to {end_date.strftime('%b %d')}")
        
        yearly_observations = get_sightings(
            region_code=region_code,
            species_dict=species_dict,
            start_date=start_date,
            end_date=end_date,
            stop_at_first=True 
        )
        
        if not yearly_observations.empty:
            yearly_observations['year'] = year
            all_first_arrivals = pd.concat([all_first_arrivals, yearly_observations], ignore_index=True)
    
    all_first_arrivals['common_name'] = all_first_arrivals['common_name'].str.replace(" ", "_").str.lower()
    
    save_to_bigquery(
        all_first_arrivals,
        dataset_id='birding',
        table_id=f'{region_code}_first_sightings_trial',
        mode='WRITE_TRUNCATE'
    )

def get_median_arrival_dates(bq_arrival_region):

     project_id = os.getenv("BQ_PROJECT_ID")

     median_arrival_dates = load_from_bigquery(
     f"""
     WITH day_of_year AS (
     SELECT 
         common_name,
         EXTRACT(YEAR FROM observation_date) AS year,
         EXTRACT(DAYOFYEAR FROM observation_date) AS day_num
     FROM {project_id}.birding.{bq_arrival_region}_first_sightings
     ),

     median_days AS (
     SELECT
         common_name,
         PERCENTILE_CONT(day_num, 0.5) OVER (PARTITION BY common_name) AS median_day
     FROM day_of_year
     GROUP BY common_name, day_num
     )

     SELECT 
         common_name,
         DATE_ADD(DATE('2000-01-01'), INTERVAL CAST(median_day AS INT64) - 1 DAY) AS median_arrival_date
     FROM median_days
     GROUP BY common_name, median_day
     """
    )

     median_arrival_dates['median_arrival_date'] = pd.to_datetime(median_arrival_dates['median_arrival_date'])

     return median_arrival_dates


def validate_early_arrivals(arrival_region):

    project_id = os.getenv("BQ_PROJECT_ID")
    early_arrivals = pd.DataFrame()

    bq_arrival_region = arrival_region.replace("-", "_").lower()
    median_arrival_dates = get_median_arrival_dates(bq_arrival_region)

    first_arrivals = load_from_bigquery(
        f"""
        SELECT 
            * EXCEPT(observation_date),
            DATE(observation_date) as observation_date
        FROM {project_id}.birding.{bq_arrival_region}_first_sightings
        """
    )

    first_arrivals['observation_date'] = pd.to_datetime(first_arrivals['observation_date'])

    for bird in first_arrivals['common_name'].unique():
        bird_first_arrivals = first_arrivals[first_arrivals['common_name'] == bird]
        bird_median_arrival_date = median_arrival_dates[median_arrival_dates['common_name'] == bird]

        if bird_median_arrival_date.empty:
            print(f"No median arrival date found for {bird}")
            continue

        for _, row in bird_first_arrivals.iterrows():
            first_arrival_date = row['observation_date']
            submission_id = row['submission_id']

            norm_first_date = first_arrival_date.replace(year=2000)
            median_date = bird_median_arrival_date.iloc[0]['median_arrival_date']
            
            if norm_first_date < median_date - datetime.timedelta(days=30):
                early_arrivals = pd.concat([early_arrivals, pd.DataFrame({
                    'common_name': [bird],
                    'species_code': [row['species_code']],
                    'submission_id': [submission_id],
                    'first_arrival_date': [first_arrival_date],
                    'median_arrival_date': [median_date.strftime('%m-%d')],
                })], ignore_index=True)

    if not early_arrivals.empty:
        for bird in early_arrivals['common_name'].unique():
            num_early = len(early_arrivals[early_arrivals['common_name'] == bird])
            print(f"{bird}: {num_early} early arrivals found")
    else:
        print("No early arrivals found.")
        return
    
    arrivals_to_validate = pd.merge(
        early_arrivals,
        first_arrivals,
        on='submission_id',
        how='left'
    )

    past_month_records = pd.DataFrame()
    
    for _, row in arrivals_to_validate.iterrows():
        first_arrival_date = pd.to_datetime(row['first_arrival_date_x' if 'first_arrival_date_x' in row else 'first_arrival_date'])
        species_code = row['species_code_x' if 'species_code_x' in row else 'species_code']
        common_name = row['common_name_x' if 'common_name_x' in row else 'common_name']
        
        month_before_first_arrival = first_arrival_date - pd.DateOffset(months=1)
        
        species_dict = {common_name: species_code}
        
        previous_sightings = get_sightings(
            region_code=arrival_region,
            species_dict=species_dict,
            start_date=month_before_first_arrival,
            end_date=first_arrival_date - pd.DateOffset(days=1),
            stop_at_first=False
        )
        
        past_month_records = pd.concat([past_month_records, previous_sightings], ignore_index=True)

    for idx, row in first_arrivals.iterrows():
        if row['submission_id'] in arrivals_to_validate['submission_id'].values:
            if previous_sightings.empty:
                if row.get('is_reviewed'):
                    if ((row.get('common_name') == 'white_stork' and row.get('count', 0) >= 2) or 
                        (row.get('common_name') != 'white_stork' and row.get('count', 0) >= 5)):
                        first_arrivals.loc[idx, 'status'] = 'confirmed_early'
                    else:
                        first_arrivals.loc[idx, 'status'] = 'possible_early'
                else:
                    first_arrivals.loc[idx, 'status'] = 'possible_early'
            elif (previous_sightings['submission_id'] == row['submission_id']).any():
                if len(previous_sightings) > 1:
                    first_arrivals.loc[idx, 'status'] = 'probable_overwintering'
                else:
                    first_arrivals.loc[idx, 'status'] = 'possible_overwintering'
        else:
            first_arrivals.loc[idx, 'status'] = 'normal'

    save_to_bigquery(
        first_arrivals,
        dataset_id='birding_raw',
        table_id=f'{bq_arrival_region}_first_sightings',
        mode='WRITE_TRUNCATE'
    )

if __name__ == "__main__":
    
    birds = {
        "white_stork": "whisto1",
        "common_swift": "comswi",
        "barn_swallow": "barswa",
        "common_cuckoo": "comcuc",
        "western_yellow_wagtail": "eaywag1"
    }

    get_first_arrivals_by_year("PL-MA", birds, 2016, 2025)
    validate_early_arrivals("PL-MA")