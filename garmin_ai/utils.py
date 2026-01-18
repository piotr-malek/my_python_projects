import json
import pandas as pd
import argparse

def combine_garmin_data(activities_df=None, sleep_df=None, health_df=None, training_df=None):
    """
    Convert multiple Garmin dataframes into a JSON string.
    
    Args:
        activities_df (DataFrame, optional): Activities data
        sleep_df (DataFrame, optional): Sleep data
        health_df (DataFrame, optional): Health data
        training_df (DataFrame, optional): Training data
        
    Returns:
        str: JSON string containing all Garmin data
    """
    garmin_data = {
        'activities_last_14d': [],
        'sleep_last_7d': [],
        'health_last_7d': [],
        'most_recent_training_data': []
    }
    
    if isinstance(activities_df, pd.DataFrame) and not activities_df.empty:
        garmin_data['activities_last_14d'] = activities_df.to_dict(orient='records')
        
    if isinstance(sleep_df, pd.DataFrame) and not sleep_df.empty:
        garmin_data['sleep_last_7d'] = sleep_df.to_dict(orient='records')
        
    if isinstance(health_df, pd.DataFrame) and not health_df.empty:
        garmin_data['health_last_7d'] = health_df.to_dict(orient='records')
        
    if isinstance(training_df, pd.DataFrame) and not training_df.empty:
        garmin_data['most_recent_training_data'] = training_df.to_dict(orient='records')
    
    return json.dumps(garmin_data, indent=4, default=str)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Request LLM feedback with the provided prompt.')
    parser.add_argument('prompt_file', help='Path to the prompt text file')
    return parser.parse_args()

