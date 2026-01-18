from garminconnect import Garmin
import pandas as pd
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

username = os.getenv('garmin_login')
password = os.getenv('garmin_password')

try:
    api = Garmin(username, password)
    api.login()
except Exception as e:
    print(f"Error establishing connection to the API: {e}")

def get_activities(days_back):
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days_back)

    activities = api.get_activities_by_date(start_date.isoformat(), today.isoformat())
    activities = pd.DataFrame(activities)
    
    columns = [
        'activityName', 'startTimeLocal', 'activityType', 'distance', 'duration', 'elapsedDuration', 'elevationGain',
        'averageSpeed', 'maxSpeed', 'calories', 'bmrCalories', 'averageHR', 'maxHR',
        'averageBikingCadenceInRevPerMinute', 'aerobicTrainingEffect', 'anaerobicTrainingEffect',
        'minTemperature', 'activityTrainingLoad', 'hrTimeInZone_0', 'hrTimeInZone_1', 'hrTimeInZone_2',
        'hrTimeInZone_3', 'hrTimeInZone_4', 'hrTimeInZone_5', 'averageRunningCadenceInStepsPerMinute',
        'steps', 'avgStrideLength', 'averageSwimCadenceInStrokesPerMinute', 'averageSwolf'
    ]

    activities = activities.reindex(columns=columns)

    activities.loc[:,'activityType'] = activities['activityType'].apply(lambda x: x.get('typeKey'))
    activities['totalTimeInZones'] = activities[['hrTimeInZone_0', 'hrTimeInZone_1', 'hrTimeInZone_2', 'hrTimeInZone_3', 'hrTimeInZone_4', 'hrTimeInZone_5']].sum(axis=1)

    for zone in range(6):
        zone_col = f'hrTimeInZone_{zone}'
        activities[zone_col] = activities[zone_col] / activities['totalTimeInZones'] * 100

    activities = activities.drop(columns=['totalTimeInZones'])

    activities.loc[:,'distance'] = activities['distance'] / 1000
    activities.loc[:,'duration'] = activities['duration'] / 60
    activities.loc[:,'elapsedDuration'] = activities['elapsedDuration'] / 60
    activities.loc[:,'averageSpeed'] = activities['averageSpeed'] * 3.6
    activities.loc[:,'maxSpeed'] = activities['maxSpeed'] * 3.6

    activities_df = activities.rename(columns={
        'hrTimeInZone_0': 'hr_zone0_percent',
        'hrTimeInZone_1': 'hr_zone1_percent',
        'hrTimeInZone_2': 'hr_zone2_percent',
        'hrTimeInZone_3': 'hr_zone3_percent',
        'hrTimeInZone_4': 'hr_zone4_percent',
        'hrTimeInZone_5': 'hr_zone5_percent',
        'distance': 'distance_km',
        'duration': 'duration_min',
        'elapsedDuration': 'elapsedDuration_min',
        'elevationGain': 'elevationGain_m',
        'averageSpeed': 'averageSpeed_kmh',
        'maxSpeed': 'maxSpeed_kmh'
        })
    
    if activities_df.empty:
        print("No recent activities collected")
    else:
        print(f"{len(activities_df)} recent activities collected")

    return activities_df

def get_sleep_data(days_back):

    all_sleep_data = []

    for i in range(days_back):
        dt = datetime.date.today() - datetime.timedelta(days=i)
        sleep_data = api.get_sleep_data(dt.isoformat())

        sleep_data = sleep_data['dailySleepDTO']
        sleep_data = {key: sleep_data[key] for key in [
            'calendarDate', 'sleepTimeSeconds', 'sleepStartTimestampLocal', 'sleepEndTimestampLocal', 'deepSleepSeconds', 'lightSleepSeconds', 'remSleepSeconds', 'awakeSleepSeconds',
            'averageRespirationValue', 'lowestRespirationValue', 'highestRespirationValue'
        ] if key in sleep_data}

        all_sleep_data.append(sleep_data)

    sleep_last_7d = pd.DataFrame(all_sleep_data)

    sleep_last_7d['sleepStartTimestampLocal'] = pd.to_datetime(sleep_last_7d['sleepStartTimestampLocal'], unit='ms').dt.time
    sleep_last_7d['sleepEndTimestampLocal'] = pd.to_datetime(sleep_last_7d['sleepEndTimestampLocal'], unit='ms').dt.time
    sleep_last_7d.loc[:, 'sleepTimeSeconds'] = sleep_last_7d['sleepTimeSeconds'] / 60
    sleep_last_7d.loc[:, 'deepSleepSeconds'] = sleep_last_7d['deepSleepSeconds'] / 60
    sleep_last_7d.loc[:, 'lightSleepSeconds'] = sleep_last_7d['lightSleepSeconds'] / 60
    sleep_last_7d.loc[:, 'remSleepSeconds'] = sleep_last_7d['remSleepSeconds'] / 60
    sleep_last_7d.loc[:, 'awakeSleepSeconds'] = sleep_last_7d['awakeSleepSeconds'] / 60

    sleep_df = sleep_last_7d.rename(columns={
        'sleepTimeSeconds': 'sleepTime_min',
        'deepSleepSeconds': 'deepSleep_min',
        'lightSleepSeconds': 'lightSleep_min',
        'remSleepSeconds': 'remSleep_min',
        'awakeSleepSeconds': 'awakeSleep_min'})
    
    if sleep_df.empty:
        print("No recent sleep data collected") 
    else:
        print(f"{len(sleep_df)} nights of sleep data collected")
    
    return sleep_df

def get_health_data(days_back):

    all_health_data = []

    for i in range(days_back):
        dt = datetime.date.today() - datetime.timedelta(days=i+1)
        health_data = api.get_stats(dt.isoformat())

        health_data = {key: health_data[key] for key in [
            'restingHeartRate', 'lastSevenDaysAvgRestingHeartRate', 'averageStressLevel', 'stressPercentage', 'restStressPercentage', 'activityStressPercentage',
            'uncategorizedStressPercentage', 'lowStressPercentage', 'mediumStressPercentage', 'highStressPercentage', 'veryHighStressPercentage', 'stressQualifier',
            'bodyBatteryChargedValue', 'bodyBatteryDrainedValue', 'bodyBatteryHighestValue', 'bodyBatteryLowestValue', 'avgWakingRespirationValue'
            ] if key in health_data}

        health_data['calendarDate'] = dt
        all_health_data.append(health_data)

    health_df = pd.DataFrame(all_health_data)

    column_order = ['calendarDate'] + [col for col in health_df.columns if col != 'calendarDate']
    health_df = health_df.reindex(columns=column_order)

    if health_df.empty:
        print("No recent health data collected")
    else:
        print(f"{len(health_df)} days of health data collected")

    return health_df

def get_training_data(days_back):
    max_retries = days_back
    retry_count = 0

    fields = [
        "monthlyLoadAerobicLow", "monthlyLoadAerobicHigh", "monthlyLoadAnaerobic",
        "monthlyLoadAerobicLowTargetMin", "monthlyLoadAerobicLowTargetMax",
        "monthlyLoadAerobicHighTargetMin", "monthlyLoadAerobicHighTargetMax",
        "monthlyLoadAnaerobicTargetMin", "monthlyLoadAnaerobicTargetMax",
        "trainingBalanceFeedbackPhrase"
    ]

    training_data_list = [] 

    while retry_count < max_retries:
        dt = datetime.date.today() - datetime.timedelta(days=retry_count)
        response = api.get_training_status(dt.isoformat())

        if response.get("mostRecentVO2Max") is not None:
            most_recent_vo2max = response["mostRecentVO2Max"]["generic"]
            metrics = response.get("mostRecentTrainingLoadBalance", {}).get("metricsTrainingLoadBalanceDTOMap", {}).get("3389646841", {})

            training_data_list.append({
                "calendarDate": most_recent_vo2max.get("calendarDate"),
                "vo2max": most_recent_vo2max.get("vo2MaxPreciseValue"),
                **{field: metrics.get(field) for field in fields}
            })

        retry_count += 1

    training_data_df = pd.DataFrame(training_data_list)

    if training_data_df.empty:
        print("No recent training data collected") 
    else:
        print(f"{len(training_data_df)} days of training data collected")

    return training_data_df

import gspread
import datetime
from google.oauth2.service_account import Credentials

def get_training_plan(days_back, days_ahead):
    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    creds = Credentials.from_service_account_file('service_account.json', scopes=scope)
    gc = gspread.authorize(creds)

    spreadsheet_id = os.getenv('spreadsheet_id')
    worksheet = gc.open_by_key(spreadsheet_id).sheet1

    data = worksheet.get_all_values()
    training_plan_df = pd.DataFrame(data[1:], columns=data[0])

    date_start = datetime.datetime.today() - datetime.timedelta(days=days_back)
    date_end = datetime.datetime.today() + datetime.timedelta(days=days_ahead)

    # Filter the data first
    filtered_df = training_plan_df[(pd.to_datetime(training_plan_df['Date']) >= date_start) & (pd.to_datetime(training_plan_df['Date']) <= date_end)]

    if filtered_df.empty:
        print("No training plan data collected")
    else:
        print(f"{len(filtered_df)} days of training plan data collected")

    return filtered_df