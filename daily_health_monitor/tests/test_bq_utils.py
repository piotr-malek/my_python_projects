import pandas as pd

from storage.bigquery import BigQueryClient


def test_date_column_coerced():
    df = pd.DataFrame([{"date": "2026-05-20", "rhr": 52.0}])
    out = BigQueryClient.prepare_dataframe(df)
    assert str(out.iloc[0]["date"]) == "2026-05-20"


def test_int_column_coerced():
    df = pd.DataFrame([{"strava_activity_id": "12345", "moving_time": 3600.0}])
    out = BigQueryClient.prepare_dataframe(df)
    assert out.iloc[0]["strava_activity_id"] == 12345


def test_float_column_coerced_from_string():
    df = pd.DataFrame([{"avg_cadence": "85.5", "avg_hr": None}])
    out = BigQueryClient.prepare_dataframe(df)
    assert out.iloc[0]["avg_cadence"] == 85.5
    assert pd.isna(out.iloc[0]["avg_hr"])
