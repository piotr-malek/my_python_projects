#!/usr/bin/env python3
"""
Build river discharge climatology (monthly percentiles per region) from daily_ingestion.openmeteo_weather.
Writes to climatology.river_discharge_climatology for use in flood/landslide modifiers and outlook.

Schema: region, month, river_discharge_p20, river_discharge_p80, river_discharge_p95
Run after backfill or once enough daily data has been ingested.
"""
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env", override=False)

import pandas as pd


def main():
    from utils.bq_utils import load_from_bigquery, save_to_bigquery

    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        print("PROJECT_ID not set.")
        return 1
    daily_dataset = "daily_ingestion"
    daily_table = "openmeteo_weather"
    clim_dataset = "climatology"
    clim_table = "river_discharge_climatology"

    query = f"""
    SELECT region_name as region, date, river_discharge
    FROM `{project_id}.{daily_dataset}.{daily_table}`
    WHERE river_discharge IS NOT NULL
    """
    df = load_from_bigquery(query, project_id=project_id)
    if df is None or df.empty:
        print(f"No data in {daily_dataset}.{daily_table}. Run backfill or daily ingestion first.")
        return 1
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month
    agg = df.groupby(["region", "month"])["river_discharge"].agg(
        p20=lambda x: x.quantile(0.20),
        p80=lambda x: x.quantile(0.80),
        p95=lambda x: x.quantile(0.95),
    ).reset_index()
    agg = agg.rename(columns={"p20": "river_discharge_p20", "p80": "river_discharge_p80", "p95": "river_discharge_p95"})
    save_to_bigquery(agg, project_id, clim_dataset, clim_table, mode="WRITE_TRUNCATE")
    print(f"Wrote {len(agg)} rows to {clim_dataset}.{clim_table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
