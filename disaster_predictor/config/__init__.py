import os


def get_project_id() -> str:
    return os.getenv("PROJECT_ID") or "disaster-predictor-470812"


def get_region_name() -> str:
    region = os.getenv("REGION_NAME") or os.getenv("REGION")
    if region:
        return region

    dataset_id = os.getenv("DATASET_ID")
    if dataset_id and dataset_id.endswith("_daily"):
        return dataset_id[: -len("_daily")]

    return "global"
