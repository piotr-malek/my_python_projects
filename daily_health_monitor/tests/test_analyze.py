import pandas as pd

from config import settings
from pipeline.analyzer import Analyzer


def test_garmin_fitness_partial_with_coros():
    df = pd.DataFrame(
        [
            {"trainer": False, "device_name": "COROS PACE 3"},
            {"trainer": False, "device_name": "Edge 530"},
        ]
    )
    assert Analyzer._garmin_fitness_partial(df, settings.GARMIN_DEVICES) is True


def test_garmin_fitness_partial_all_garmin():
    df = pd.DataFrame(
        [
            {"trainer": False, "device_name": "Edge 530"},
            {"trainer": False, "device_name": "Garmin vivoactive 4S"},
        ]
    )
    assert Analyzer._garmin_fitness_partial(df, settings.GARMIN_DEVICES) is False


def test_garmin_fitness_partial_indoor():
    df = pd.DataFrame([{"trainer": True, "device_name": "Wahoo KICKR CORE"}])
    assert Analyzer._garmin_fitness_partial(df, settings.GARMIN_DEVICES) is True
