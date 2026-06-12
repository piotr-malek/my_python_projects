import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=False)

_settings = None


def _env(name, *, default=None, required=True):
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


class Settings:
    def __init__(self):
        self.BQ_PROJECT_ID = _env("BQ_PROJECT_ID")
        self.BQ_DATASET_ID = _env("BQ_DATASET_ID")
        self.BQ_LOCATION = _env("BQ_LOCATION")
        self.SERVICE_ACCOUNT_PATH = Path(
            _env("GOOGLE_APPLICATION_CREDENTIALS")
        ).expanduser()
        self.GARMIN_EMAIL = _env("GARMIN_EMAIL")
        self.GARMIN_PASSWORD = _env("GARMIN_PASSWORD")
        self.GARMINTOKENS = Path(_env("GARMINTOKENS")).expanduser()
        self.STRAVA_CLIENT_ID = _env("STRAVA_CLIENT_ID")
        self.STRAVA_CLIENT_SECRET = _env("STRAVA_CLIENT_SECRET")
        self.STRAVA_REFRESH_TOKEN = _env("STRAVA_REFRESH_TOKEN")
        self.STRAVA_TOKEN_PATH = Path(_env("STRAVA_TOKEN_PATH")).expanduser()
        self.STRAVA_LOOKBACK_DAYS = int(_env("STRAVA_LOOKBACK_DAYS"))
        self.ANALYSIS_DAYS = int(_env("ANALYSIS_DAYS"))
        self.OLLAMA_HOST = _env("OLLAMA_HOST")
        self.OLLAMA_MODEL = _env("OLLAMA_MODEL")
        self.OLLAMA_NUM_PREDICT = int(_env("OLLAMA_NUM_PREDICT"))
        self.OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "32768"))
        self.SMTP_HOST = _env("SMTP_HOST")
        self.SMTP_PORT = int(_env("SMTP_PORT"))
        self.SMTP_USER = _env("SMTP_USER")
        self.SMTP_PASSWORD = _env("SMTP_PASSWORD")
        self.EMAIL_TO = _env("EMAIL_TO")
        self.DIGEST_HOUR = int(_env("DIGEST_HOUR"))
        self.DIGEST_MINUTE = int(_env("DIGEST_MINUTE"))
        self.FTP_WATTS = float(_env("FTP_WATTS"))
        self.THRESHOLD_HR = float(_env("THRESHOLD_HR"))
        self.LOCAL_STATE_DIR = Path(_env("LOCAL_STATE_DIR")).expanduser()
        self.GARMIN_DEVICES = frozenset(
            {"garmin", "edge", "vivoactive", "forerunner", "fenix", "epix"}
        )
        self.CTL_FLOOR = float(os.getenv("CTL_FLOOR", "30"))
        self.STRESS_TIME_BANDS = {
            "morning": os.getenv("STRESS_BAND_MORNING", "06:00–09:00"),
            "work": os.getenv("STRESS_BAND_WORK", "09:00–18:00"),
            "evening": os.getenv("STRESS_BAND_EVENING", "18:00–22:00"),
        }

    @property
    def LOG_DIR(self):
        return self.LOCAL_STATE_DIR / "logs"

    @property
    def FALLBACK_DIGEST_DIR(self):
        return self.LOCAL_STATE_DIR / "fallback_digests"

    def table_id(self, name):
        return f"`{self.BQ_PROJECT_ID}.{self.BQ_DATASET_ID}.{name}`"


def get_settings():
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings():
    """Clear cached settings (for tests)."""
    global _settings
    _settings = None


class _SettingsProxy:
    def __getattr__(self, name):
        return getattr(get_settings(), name)


settings = _SettingsProxy()
