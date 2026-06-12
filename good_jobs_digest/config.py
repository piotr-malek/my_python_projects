"""Load settings from environment (see `.env.example`)."""

import os
from pathlib import Path

from dotenv import load_dotenv

from core.env import normalize_google_credentials_env

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env", override=False)
normalize_google_credentials_env(ROOT)


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    def __init__(self):
        self.OLLAMA_HOST = _env("OLLAMA_HOST", "http://localhost:11434")
        self.OLLAMA_MODEL = _env("OLLAMA_MODEL", "qwen3:14b-q4_K_M")
        self.OLLAMA_NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT", "512"))
        self.OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "30m")
        self.OLLAMA_SCORE_WORKERS = max(1, int(os.getenv("OLLAMA_SCORE_WORKERS", "2")))
        self.OLLAMA_SCORE_BATCH_SIZE = max(1, int(os.getenv("OLLAMA_SCORE_BATCH_SIZE", "4")))
        self.OLLAMA_DESC_TRUNCATE = int(os.getenv("OLLAMA_DESC_TRUNCATE", "2000"))
        self.OLLAMA_MISSION_BATCH_SIZE = max(1, int(os.getenv("OLLAMA_MISSION_BATCH_SIZE", "8")))
        self.OLLAMA_MISSION_WORKERS = max(1, int(os.getenv("OLLAMA_MISSION_WORKERS", "2")))
        # Auto-approve curated employers at or above this mission_score (liberal default).
        self.MISSION_APPROVE_MIN_SCORE = max(0, min(100, int(os.getenv("MISSION_APPROVE_MIN_SCORE", "50"))))
        self.REGISTRY_LLM_FILTER = _env_bool("REGISTRY_LLM_FILTER", True)
        self.SCORE_MAX_PER_RUN = int(os.getenv("SCORE_MAX_PER_RUN", "0"))  # 0 = no cap
        self.SCORE_MAX_AGE_DAYS = int(os.getenv("SCORE_MAX_AGE_DAYS", "30"))  # 0 = no age filter
        # Digest cutoff; 0 = include all scored jobs. Set >0 to filter weak matches from email.
        self.MIN_COMBINED_SCORE = float(os.getenv("MIN_COMBINED_SCORE", "0"))
        self.DIGEST_TOP_N = int(os.getenv("DIGEST_TOP_N", "50"))
        self.DIGEST_REMOTE_ONLY = _env_bool("DIGEST_REMOTE_ONLY", True)
        self.SMTP_HOST = _env("SMTP_HOST", "smtp.gmail.com")
        self.SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        self.SMTP_USER = os.getenv("SMTP_USER") or ""
        self.SMTP_PASSWORD = os.getenv("SMTP_PASSWORD") or ""
        self.EMAIL_TO = os.getenv("EMAIL_TO") or ""
        self.SQLITE_PATH = (ROOT / os.getenv("SQLITE_PATH", "data/jobs.db")).resolve()
        self.PROFILE_PATH = (ROOT / os.getenv("PROFILE_PATH", "profile/profile.md")).resolve()
        self.PREFERENCES_PATH = (
            ROOT / os.getenv("PREFERENCES_PATH", "profile/preferences.yaml")
        ).resolve()
        self.INGEST_DELAY_MS = int(os.getenv("INGEST_DELAY_MS", "150"))
        self.INGEST_WORKERS = max(1, int(os.getenv("INGEST_WORKERS", "10")))
        self.SMARTRECRUITERS_API_KEY = os.getenv("SMARTRECRUITERS_API_KEY") or ""
        self.TARGET_ROLE_KEYWORDS = [
            k.strip().lower()
            for k in os.getenv(
                "TARGET_ROLE_KEYWORDS",
                "artificial intelligence engineer,analytics engineer,analytics engineering,"
                "ai engineer,machine learning engineer,ml engineer,data engineer,data engineering,"
                "data integration,data integrations,data platform engineer,etl engineer",
            ).split(",")
            if k.strip()
        ]
        self.EXCLUDE_TITLE_KEYWORDS = [
            k.strip().lower()
            for k in os.getenv(
                "EXCLUDE_TITLE_KEYWORDS",
                "intern,internship",
            ).split(",")
            if k.strip()
        ]
        self.FALLBACK_DIGEST_DIR = (ROOT / "data" / "digests").resolve()
        self.POLL_OVERRIDES_PATH = (ROOT / "data" / "poll_overrides.json").resolve()
        self.CURATED_COMPANIES_PATH = (
            ROOT / os.getenv("CURATED_COMPANIES_PATH", "registry/curated_companies.csv")
        ).resolve()
        self.BQ_ENABLED = _env_bool("BQ_ENABLED", True)
        self.BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "")
        self.BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "good_jobs_digest")
        self.BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")
        self.BQ_MERGE_ON_INGEST = _env_bool("BQ_MERGE_ON_INGEST", False)
        self.BQ_BATCH_NORMALIZED = _env_bool("BQ_BATCH_NORMALIZED", True)
        self.BQ_BATCH_LLM_SCORES = _env_bool("BQ_BATCH_LLM_SCORES", True)
        self.BQ_BATCH_CHUNK_SIZE = int(os.getenv("BQ_BATCH_CHUNK_SIZE", "50"))
        self.BQ_RAW_BATCH_SIZE = int(os.getenv("BQ_RAW_BATCH_SIZE", "50"))
        self.GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or ""
        # Mission job boards (Climatebase, 80k Hours, etc.)
        self.JOB_BOARDS_ENABLED = _env_bool("JOB_BOARDS_ENABLED", True)
        self.BOARD_INGEST_DELAY_MS = int(os.getenv("BOARD_INGEST_DELAY_MS", "2000"))
        self.BOARD_DETAIL_DELAY_MS = int(os.getenv("BOARD_DETAIL_DELAY_MS", "1500"))
        self.BOARD_PAUSE_BETWEEN_MS = int(os.getenv("BOARD_PAUSE_BETWEEN_MS", "3000"))
        self.WEBSHARE_PROXIES_PATH = (
            ROOT / os.getenv("WEBSHARE_PROXIES_PATH", "config/webshare_proxies.txt")
        ).resolve()
        self.WEBSHARE_PROXY_LIST_URL = os.getenv("WEBSHARE_PROXY_LIST_URL") or ""
        self.CLIMATEBASE_MAX_LISTINGS = int(os.getenv("CLIMATEBASE_MAX_LISTINGS", "100"))
        self.CLIMATEBASE_FETCH_DETAILS = _env_bool("CLIMATEBASE_FETCH_DETAILS", True)
        self.BOARD_80000HOURS_MAX_PAGES = int(os.getenv("BOARD_80000HOURS_MAX_PAGES", "3"))
        self.BOARD_ESCAPETHECITY_MAX_PAGES = int(os.getenv("BOARD_ESCAPETHECITY_MAX_PAGES", "3"))
        self.TJFG_FETCH_DETAILS = _env_bool("TJFG_FETCH_DETAILS", True)
        self.RELIEFWEB_ENABLED = _env_bool("RELIEFWEB_ENABLED", True)
        self.RELIEFWEB_APPNAME = (os.getenv("RELIEFWEB_APPNAME") or "").strip()
        self.RELIEFWEB_JOBS_LIMIT = int(os.getenv("RELIEFWEB_JOBS_LIMIT", "200"))

    def combined_weighted(self, role: float, mission: float, fit: float) -> float:
        return 0.4 * role + 0.35 * mission + 0.25 * fit

    def reliefweb_configured(self) -> bool:
        """True when ReliefWeb API should be called (valid appname + not disabled)."""
        if not self.RELIEFWEB_ENABLED:
            return False
        app = self.RELIEFWEB_APPNAME
        if not app:
            return False
        lowered = app.lower()
        if lowered.startswith("<") and app.endswith(">"):
            return False
        if "replace" in lowered or "approved appname" in lowered or "your_" in lowered:
            return False
        return True


settings = Settings()
