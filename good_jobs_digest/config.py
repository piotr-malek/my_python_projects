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
        # Gemini Flash (AI Studio). Use a key from a project with billing DISABLED:
        # over-quota calls then return 429 and can never be charged.
        self.GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
        self.GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")
        # Tried in order if GEMINI_MODEL is retired (Google 404s old ids without notice).
        self.GEMINI_MODEL_FALLBACKS = tuple(
            m.strip()
            for m in os.getenv(
                "GEMINI_MODEL_FALLBACKS", "gemini-flash-lite-latest,gemini-2.5-flash"
            ).split(",")
            if m.strip()
        )
        self.GEMINI_MAX_OUTPUT_TOKENS = int(os.getenv("GEMINI_MAX_OUTPUT_TOKENS", "4096"))
        self.GEMINI_MAX_RETRIES = max(1, int(os.getenv("GEMINI_MAX_RETRIES", "3")))
        # Free-tier guardrails: requests/minute and a persisted requests/day cap.
        self.GEMINI_RPM = int(os.getenv("GEMINI_RPM", "8"))
        self.GEMINI_DAILY_REQUEST_BUDGET = int(os.getenv("GEMINI_DAILY_REQUEST_BUDGET", "300"))
        self.GEMINI_USAGE_PATH = (ROOT / "data" / "gemini_usage.json").resolve()
        # Batching keeps the request count (and therefore the free-tier usage) low.
        self.LLM_SCORE_WORKERS = max(1, int(os.getenv("LLM_SCORE_WORKERS", "2")))
        self.LLM_SCORE_BATCH_SIZE = max(1, int(os.getenv("LLM_SCORE_BATCH_SIZE", "8")))
        self.LLM_DESC_TRUNCATE = int(os.getenv("LLM_DESC_TRUNCATE", "2000"))
        self.LLM_MISSION_BATCH_SIZE = max(1, int(os.getenv("LLM_MISSION_BATCH_SIZE", "20")))
        self.LLM_MISSION_WORKERS = max(1, int(os.getenv("LLM_MISSION_WORKERS", "2")))
        # Auto-approve curated employers at or above this mission_score (liberal default).
        self.MISSION_APPROVE_MIN_SCORE = max(0, min(100, int(os.getenv("MISSION_APPROVE_MIN_SCORE", "50"))))
        self.REGISTRY_LLM_FILTER = _env_bool("REGISTRY_LLM_FILTER", True)
        self.SCORE_MAX_PER_RUN = int(os.getenv("SCORE_MAX_PER_RUN", "0"))  # 0 = no cap
        self.SCORE_MAX_AGE_DAYS = int(os.getenv("SCORE_MAX_AGE_DAYS", "30"))  # 0 = no age filter
        # Digest cutoff; 0 = include all scored jobs. Set >0 to filter weak matches from email.
        self.MIN_COMBINED_SCORE = float(os.getenv("MIN_COMBINED_SCORE", "0"))
        # Digest floor on the LLM candidate_fit score (fit_score column); 0 = disabled.
        self.MIN_CANDIDATE_FIT = float(os.getenv("MIN_CANDIDATE_FIT", "40"))
        # Max jobs per digest section (curated / boards); 0 = unlimited.
        self.DIGEST_TOP_N = int(os.getenv("DIGEST_TOP_N", "50"))
        self.DIGEST_REMOTE_ONLY = _env_bool("DIGEST_REMOTE_ONLY", True)
        self.SMTP_HOST = _env("SMTP_HOST", "smtp.gmail.com")
        self.SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        self.SMTP_TIMEOUT_SECONDS = float(os.getenv("SMTP_TIMEOUT_SECONDS", "30"))
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
        # Poll 1/N of curated employers per ingest (stalest first). 1 = poll all every run.
        self.CURATED_POLL_ROTATION_DIVISOR = max(
            1, int(os.getenv("CURATED_POLL_ROTATION_DIVISOR", "1"))
        )
        self.SMARTRECRUITERS_API_KEY = os.getenv("SMARTRECRUITERS_API_KEY") or ""
        self.TARGET_ROLE_KEYWORDS = [
            k.strip().lower()
            for k in os.getenv(
                "TARGET_ROLE_KEYWORDS",
                "artificial intelligence engineer,analytics engineer,analytics engineering,"
                "ai engineer,data engineer,data engineering,"
                "data integration,data integrations,data platform engineer,etl engineer",
            ).split(",")
            if k.strip()
        ]
        self.EXCLUDE_TITLE_KEYWORDS = [
            k.strip().lower()
            for k in os.getenv(
                "EXCLUDE_TITLE_KEYWORDS",
                "intern,internship,software engineer,software developer,"
                "frontend engineer,front-end engineer,backend engineer,back-end engineer,"
                "full stack,fullstack,devops,site reliability,sre,"
                "mobile engineer,ios engineer,android engineer,qa engineer,quality engineer,"
                "security engineer,sales engineer,product engineer,"
                "mechanical engineer,civil engineer,field engineer,manufacturing engineer,"
                "customer success,account executive,recruiter,marketing manager,"
                "machine learning,ml engineer,mlops",
            ).split(",")
            if k.strip()
        ]
        # Title-level seniority gate (word-boundary match). Drops levels outside the
        # target (see profile/preferences.yaml seniority); empty string disables.
        self.SENIORITY_EXCLUDE_KEYWORDS = [
            k.strip().lower()
            for k in os.getenv(
                "SENIORITY_EXCLUDE_KEYWORDS",
                "senior,staff,principal,lead,head,director,vp,chief,manager,"
                "graduate,working student,trainee",
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
        # The BigQuery client waits forever by default; a dropped socket would hang
        # an unattended run. 0 disables the timeout.
        self.BQ_JOB_TIMEOUT_SECONDS = float(os.getenv("BQ_JOB_TIMEOUT_SECONDS", "120"))
        # Daily pipeline: read curated_companies + batch-load jobs_normalized (no streaming inserts).
        self.BQ_WRITE_JOBS = _env_bool("BQ_WRITE_JOBS", True)
        self.BQ_WRITE_RAW_PAYLOADS = _env_bool("BQ_WRITE_RAW_PAYLOADS", False)
        self.BQ_WRITE_LLM_SCORES = _env_bool("BQ_WRITE_LLM_SCORES", False)
        self.BQ_WRITE_DIGEST_HISTORY = _env_bool("BQ_WRITE_DIGEST_HISTORY", False)
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
        # Preferred over the download URL: the API reports which proxies are still
        # valid, and the key isn't embedded in a shareable URL.
        self.WEBSHARE_API_KEY = os.getenv("WEBSHARE_API_KEY") or ""
        # Free Webshare proxies get rotated without notice, so the cached list is
        # re-downloaded once it is older than this. 0 = refresh on every run.
        self.WEBSHARE_PROXY_MAX_AGE_HOURS = float(
            os.getenv("WEBSHARE_PROXY_MAX_AGE_HOURS", "12")
        )
        self.CLIMATEBASE_MAX_LISTINGS = int(os.getenv("CLIMATEBASE_MAX_LISTINGS", "100"))
        self.CLIMATEBASE_FETCH_DETAILS = _env_bool("CLIMATEBASE_FETCH_DETAILS", True)
        self.BOARD_80000HOURS_MAX_PAGES = int(os.getenv("BOARD_80000HOURS_MAX_PAGES", "3"))
        self.BOARD_ESCAPETHECITY_MAX_PAGES = int(os.getenv("BOARD_ESCAPETHECITY_MAX_PAGES", "3"))
        self.TJFG_FETCH_DETAILS = _env_bool("TJFG_FETCH_DETAILS", True)
        # ReliefWeb needs an approved appname we never obtained — off unless re-enabled.
        self.RELIEFWEB_ENABLED = _env_bool("RELIEFWEB_ENABLED", False)
        self.RELIEFWEB_APPNAME = (os.getenv("RELIEFWEB_APPNAME") or "").strip()
        self.RELIEFWEB_JOBS_LIMIT = int(os.getenv("RELIEFWEB_JOBS_LIMIT", "200"))
        # Zero-yield HTML scrapers — off by default (Phase 3 board repairs).
        self.BOARD_IDEALIST_ENABLED = _env_bool("BOARD_IDEALIST_ENABLED", False)
        self.BOARD_IMPACTPOOL_ENABLED = _env_bool("BOARD_IMPACTPOOL_ENABLED", False)
        self.BOARD_AAC_ENABLED = _env_bool("BOARD_AAC_ENABLED", False)
        self.BOARD_WORKONCLIMATE_ENABLED = _env_bool("BOARD_WORKONCLIMATE_ENABLED", False)
        # New API boards (Phase 3).
        self.BOARD_REMOTIVE_ENABLED = _env_bool("BOARD_REMOTIVE_ENABLED", True)
        self.BOARD_ARBEITNOW_ENABLED = _env_bool("BOARD_ARBEITNOW_ENABLED", True)
        self.BOARD_JOBICY_ENABLED = _env_bool("BOARD_JOBICY_ENABLED", True)
        self.BOARD_HIMALAYAS_ENABLED = _env_bool("BOARD_HIMALAYAS_ENABLED", True)
        self.BOARD_ARBEITNOW_MAX_PAGES = int(os.getenv("BOARD_ARBEITNOW_MAX_PAGES", "5"))
        self.BOARD_JOBICY_COUNT = int(os.getenv("BOARD_JOBICY_COUNT", "50"))
        self.BOARD_JOBICY_GEO = os.getenv("BOARD_JOBICY_GEO", "emea")
        self.BOARD_HIMALAYAS_LIMIT = int(os.getenv("BOARD_HIMALAYAS_LIMIT", "200"))
        self.BOARD_REMOTEOK_ENABLED = _env_bool("BOARD_REMOTEOK_ENABLED", True)
        self.BOARD_WEWORKREMOTELY_ENABLED = _env_bool("BOARD_WEWORKREMOTELY_ENABLED", True)
        self.BOARD_WORKINGNOMADS_ENABLED = _env_bool("BOARD_WORKINGNOMADS_ENABLED", True)
        self.BOARD_HN_ENABLED = _env_bool("BOARD_HN_ENABLED", True)
        self.BOARD_HN_MAX_COMMENTS = int(os.getenv("BOARD_HN_MAX_COMMENTS", "400"))
        # Indeed via python-jobspy (optional dependency: pip install python-jobspy)
        self.BOARD_INDEED_ENABLED = _env_bool("BOARD_INDEED_ENABLED", True)
        self.INDEED_COUNTRIES = tuple(
            c.strip()
            for c in os.getenv(
                "INDEED_COUNTRIES", "Germany,Netherlands,Poland,Ireland,Spain"
            ).split(",")
            if c.strip()
        )
        self.INDEED_SEARCH_TERMS = tuple(
            t.strip()
            for t in os.getenv(
                "INDEED_SEARCH_TERMS",
                "data engineer,analytics engineer,data platform engineer",
            ).split(",")
            if t.strip()
        )
        self.INDEED_RESULTS_WANTED = int(os.getenv("INDEED_RESULTS_WANTED", "25"))
        # Employer mission gate for board-sourced jobs (cached per employer).
        self.EMPLOYER_MISSION_GATE_ENABLED = _env_bool("EMPLOYER_MISSION_GATE_ENABLED", True)

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
