import os

# Minimal env so tests can import config/storage without a real .env file.
_TEST_ENV = {
    "BQ_PROJECT_ID": "test-project",
    "BQ_DATASET_ID": "health_monitoring",
    "BQ_LOCATION": "US",
    "GOOGLE_APPLICATION_CREDENTIALS": "config/service_account.json.example",
    "GARMIN_EMAIL": "test@example.com",
    "GARMIN_PASSWORD": "test-password",
    "GARMINTOKENS": "~/.garminconnect-test",
    "STRAVA_CLIENT_ID": "0",
    "STRAVA_CLIENT_SECRET": "test-secret",
    "STRAVA_REFRESH_TOKEN": "test-refresh-token",
    "STRAVA_TOKEN_PATH": "~/.health_monitoring_test/strava_tokens.json",
    "STRAVA_LOOKBACK_DAYS": "7",
    "ANALYSIS_DAYS": "90",
    "OLLAMA_HOST": "http://localhost:11434",
    "OLLAMA_MODEL": "qwen3:14b-q4_K_M",
    "OLLAMA_NUM_PREDICT": "1024",
    "SMTP_HOST": "smtp.example.com",
    "SMTP_PORT": "587",
    "SMTP_USER": "test@example.com",
    "SMTP_PASSWORD": "test-smtp-password",
    "EMAIL_TO": "test@example.com",
    "DIGEST_HOUR": "7",
    "DIGEST_MINUTE": "30",
    "FTP_WATTS": "225",
    "THRESHOLD_HR": "170",
    "LOCAL_STATE_DIR": "~/.health_monitoring_test",
}

for key, value in _TEST_ENV.items():
    os.environ.setdefault(key, value)
