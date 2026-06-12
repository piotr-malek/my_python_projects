import json
import time

import requests

STREAM_KEYS = (
    "time,heartrate,watts,cadence,distance,velocity_smooth,grade_smooth,altitude,moving"
)
BASE = "https://www.strava.com/api/v3"


class StravaClient:
    _instance = None

    def __init__(self, settings):
        self._settings = settings
        self._access_token = None
        self._refresh_token = None
        self._load_tokens()

    @classmethod
    def get(cls, settings):
        if cls._instance is None:
            cls._instance = cls(settings)
        return cls._instance

    def _load_tokens(self):
        path = self._settings.STRAVA_TOKEN_PATH
        if path.is_file():
            cached = json.loads(path.read_text())
            self._access_token = cached.get("access_token")
            self._refresh_token = cached.get("refresh_token")
        if not self._refresh_token:
            self._refresh_token = self._settings.STRAVA_REFRESH_TOKEN
        if not self._access_token:
            self.refresh_access_token()
        if not self._access_token:
            raise RuntimeError("Strava access token unavailable")

    def _save_tokens(self, data):
        path = self._settings.STRAVA_TOKEN_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2))
        path.chmod(0o600)
        self._access_token = data.get("access_token")
        if data.get("refresh_token"):
            self._refresh_token = data["refresh_token"]

    def refresh_access_token(self):
        payload = {
            "client_id": self._settings.STRAVA_CLIENT_ID,
            "client_secret": self._settings.STRAVA_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }
        r = requests.post("https://www.strava.com/oauth/token", data=payload, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Strava token refresh failed: {r.status_code} {r.text}")
        data = r.json()
        self._save_tokens(data)
        return data["access_token"]

    def _headers(self):
        if not self._access_token:
            self.refresh_access_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    def _request(self, method, path, params=None, retried=False):
        url = f"{BASE}{path}"
        r = requests.request(method, url, headers=self._headers(), params=params, timeout=60)
        if r.status_code == 401 and not retried:
            self.refresh_access_token()
            return self._request(method, path, params, retried=True)
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 900))
            time.sleep(min(retry_after, 900))
            return self._request(method, path, params, retried=retried)
        if r.status_code >= 400:
            raise RuntimeError(f"Strava API {r.status_code}: {r.text[:500]}")
        self._maybe_throttle(r)
        return r.json() if r.text else None

    @staticmethod
    def _maybe_throttle(response):
        usage = response.headers.get("X-ReadRateLimit-Usage") or response.headers.get(
            "X-RateLimit-Usage"
        )
        limit = response.headers.get("X-ReadRateLimit-Limit") or response.headers.get(
            "X-RateLimit-Limit"
        )
        if usage and limit:
            u15, _ = [int(x) for x in usage.split(",")]
            l15, _ = [int(x) for x in limit.split(",")]
            if l15 and u15 >= l15 * 0.85:
                time.sleep(30)

    def list_activities(self, after=None, page=1):
        params = {"per_page": 200, "page": page}
        if after is not None:
            params["after"] = after
        return self._request("GET", "/athlete/activities", params) or []

    def get_activity(self, activity_id):
        return self._request("GET", f"/activities/{activity_id}") or {}

    def get_streams(self, activity_id):
        params = {"keys": STREAM_KEYS, "key_by_type": "true"}
        return self._request("GET", f"/activities/{activity_id}/streams", params) or []
