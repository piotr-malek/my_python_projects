import json
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402

REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "activity:read_all,read,profile:read_all"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        code = qs.get("code", [None])[0]
        if code:
            self.server.auth_code = code
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authorization complete. You can close this tab.")
        else:
            self.send_response(400)
            self.end_headers()

    def log_message(self, *_):
        pass


def main():
    url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={settings.STRAVA_CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&approval_prompt=force"
        f"&scope={SCOPES}"
    )
    print("Authorize at:\n", url)
    webbrowser.open(url)
    server = HTTPServer(("localhost", 8080), Handler)
    server.handle_request()
    code = getattr(server, "auth_code", None)
    if not code:
        raise SystemExit("No authorization code received")

    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": settings.STRAVA_CLIENT_ID,
            "client_secret": settings.STRAVA_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    settings.STRAVA_TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    settings.STRAVA_TOKEN_PATH.write_text(json.dumps(data, indent=2))
    settings.STRAVA_TOKEN_PATH.chmod(0o600)
    print(f"Tokens saved to {settings.STRAVA_TOKEN_PATH}")
    print(f"scope: {data.get('scope')}")
    print(f"Add to .env: STRAVA_REFRESH_TOKEN={data.get('refresh_token')}")


if __name__ == "__main__":
    main()
