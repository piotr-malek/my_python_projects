#!/usr/bin/env python3
"""Download Webshare proxy list into config/webshare_proxies.txt (gitignored)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config import settings


def main() -> int:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default=settings.WEBSHARE_PROXY_LIST_URL or None,
        help="Webshare download URL (or set WEBSHARE_PROXY_LIST_URL in .env)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=settings.WEBSHARE_PROXIES_PATH,
        help="Output file path",
    )
    args = parser.parse_args()
    if not args.url:
        if args.out.is_file() and args.out.stat().st_size > 0:
            print(f"No WEBSHARE_PROXY_LIST_URL; keeping existing {args.out}")
            return 0
        print(
            "Set WEBSHARE_PROXY_LIST_URL in .env or pass --url",
            file=sys.stderr,
        )
        return 1
    out: Path = args.out
    out.parent.mkdir(parents=True, exist_ok=True)
    resp = httpx.get(args.url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    lines = [ln.strip() for ln in resp.text.splitlines() if ln.strip() and not ln.startswith("#")]
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(lines)} proxies to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
