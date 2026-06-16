"""Reverse geocoding and nearby-place lookup for region display-name research."""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any, Optional

USER_AGENT = "disaster-predictor-region-display-names/1.0 (research; contact: local-dev)"
NOMINATIM_BASE = "https://nominatim.openstreetmap.org"
_MIN_INTERVAL_SEC = 1.1
_last_request_at = 0.0


def _throttled_get(url: str, timeout: int = 30) -> dict | list:
    global _last_request_at
    elapsed = time.time() - _last_request_at
    if elapsed < _MIN_INTERVAL_SEC:
        time.sleep(_MIN_INTERVAL_SEC - elapsed)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8")
    _last_request_at = time.time()
    return json.loads(data)


def reverse_geocode(lat: float, lon: float, zoom: int = 10) -> dict[str, Any]:
    """Return Nominatim reverse-geocode payload for a point."""
    params = urllib.parse.urlencode(
        {
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "format": "jsonv2",
            "addressdetails": 1,
            "extratags": 1,
            "namedetails": 1,
            "zoom": zoom,
        }
    )
    url = f"{NOMINATIM_BASE}/reverse?{params}"
    try:
        payload = _throttled_get(url)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"error": str(exc)}


def _address_bits(address: dict[str, Any]) -> list[str]:
    keys = (
        "city",
        "town",
        "village",
        "hamlet",
        "municipality",
        "county",
        "state_district",
        "state",
        "region",
        "country",
        "national_park",
        "natural",
        "isolated_dwelling",
    )
    out: list[str] = []
    for k in keys:
        v = address.get(k)
        if v and v not in out:
            out.append(str(v))
    return out


def summarize_reverse(payload: dict[str, Any]) -> dict[str, Any]:
    """Compact summary useful for naming."""
    if not payload or payload.get("error"):
        return {"error": payload.get("error", "empty")}
    address = payload.get("address") or {}
    return {
        "display_name": payload.get("display_name"),
        "name": payload.get("name"),
        "type": payload.get("type"),
        "category": payload.get("category"),
        "addresstags": _address_bits(address),
        "namedetails": payload.get("namedetails") or {},
        "extratags": {
            k: v
            for k, v in (payload.get("extratags") or {}).items()
            if k in {"wikipedia", "wikidata", "website", "official_name"}
        },
    }


def search_places_near(
    lat: float,
    lon: float,
    radius_km: float = 80.0,
    limit: int = 8,
) -> list[dict[str, Any]]:
    """
    Find populated places / landmarks near a point using Nominatim search.
    Uses a viewbox derived from radius.
    """
    # ~1 deg lat ≈ 111 km
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * max(0.2, abs(__import__("math").cos(__import__("math").radians(lat)))))
    viewbox = f"{lon - dlon},{lat + dlat},{lon + dlon},{lat - dlat}"
    params = urllib.parse.urlencode(
        {
            "q": "town",
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": limit,
            "viewbox": viewbox,
            "bounded": 0,
        }
    )
    url = f"{NOMINATIM_BASE}/search?{params}"
    try:
        results = _throttled_get(url)
        if not isinstance(results, list):
            return []
        compact = []
        for r in results:
            addr = r.get("address") or {}
            place = (
                addr.get("city")
                or addr.get("town")
                or addr.get("village")
                or addr.get("hamlet")
                or r.get("name")
            )
            if not place:
                continue
            compact.append(
                {
                    "name": place,
                    "type": r.get("type"),
                    "category": r.get("category"),
                    "distance_hint_km": None,
                    "display_name": r.get("display_name"),
                }
            )
        return compact[:limit]
    except Exception:
        return []


def research_region(
    region: str,
    parent_region: str,
    country: str,
    centroid_lat: float,
    centroid_lon: float,
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
    area_km2: Optional[float] = None,
) -> dict[str, Any]:
    """Gather geocoding context for one monitoring subregion."""
    centroid = summarize_reverse(reverse_geocode(centroid_lat, centroid_lon, zoom=10))
    # Corner samples help capture large parks / elongated regions
    corners = {
        "sw": summarize_reverse(reverse_geocode(lat_min, lon_min, zoom=8)),
        "ne": summarize_reverse(reverse_geocode(lat_max, lon_max, zoom=8)),
    }
    nearby = search_places_near(centroid_lat, centroid_lon, radius_km=min(120, max(40, (area_km2 or 10000) ** 0.5 * 2)))

    return {
        "region": region,
        "parent_region": parent_region,
        "country": country,
        "centroid_lat": centroid_lat,
        "centroid_lon": centroid_lon,
        "bbox": [lon_min, lat_min, lon_max, lat_max],
        "area_km2": area_km2,
        "centroid_geocode": centroid,
        "corner_geocode": corners,
        "nearby_places": nearby,
    }
