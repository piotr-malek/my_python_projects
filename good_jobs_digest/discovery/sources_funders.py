"""Funder portfolio org lists for employer candidate discovery."""

from __future__ import annotations

import csv
import io
import logging
import re
from typing import Any, Iterable

import httpx

from discovery.resolve import EmployerCandidate

logger = logging.getLogger(__name__)

# Open Philanthropy rebranded to Coefficient Giving; old export URL redirects away from CSV.
COEFFICIENT_GRANTS_URL = (
    "https://coefficientgiving.org/wp-content/uploads/Coefficient-Giving-Grants-Archive.csv"
)
FAST_FORWARD_DATA_URL = "https://www.ffwd.org/directory/__data.json"
ECHOING_GREEN_URL = "https://www.echoinggreen.org/fellows"
ASHOKA_FELLOWS_URL = "https://www.ashoka.org/en-us/ashoka-fellows"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def _http_get(url: str, *, timeout: float = 60.0, params: dict[str, Any] | None = None) -> httpx.Response:
    with httpx.Client(headers=_BROWSER_HEADERS, timeout=timeout, follow_redirects=True) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp


def _deref_sveltekit_node(data: dict[str, Any], *, slot: int = 1) -> object:
    arr = data["nodes"][slot]["data"]
    visiting: set[int] = set()

    def resolve(value: object) -> object:
        if isinstance(value, int):
            if value in visiting or value < 0 or value >= len(arr):
                return None
            visiting.add(value)
            try:
                return resolve(arr[value])
            finally:
                visiting.discard(value)
        if isinstance(value, list):
            return [resolve(item) for item in value]
        if isinstance(value, dict):
            return {key: resolve(val) for key, val in value.items()}
        return value

    return resolve(0)


def collect_from_openphil() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    try:
        resp = _http_get(COEFFICIENT_GRANTS_URL, timeout=120.0)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Coefficient Giving / Open Phil grants fetch failed: %s", exc)
        return out
    reader = csv.DictReader(io.StringIO(resp.text))
    for row in reader:
        name = (
            row.get("Organization Name")
            or row.get("Grantee")
            or row.get("grantee")
            or row.get("Organization")
            or ""
        ).strip()
        if not name or len(name) < 3:
            continue
        key = name.lower()
        if key in out:
            continue
        out[key] = EmployerCandidate(
            company_name=name,
            mission_category="effective_altruism",
            discovery_source="openphil",
        )
    logger.info("Open Philanthropy (Coefficient Giving): %s employers", len(out))
    return out


def _scrape_org_names_from_html(html: str, *, min_len: int = 3) -> list[str]:
    """Best-effort org name extraction from listing pages."""
    names: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(r'<h[23][^>]*>([^<]{3,120})</h[23]>', html, re.I):
        name = re.sub(r"\s+", " ", m.group(1)).strip()
        key = name.lower()
        if len(name) >= min_len and key not in seen and not name.startswith("http"):
            seen.add(key)
            names.append(name)
    for m in re.finditer(r'data-org(?:anization)?-name="([^"]+)"', html, re.I):
        name = m.group(1).strip()
        key = name.lower()
        if name and key not in seen:
            seen.add(key)
            names.append(name)
    return names


def collect_from_fast_forward() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    try:
        page = 1
        while page <= 200:
            params = {"page": page} if page > 1 else None
            resp = _http_get(FAST_FORWARD_DATA_URL, timeout=120.0, params=params)
            root = _deref_sveltekit_node(resp.json())
            if not isinstance(root, dict):
                break
            companies = root.get("companies")
            if not isinstance(companies, dict):
                break
            results = companies.get("results")
            if not isinstance(results, list) or not results:
                break
            for row in results:
                if not isinstance(row, dict):
                    continue
                props = row.get("properties")
                if not isinstance(props, dict):
                    continue
                name = str(props.get("name") or "").strip()
                website = str(props.get("website") or "").strip()
                if not name or len(name) < 2:
                    continue
                out[name.lower()] = EmployerCandidate(
                    company_name=name,
                    mission_category="tech_nonprofit",
                    website=website,
                    discovery_source="fast_forward",
                )
            page += 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("Fast Forward fetch failed: %s", exc)
    logger.info("Fast Forward: %s employers", len(out))
    return out


def collect_from_echoing_green() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    try:
        resp = _http_get(ECHOING_GREEN_URL)
        names = _scrape_org_names_from_html(resp.text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Echoing Green fetch failed: %s", exc)
        return out
    for name in names:
        out[name.lower()] = EmployerCandidate(
            company_name=name,
            mission_category="social_entrepreneurship",
            discovery_source="echoing_green",
        )
    logger.info("Echoing Green: %s employers", len(out))
    return out


def collect_from_ashoka_fellows() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    try:
        resp = _http_get(ASHOKA_FELLOWS_URL)
        names = _scrape_org_names_from_html(resp.text)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ashoka fellows fetch failed: %s", exc)
        return out
    for name in names:
        out[name.lower()] = EmployerCandidate(
            company_name=name,
            mission_category="social_entrepreneurship",
            discovery_source="ashoka_fellows",
        )
    logger.info("Ashoka fellows: %s employers", len(out))
    return out


def collect_funder_sources(sources: Iterable[str]) -> list[EmployerCandidate]:
    wanted = {s.strip().lower() for s in sources}
    merged: dict[str, EmployerCandidate] = {}
    collectors = {
        "openphil": collect_from_openphil,
        "fast_forward": collect_from_fast_forward,
        "echoing_green": collect_from_echoing_green,
        "ashoka_fellows": collect_from_ashoka_fellows,
    }
    for key, fn in collectors.items():
        if key in wanted:
            merged.update(fn())
    return sorted(merged.values(), key=lambda c: c.company_name.lower())
