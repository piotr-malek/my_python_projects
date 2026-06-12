"""Collect employer candidates from mission-oriented job boards and seed files."""

from __future__ import annotations

import csv
import json
import logging
import math
import os
import random
import re
import time
from pathlib import Path
from typing import Iterable

from discovery.resolve import EmployerCandidate, parse_ats_from_text
from pipelines.job_boards.sources.climatebase import JOBS_URL, fetch_job_detail, _parse_next_data
from pipelines.job_boards.sources.eighty_k_hours import fetch_jobs_paginated
from pipelines.job_boards.sources.http import browser_client, json_client, polite_sleep
from pipelines.job_boards.sources.escapethecity import (
    ALGOLIA_API_KEY,
    ALGOLIA_APP_ID,
    ALGOLIA_INDEX,
    ALGOLIA_URL,
    JOB_FILTER,
    _normalize as normalize_etc,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEEDS_PATH = ROOT / "discovery" / "seeds" / "mission_employers.csv"
B_CORP_HOST = "https://94eo8lmsqa0nd3j5p.a1.typesense.net"
B_CORP_COLLECTION = "companies-production-en-us"
B_CORP_MAX_PER_PAGE = 250
B_CORP_DEFAULT_RPS = 2.0
B_CORP_CHECKPOINT_PATH = ROOT / "data" / "bcorp_checkpoint.json"
B_CORP_JSONL_PATH = ROOT / "data" / "bcorp_companies.jsonl"
B_CORP_SOURCE_PAGE = "https://www.bcorporation.net/en-us/find-a-b-corp/"


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _merge(
    out: dict[str, EmployerCandidate],
    cand: EmployerCandidate,
) -> None:
    key = cand.company_name.strip().lower()
    if not key:
        return
    existing = out.get(key)
    if existing is None:
        out[key] = cand
        return
    if cand.ats_hint and not existing.ats_hint:
        existing.ats_hint = cand.ats_hint
    for slug in cand.extra_slugs:
        if slug not in existing.extra_slugs:
            existing.extra_slugs.append(slug)
    if cand.website and not existing.website:
        existing.website = cand.website
    if cand.discovery_source and cand.discovery_source not in (existing.discovery_source or ""):
        existing.discovery_source = f"{existing.discovery_source}+{cand.discovery_source}"


def collect_from_80000hours(*, max_pages: int = 50) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    jobs = fetch_jobs_paginated(max_pages=max_pages, hits_per_page=100)
    for job in jobs:
        name = (job.get("company_name") or "").strip()
        if not name:
            continue
        blob = " ".join(str(x) for x in (job.get("url"), job.get("company_url")))
        hint = parse_ats_from_text(blob)
        _merge(
            out,
            EmployerCandidate(
                company_name=name,
                mission_category="effective_altruism",
                website=str(job.get("company_url") or ""),
                discovery_source="80000hours",
                ats_hint=hint,
            ),
        )
    logger.info("80,000 Hours: %s employers", len(out))
    return out


def collect_from_escapethecity(*, max_pages: int = 12) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    with json_client() as client:
        headers = {
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "X-Algolia-API-Key": ALGOLIA_API_KEY,
            "Content-Type": "application/json",
        }
        for page in range(max_pages):
            params = f"hitsPerPage=100&page={page}&filters={JOB_FILTER}"
            resp = client.post(
                ALGOLIA_URL,
                headers=headers,
                content=json.dumps({"params": params}),
            )
            resp.raise_for_status()
            hits = resp.json().get("hits") or []
            if not hits:
                break
            for hit in hits:
                job = normalize_etc(hit)
                name = (job.get("company_name") or "").strip()
                if not name:
                    continue
                raw = job.get("raw") or {}
                blob = json.dumps(raw, default=str)
                hint = parse_ats_from_text(blob)
                _merge(
                    out,
                    EmployerCandidate(
                        company_name=name,
                        mission_category="impact",
                        discovery_source="escapethecity",
                        ats_hint=hint,
                    ),
                )
            polite_sleep(0.25)
    logger.info("Escape the City: %s employers", len(out))
    return out


def collect_from_climatebase(
    *,
    max_listings: int = 100,
    fetch_details: bool = True,
) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    with browser_client() as client:
        resp = client.get(JOBS_URL)
        resp.raise_for_status()
        payload = _parse_next_data(resp.text)
    rows = payload.get("props", {}).get("pageProps", {}).get("jobs") or []
    rows = rows[:max_listings]
    for listing in rows:
        name = (listing.get("name_of_employer") or "").strip()
        if not name:
            continue
        hint: tuple[str, str] | None = None
        if fetch_details and listing.get("id"):
            try:
                detail = fetch_job_detail(listing["id"])
                apply = str(detail.get("how_to_apply") or "")
                hint = parse_ats_from_text(apply)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Climatebase detail %s: %s", listing.get("id"), exc)
        sectors = listing.get("sectors")
        mission = sectors[0] if isinstance(sectors, list) and sectors else "climate"
        _merge(
            out,
            EmployerCandidate(
                company_name=name,
                mission_category=str(mission).lower().replace(" ", "_"),
                discovery_source="climatebase",
                ats_hint=hint,
            ),
        )
    logger.info("Climatebase: %s employers", len(out))
    return out


def collect_from_seeds(path: Path = DEFAULT_SEEDS_PATH) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    if not path.exists():
        logger.warning("Seed file missing: %s", path)
        return out
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            name = (raw.get("company_name") or "").strip()
            if not name:
                continue
            hint: tuple[str, str] | None = None
            ats_type = (raw.get("ats_type") or raw.get("slug_hint_ats") or "").strip().lower()
            slug = (raw.get("ats_slug") or raw.get("slug_hint") or "").strip()
            if ats_type and slug:
                hint = (ats_type, slug)
            _merge(
                out,
                EmployerCandidate(
                    company_name=name,
                    mission_category=(raw.get("mission_category") or "mission").strip(),
                    website=(raw.get("website") or "").strip(),
                    discovery_source="seeds",
                    ats_hint=hint,
                ),
            )
    logger.info("Seeds: %s employers from %s", len(out), path.name)
    return out


def _bcorp_headers() -> dict[str, str]:
    key = _env("BCORP_TYPESENSE_API_KEY")
    if not key:
        key = _discover_bcorp_typesense_key()
    if not key:
        raise RuntimeError("BCORP_TYPESENSE_API_KEY is not set")
    return {"X-TYPESENSE-API-KEY": key}


def _discover_bcorp_typesense_key() -> str:
    """
    Best-effort extraction from public frontend assets.
    Looks for a Typesense search key used by find-a-b-corp page.
    """
    with browser_client(timeout=30.0) as client:
        html = client.get(B_CORP_SOURCE_PAGE).text
    script_paths = re.findall(r'<script[^>]+src="([^"]+)"', html)
    # prioritize Next.js chunks where config is usually embedded
    candidates = [s for s in script_paths if "_next" in s] + script_paths
    checked = 0
    for src in candidates:
        if checked >= 40:
            break
        checked += 1
        if src.startswith("//"):
            url = "https:" + src
        elif src.startswith("/"):
            url = "https://www.bcorporation.net" + src
        elif src.startswith("http"):
            url = src
        else:
            continue
        try:
            with browser_client(timeout=20.0) as client:
                text = client.get(url).text
        except Exception:  # noqa: BLE001
            continue
        m = re.search(r'X-TYPESENSE-API-KEY["\']?\s*[:=]\s*["\']([A-Za-z0-9_\-\.]+)["\']', text)
        if not m:
            m = re.search(r'typesense[^"\']*key["\']?\s*[:=]\s*["\']([A-Za-z0-9_\-\.]+)["\']', text, re.I)
        if not m:
            m = re.search(r'apiKey["\']?\s*[:=]\s*["\']([A-Za-z0-9_\-\.]{16,})["\']', text)
        if m:
            logger.info("Discovered B Corp Typesense key from frontend bundle")
            return m.group(1)
    return ""


def _bcorp_search_page(*, page: int, per_page: int, headers: dict[str, str]) -> dict:
    url = f"{B_CORP_HOST}/collections/{B_CORP_COLLECTION}/documents/search"
    params = {"q": "*", "query_by": "name", "per_page": per_page, "page": page}
    with json_client(timeout=45.0) as client:
        resp = client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()


def _load_bcorp_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.warning("Invalid B Corp checkpoint JSON: %s", path)
        return {}


def _save_bcorp_checkpoint(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _bcorp_unique_id(doc: dict) -> str:
    return str(doc.get("id") or doc.get("slug") or "").strip().lower()


def collect_from_bcorp(
    *,
    per_page: int = B_CORP_MAX_PER_PAGE,
    max_pages: int = 0,
    requests_per_second: float = B_CORP_DEFAULT_RPS,
    checkpoint_path: Path = B_CORP_CHECKPOINT_PATH,
    output_jsonl_path: Path = B_CORP_JSONL_PATH,
    reset_checkpoint: bool = False,
) -> dict[str, EmployerCandidate]:
    """
    Fetch B Corp directory from Typesense API with retries/checkpoints.

    Uses BCORP_TYPESENSE_API_KEY from env.
    """
    if per_page > B_CORP_MAX_PER_PAGE:
        per_page = B_CORP_MAX_PER_PAGE
    delay = 1.0 / max(0.1, requests_per_second)
    if reset_checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()

    headers = _bcorp_headers()
    checkpoint = _load_bcorp_checkpoint(checkpoint_path)
    seen_ids: set[str] = set(checkpoint.get("seen_ids", []))
    last_completed_page = int(checkpoint.get("last_completed_page", 0) or 0)
    total_pages_checkpoint = int(checkpoint.get("total_pages", 0) or 0)

    first = _bcorp_search_page(page=1, per_page=per_page, headers=headers)
    found = int(first.get("found") or 0)
    total_pages = int(math.ceil(found / per_page)) if found else 0
    if max_pages > 0:
        total_pages = min(total_pages, max_pages)
    if total_pages_checkpoint and total_pages_checkpoint != total_pages:
        logger.info(
            "B Corp found/page drift detected: old total_pages=%s new=%s",
            total_pages_checkpoint,
            total_pages,
        )

    out: dict[str, EmployerCandidate] = {}
    output_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if last_completed_page > 0 and output_jsonl_path.exists() else "w"
    with output_jsonl_path.open(mode, encoding="utf-8") as out_f:
        start_page = max(1, last_completed_page + 1)
        for page in range(start_page, total_pages + 1):
            data = None
            for attempt in range(3):
                try:
                    data = _bcorp_search_page(page=page, per_page=per_page, headers=headers)
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt >= 2:
                        raise
                    backoff = (2**attempt) + random.random()
                    logger.warning("B Corp page %s retry %s: %s", page, attempt + 1, exc)
                    time.sleep(backoff)
            hits = (data or {}).get("hits") or []
            for hit in hits:
                doc = hit.get("document") or {}
                uid = _bcorp_unique_id(doc)
                if not uid or uid in seen_ids:
                    continue
                seen_ids.add(uid)
                out_f.write(json.dumps(doc, ensure_ascii=True) + "\n")
                name = str(doc.get("name") or "").strip()
                if not name:
                    continue
                website = str(doc.get("website") or "")
                _merge(
                    out,
                    EmployerCandidate(
                        company_name=name,
                        mission_category="bcorp",
                        website=website,
                        discovery_source="bcorp",
                    ),
                )
            _save_bcorp_checkpoint(
                checkpoint_path,
                {
                    "updated_at": time.time(),
                    "last_completed_page": page,
                    "total_pages": total_pages,
                    "found": found,
                    "seen_ids": sorted(seen_ids),
                },
            )
            if page % 5 == 0 or page == total_pages:
                logger.info("B Corp pages %s/%s; candidates=%s", page, total_pages, len(out))
            time.sleep(delay)
    logger.info("B Corp: %s employers (found=%s)", len(out), found)
    return out


def collect_all(
    *,
    sources: Iterable[str],
    climatebase_max_listings: int = 100,
    climatebase_fetch_details: bool = True,
    eighty_k_max_pages: int = 50,
    escapethecity_max_pages: int = 12,
    seeds_path: Path = DEFAULT_SEEDS_PATH,
    bcorp_max_pages: int = 0,
    bcorp_per_page: int = B_CORP_MAX_PER_PAGE,
    bcorp_requests_per_second: float = B_CORP_DEFAULT_RPS,
    bcorp_reset_checkpoint: bool = False,
) -> list[EmployerCandidate]:
    merged: dict[str, EmployerCandidate] = {}
    wanted = {s.strip().lower() for s in sources}

    if "80000hours" in wanted:
        for k, v in collect_from_80000hours(max_pages=eighty_k_max_pages).items():
            merged[k] = v
    if "escapethecity" in wanted:
        for k, v in collect_from_escapethecity(max_pages=escapethecity_max_pages).items():
            _merge(merged, v)
    if "climatebase" in wanted:
        for k, v in collect_from_climatebase(
            max_listings=climatebase_max_listings,
            fetch_details=climatebase_fetch_details,
        ).items():
            _merge(merged, v)
    if "seeds" in wanted:
        for k, v in collect_from_seeds(seeds_path).items():
            _merge(merged, v)
    if "bcorp" in wanted:
        try:
            for k, v in collect_from_bcorp(
                max_pages=bcorp_max_pages,
                per_page=bcorp_per_page,
                requests_per_second=bcorp_requests_per_second,
                reset_checkpoint=bcorp_reset_checkpoint,
            ).items():
                _merge(merged, v)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping B Corp source: %s", exc)

    return sorted(merged.values(), key=lambda c: c.company_name.lower())
