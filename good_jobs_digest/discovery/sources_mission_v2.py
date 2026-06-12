"""Mission-purity org discovery from grant registries and evaluator lists (v2 pipeline)."""

from __future__ import annotations

import csv
import html as html_module
import io
import json
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable, Iterable

import httpx

from discovery.resolve import EmployerCandidate
from discovery.sources import _merge

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
MISSION_V2_CANDIDATES_PATH = ROOT / "data" / "mission_org_candidates_v2.jsonl"
MISSION_V2_SCRAPE_CHECKPOINT_PATH = ROOT / "data" / "mission_org_scrape_progress_v2.json"

COEFFICIENT_CSV_URL = (
    "https://coefficientgiving.org/wp-content/uploads/Coefficient-Giving-Grants-Archive.csv"
)
GATES_CSV_URL = (
    "https://www.gatesfoundation.org/-/media/files/bmgf-grants.csv"
    "?rev=fd8381a8d89f4f23af85dbb2656faad2"
)
SFF_URL = "https://survivalandflourishing.fund/recommendations"
GWWC_URL = "https://www.givingwhatwecan.org/best-charities-to-donate-to-2026"
ACE_URL = "https://animalcharityevaluators.org/recommended-charities/"
GIVEWELL_URL = "https://www.givewell.org/charities/top-charities"
EA_FUNDS_SITEMAP_URL = "https://funds.effectivealtruism.org/sitemap.xml"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,text/csv,*/*",
}

_GIVEWELL_SKIP_HEADINGS = frozenset(
    {
        "search form",
        "main menu",
        "footer menu",
        "follow us:",
        "subscribe to email updates:",
    }
)

_UNIVERSITY_RE = re.compile(
    r"\b(university|college|institute of technology|school of medicine|"
    r"school of public health)\b",
    re.I,
)

_HIGH_SIGNAL_FOCUS_AREAS = frozenset(
    {
        "Navigating Transformative AI",
        "Biosecurity & Pandemic Preparedness",
        "Farm Animal Welfare",
        "Global Catastrophic Risks",
        "Global Catastrophic Risks Capacity Building",
        "Global Health & Development",
        "Human Health and Wellbeing",
        "Animal Welfare",
        "Broiler Chicken Welfare",
        "Cage-Free Reforms",
        "Global Aid Policy",
        "Scientific Research",
    }
)

_FOCUS_AREA_CATEGORY: dict[str, str] = {
    "Navigating Transformative AI": "ai_safety",
    "Biosecurity & Pandemic Preparedness": "biosecurity",
    "Farm Animal Welfare": "animal_welfare",
    "Broiler Chicken Welfare": "animal_welfare",
    "Cage-Free Reforms": "animal_welfare",
    "Animal Welfare": "animal_welfare",
    "Global Catastrophic Risks": "xrisk",
    "Global Catastrophic Risks Capacity Building": "xrisk",
    "Global Health & Development": "global_health",
    "Human Health and Wellbeing": "global_health",
    "Global Aid Policy": "global_health",
    "Scientific Research": "research",
    "Criminal Justice Reform": "policy",
    "Immigration Policy": "policy",
}

_GATES_TOPIC_ALLOWLIST = frozenset(
    {
        "HIV",
        "Malaria",
        "Tuberculosis",
        "Vaccine Development",
        "Enteric Diseases and Diarrhea",
        "Discovery and Translational Sciences",
        "Polio",
        "Neglected Tropical Diseases",
        "Family Planning",
        "Pneumonia",
        "Child Health",
        "Maternal and Child Health",
        "Nutrition",
        "Vaccine Delivery",
        "Integrated Delivery",
        "Health Systems Strengthening",
    }
)

_MISSION_V2_SOURCES = (
    "coefficient",
    "sff",
    "gwwc",
    "ace",
    "givewell",
    "gates",
    "ea_funds",
)


def _http_get(url: str, *, timeout: float = 60.0) -> httpx.Response:
    with httpx.Client(
        headers=_BROWSER_HEADERS,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp


def _category_from_focus_area(area: str) -> str:
    return _FOCUS_AREA_CATEGORY.get(area.strip(), "mission")


def _extract_next_data_charities(page_html: str) -> list[dict]:
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', page_html, re.S)
    if not m:
        return []
    payload = json.loads(m.group(1))
    charities = payload.get("props", {}).get("pageProps", {}).get("charities")
    return charities if isinstance(charities, list) else []


def collect_from_coefficient() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    resp = _http_get(COEFFICIENT_CSV_URL, timeout=90.0)
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    org_meta: dict[str, dict[str, object]] = defaultdict(
        lambda: {"areas": set(), "website": ""},
    )
    for row in rows:
        name = (row.get("Organization Name") or "").strip()
        area = (row.get("Focus Area") or "").strip()
        if not name or "givewell" in area.lower():
            continue
        if _UNIVERSITY_RE.search(name) and area not in _HIGH_SIGNAL_FOCUS_AREAS:
            continue
        if area and area not in _HIGH_SIGNAL_FOCUS_AREAS:
            continue
        org_meta[name]["areas"].add(area)

    for name, meta in org_meta.items():
        areas = meta["areas"]
        if not areas:
            continue
        primary = sorted(areas)[0]
        _merge(
            out,
            EmployerCandidate(
                company_name=name,
                mission_category=_category_from_focus_area(str(primary)),
                discovery_source="coefficient",
            ),
        )
    logger.info("Coefficient Giving: %s employers", len(out))
    return out


def collect_from_sff() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    resp = _http_get(SFF_URL)
    tables = re.findall(r"<table[^>]*>(.*?)</table>", resp.text, re.S | re.I)
    if not tables:
        logger.warning("SFF: no table found")
        return out
    for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", tables[0], re.S | re.I)[1:]:
        cells = [
            html_module.unescape(re.sub(r"<[^>]+>", "", c).strip())
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S | re.I)
        ]
        if len(cells) < 3:
            continue
        org = cells[2].strip()
        if not org:
            continue
        _merge(
            out,
            EmployerCandidate(
                company_name=org,
                mission_category="xrisk",
                discovery_source="sff",
            ),
        )
    logger.info("SFF: %s employers", len(out))
    return out


def collect_from_gwwc() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    resp = _http_get(GWWC_URL)
    for charity in _extract_next_data_charities(resp.text):
        title = (charity.get("title") or "").strip()
        if not title:
            continue
        _merge(
            out,
            EmployerCandidate(
                company_name=title,
                mission_category="effective_altruism",
                discovery_source="gwwc",
            ),
        )
    logger.info("GWWC: %s employers", len(out))
    return out


def collect_from_ace() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    resp = _http_get(ACE_URL)
    review_urls = sorted(
        set(
            re.findall(
                r'href="(https://animalcharityevaluators\.org/charity-review/[^"]+)"',
                resp.text,
            ),
        ),
    )
    with httpx.Client(
        headers=_BROWSER_HEADERS,
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        for url in review_urls:
            try:
                page = client.get(url)
                page.raise_for_status()
            except httpx.HTTPError as exc:
                logger.debug("ACE review fetch %s: %s", url, exc)
                continue
            m = re.search(r"<h1[^>]*>([^<]+)</h1>", page.text)
            name = html_module.unescape(m.group(1).strip()) if m else ""
            if not name:
                continue
            website = ""
            for href in re.findall(r'href="(https?://[^"]+)"', page.text):
                if "animalcharityevaluators.org" in href:
                    continue
                if any(x in href for x in ("facebook.", "twitter.", "linkedin.", "youtube.")):
                    continue
                website = href
                break
            _merge(
                out,
                EmployerCandidate(
                    company_name=name,
                    mission_category="animal_welfare",
                    website=website,
                    discovery_source="ace",
                ),
            )
    logger.info("ACE: %s employers", len(out))
    return out


def collect_from_givewell_top() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    resp = _http_get(GIVEWELL_URL)
    for m in re.finditer(r"<h2[^>]*>([^<]+)</h2>", resp.text):
        name = html_module.unescape(m.group(1).strip())
        if not name or name.lower() in _GIVEWELL_SKIP_HEADINGS:
            continue
        if "donate" in name.lower():
            continue
        _merge(
            out,
            EmployerCandidate(
                company_name=name,
                mission_category="global_health",
                discovery_source="givewell",
            ),
        )
    logger.info("GiveWell top charities: %s employers", len(out))
    return out


def collect_from_gates() -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    csv.field_size_limit(sys.maxsize)
    resp = _http_get(GATES_CSV_URL, timeout=120.0)
    lines = resp.text.splitlines()
    header_idx = 0
    for i, line in enumerate(lines[:5]):
        if "GRANTEE" in line and "GRANT ID" in line:
            header_idx = i
            break
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    seen: set[str] = set()
    for row in reader:
        topic = (row.get("TOPIC") or "").strip()
        if topic not in _GATES_TOPIC_ALLOWLIST:
            continue
        name = (row.get("GRANTEE") or "").strip()
        if not name or name in seen:
            continue
        if _UNIVERSITY_RE.search(name):
            continue
        seen.add(name)
        website = (row.get("GRANTEE WEBSITE") or "").strip()
        _merge(
            out,
            EmployerCandidate(
                company_name=name,
                mission_category="global_health",
                website=website,
                discovery_source="gates",
            ),
        )
    logger.info("Gates Foundation (filtered): %s employers", len(out))
    return out


def _parse_ea_funds_grantees(page_html: str) -> list[str]:
    text = html_module.unescape(re.sub(r"<[^>]+>", " ", page_html))
    text = re.sub(r"\s+", " ", text)
    names = re.findall(
        r"([A-Z][A-Za-z0-9&'’\-\. ]{2,70}?)\s*\(\$[\d,]+(?:\.\d+)?\)\s*:",
        text,
    )
    cleaned: list[str] = []
    for raw in names:
        name = re.sub(r"\s+", " ", raw).strip(" -")
        if len(name) < 3:
            continue
        if name.lower().startswith("highlighted grants"):
            continue
        cleaned.append(name)
    return cleaned


def collect_from_ea_funds_payouts(
    *,
    completed_urls: set[str] | None = None,
    on_candidate: Callable[[EmployerCandidate], None] | None = None,
    on_url_done: Callable[[str, int], None] | None = None,
) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    done = completed_urls or set()
    sitemap = _http_get(EA_FUNDS_SITEMAP_URL).text
    payout_urls = sorted(
        set(re.findall(r"<loc>(https://funds\.effectivealtruism\.org/payouts/[^<]+)</loc>", sitemap)),
    )
    pending = [u for u in payout_urls if u not in done]
    if done:
        logger.info("EA Funds payouts: skipping %s completed URLs (%s remaining)", len(done), len(pending))
    with httpx.Client(
        headers=_BROWSER_HEADERS,
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        for url in pending:
            added = 0
            try:
                page = client.get(url)
                page.raise_for_status()
            except httpx.HTTPError as exc:
                logger.debug("EA Funds payout %s: %s", url, exc)
                continue
            for name in _parse_ea_funds_grantees(page.text):
                cand = EmployerCandidate(
                    company_name=name,
                    mission_category="effective_altruism",
                    discovery_source="ea_funds",
                )
                _merge(out, cand)
                if on_candidate:
                    on_candidate(cand)
                added += 1
            if on_url_done:
                on_url_done(url, added)
    logger.info("EA Funds payouts: %s employers this run", len(out))
    return out


def collect_from_ace_incremental(
    *,
    completed_urls: set[str] | None = None,
    on_candidate: Callable[[EmployerCandidate], None] | None = None,
    on_url_done: Callable[[str, int], None] | None = None,
) -> dict[str, EmployerCandidate]:
    out: dict[str, EmployerCandidate] = {}
    done = completed_urls or set()
    resp = _http_get(ACE_URL)
    review_urls = sorted(
        set(
            re.findall(
                r'href="(https://animalcharityevaluators\.org/charity-review/[^"]+)"',
                resp.text,
            ),
        ),
    )
    pending = [u for u in review_urls if u not in done]
    if done:
        logger.info("ACE reviews: skipping %s completed URLs (%s remaining)", len(done), len(pending))
    with httpx.Client(
        headers=_BROWSER_HEADERS,
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        for url in pending:
            added = 0
            try:
                page = client.get(url)
                page.raise_for_status()
            except httpx.HTTPError as exc:
                logger.debug("ACE review fetch %s: %s", url, exc)
                continue
            m = re.search(r"<h1[^>]*>([^<]+)</h1>", page.text)
            name = html_module.unescape(m.group(1).strip()) if m else ""
            if not name:
                if on_url_done:
                    on_url_done(url, 0)
                continue
            website = ""
            for href in re.findall(r'href="(https?://[^"]+)"', page.text):
                if "animalcharityevaluators.org" in href:
                    continue
                if any(x in href for x in ("facebook.", "twitter.", "linkedin.", "youtube.")):
                    continue
                website = href
                break
            cand = EmployerCandidate(
                company_name=name,
                mission_category="animal_welfare",
                website=website,
                discovery_source="ace",
            )
            _merge(out, cand)
            if on_candidate:
                on_candidate(cand)
            added = 1
            if on_url_done:
                on_url_done(url, added)
    logger.info("ACE: %s employers this run", len(out))
    return out


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_scrape_checkpoint(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"sources": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        logger.warning("Invalid scrape checkpoint JSON: %s", path)
        return {"sources": {}}
    if not isinstance(data, dict):
        return {"sources": {}}
    if not isinstance(data.get("sources"), dict):
        data["sources"] = {}
    return data


def _save_scrape_checkpoint(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = _utc_now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _source_state(checkpoint: dict[str, object], source: str) -> dict[str, object]:
    sources = checkpoint.setdefault("sources", {})
    assert isinstance(sources, dict)
    raw = sources.get(source)
    if isinstance(raw, dict):
        return raw
    state: dict[str, object] = {"status": "pending", "completed_items": [], "count": 0}
    sources[source] = state
    return state


def _source_completed(checkpoint: dict[str, object], source: str) -> bool:
    state = _source_state(checkpoint, source)
    return str(state.get("status") or "") == "completed"


def _completed_items(checkpoint: dict[str, object], source: str) -> set[str]:
    state = _source_state(checkpoint, source)
    items = state.get("completed_items")
    if not isinstance(items, list):
        return set()
    return {str(x) for x in items if x}


def _mark_source_completed(checkpoint: dict[str, object], source: str, count: int) -> None:
    state = _source_state(checkpoint, source)
    state["status"] = "completed"
    state["count"] = count
    state["completed_at"] = _utc_now_iso()


def _mark_item_completed(
    checkpoint: dict[str, object],
    source: str,
    item: str,
    *,
    checkpoint_path: Path,
) -> None:
    state = _source_state(checkpoint, source)
    state["status"] = "in_progress"
    items = state.get("completed_items")
    if not isinstance(items, list):
        items = []
        state["completed_items"] = items
    if item not in items:
        items.append(item)
    state["count"] = len(items)
    _save_scrape_checkpoint(checkpoint_path, checkpoint)


class _IncrementalWriter:
    """Append-only JSONL writer with in-memory merge for resume-safe scraping."""

    def __init__(self, path: Path):
        self.path = path
        self.merged: dict[str, EmployerCandidate] = {}
        self.appended = 0
        if path.exists():
            for cand in _iter_candidates_merged(path):
                key = cand.company_name.strip().lower()
                if key:
                    self.merged[key] = cand

    def record_many(self, batch: dict[str, EmployerCandidate]) -> int:
        added = 0
        for cand in batch.values():
            if self.record(cand):
                added += 1
        return added

    def record(self, cand: EmployerCandidate) -> bool:
        key = cand.company_name.strip().lower()
        if not key:
            return False
        append_candidate_v2(cand, self.path)
        _merge(self.merged, cand)
        self.appended += 1
        return True

    def candidates(self) -> list[EmployerCandidate]:
        return sorted(self.merged.values(), key=lambda c: c.company_name.lower())


def collect_mission_v2_incremental(
    *,
    sources: Iterable[str] | None = None,
    candidates_path: Path = MISSION_V2_CANDIDATES_PATH,
    scrape_checkpoint_path: Path = MISSION_V2_SCRAPE_CHECKPOINT_PATH,
    force_rescrape: bool = False,
) -> list[EmployerCandidate]:
    """
    Collect employers incrementally: append to JSONL and skip completed sources/items.

    Safe to interrupt — rerun resumes from scrape checkpoint and append-only JSONL.
    """
    wanted = {s.strip().lower() for s in (sources or _MISSION_V2_SOURCES)}
    writer = _IncrementalWriter(candidates_path)
    checkpoint = _load_scrape_checkpoint(scrape_checkpoint_path)
    if force_rescrape:
        checkpoint = {"sources": {}}
        _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
    elif not checkpoint.get("sources") and candidates_path.exists() and candidates_path.stat().st_size > 0:
        bootstrap_scrape_checkpoint_from_jsonl(
            candidates_path=candidates_path,
            scrape_checkpoint_path=scrape_checkpoint_path,
            sources=wanted,
        )
        checkpoint = _load_scrape_checkpoint(scrape_checkpoint_path)

    atomic_collectors: dict[str, Callable[[], dict[str, EmployerCandidate]]] = {
        "coefficient": collect_from_coefficient,
        "sff": collect_from_sff,
        "gwwc": collect_from_gwwc,
        "givewell": collect_from_givewell_top,
        "gates": collect_from_gates,
    }

    for key, fn in atomic_collectors.items():
        if key not in wanted:
            continue
        if _source_completed(checkpoint, key):
            logger.info("Skipping completed source: %s", key)
            continue
        logger.info("Scraping source: %s", key)
        _source_state(checkpoint, key)["status"] = "in_progress"
        _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
        try:
            batch = fn()
            added = writer.record_many(batch)
            _mark_source_completed(checkpoint, key, added)
            _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
            logger.info("Source %s done — appended %s rows (%s unique total)", key, added, len(writer.merged))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Source %s failed (will retry next run): %s", key, exc)

    if "ace" in wanted and not _source_completed(checkpoint, "ace"):
        logger.info("Scraping source: ace (per-review checkpoint)")
        _source_state(checkpoint, "ace")["status"] = "in_progress"
        _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)

        def on_ace_url(url: str, _added: int) -> None:
            _mark_item_completed(checkpoint, "ace", url, checkpoint_path=scrape_checkpoint_path)

        try:
            batch = collect_from_ace_incremental(
                completed_urls=_completed_items(checkpoint, "ace"),
                on_candidate=writer.record,
                on_url_done=on_ace_url,
            )
            added = len(batch)
            _mark_source_completed(checkpoint, "ace", added)
            _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
            logger.info("Source ace done — appended %s rows (%s unique total)", added, len(writer.merged))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Source ace failed (will retry next run): %s", exc)

    if "ea_funds" in wanted and not _source_completed(checkpoint, "ea_funds"):
        logger.info("Scraping source: ea_funds (per-payout checkpoint)")
        _source_state(checkpoint, "ea_funds")["status"] = "in_progress"
        _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)

        def on_ea_url(url: str, _added: int) -> None:
            _mark_item_completed(checkpoint, "ea_funds", url, checkpoint_path=scrape_checkpoint_path)

        try:
            batch = collect_from_ea_funds_payouts(
                completed_urls=_completed_items(checkpoint, "ea_funds"),
                on_candidate=writer.record,
                on_url_done=on_ea_url,
            )
            added = len(batch)
            _mark_source_completed(checkpoint, "ea_funds", added)
            _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
            logger.info("Source ea_funds done — appended %s rows (%s unique total)", added, len(writer.merged))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Source ea_funds failed (will retry next run): %s", exc)

    result = writer.candidates()
    logger.info("Mission v2 total unique employers: %s", len(result))
    return result


def collect_mission_v2(
    *,
    sources: Iterable[str] | None = None,
    candidates_path: Path = MISSION_V2_CANDIDATES_PATH,
    scrape_checkpoint_path: Path = MISSION_V2_SCRAPE_CHECKPOINT_PATH,
    incremental: bool = True,
    force_rescrape: bool = False,
) -> list[EmployerCandidate]:
    """Collect employers from mission-v2 sources (incremental by default)."""
    if incremental and not force_rescrape:
        return collect_mission_v2_incremental(
            sources=sources,
            candidates_path=candidates_path,
            scrape_checkpoint_path=scrape_checkpoint_path,
            force_rescrape=False,
        )
    return collect_mission_v2_incremental(
        sources=sources,
        candidates_path=candidates_path,
        scrape_checkpoint_path=scrape_checkpoint_path,
        force_rescrape=True,
    )


def candidate_to_dict(cand: EmployerCandidate) -> dict[str, object]:
    return {
        "company_name": cand.company_name,
        "mission_category": cand.mission_category,
        "website": cand.website,
        "discovery_source": cand.discovery_source,
        "ats_hint": list(cand.ats_hint) if cand.ats_hint else None,
        "extra_slugs": cand.extra_slugs,
    }


def candidate_from_dict(raw: dict[str, object]) -> EmployerCandidate:
    hint_raw = raw.get("ats_hint")
    hint: tuple[str, str] | None = None
    if isinstance(hint_raw, list) and len(hint_raw) == 2:
        hint = (str(hint_raw[0]), str(hint_raw[1]))
    extra = raw.get("extra_slugs")
    return EmployerCandidate(
        company_name=str(raw.get("company_name") or ""),
        mission_category=str(raw.get("mission_category") or "mission"),
        website=str(raw.get("website") or ""),
        discovery_source=str(raw.get("discovery_source") or ""),
        ats_hint=hint,
        extra_slugs=[str(s) for s in extra] if isinstance(extra, list) else [],
    )


def append_candidate_v2(
    cand: EmployerCandidate,
    path: Path = MISSION_V2_CANDIDATES_PATH,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(candidate_to_dict(cand), ensure_ascii=True) + "\n")


def _iter_candidates_merged(path: Path) -> Iterable[EmployerCandidate]:
    merged: dict[str, EmployerCandidate] = {}
    if not path.exists():
        return
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            cand = candidate_from_dict(json.loads(line))
            key = cand.company_name.strip().lower()
            if not key:
                continue
            existing = merged.get(key)
            if existing is None:
                merged[key] = cand
            else:
                _merge(merged, cand)
    yield from merged.values()


def save_candidates_v2(
    candidates: Iterable[EmployerCandidate],
    path: Path = MISSION_V2_CANDIDATES_PATH,
    *,
    overwrite: bool = False,
) -> int:
    """Write candidates to JSONL. Default is append; pass overwrite=True to replace file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if overwrite else "a"
    count = 0
    with path.open(mode, encoding="utf-8") as f:
        for cand in candidates:
            f.write(json.dumps(candidate_to_dict(cand), ensure_ascii=True) + "\n")
            count += 1
    logger.info("%s %s candidates to %s", "Wrote" if overwrite else "Appended", count, path)
    return count


def load_candidates_v2(path: Path = MISSION_V2_CANDIDATES_PATH) -> list[EmployerCandidate]:
    if not path.exists():
        return []
    return sorted(_iter_candidates_merged(path), key=lambda c: c.company_name.lower())


def reset_scrape_checkpoint(path: Path = MISSION_V2_SCRAPE_CHECKPOINT_PATH) -> None:
    if path.exists():
        path.unlink()
        logger.info("Deleted scrape checkpoint: %s", path)


def bootstrap_scrape_checkpoint_from_jsonl(
    *,
    candidates_path: Path = MISSION_V2_CANDIDATES_PATH,
    scrape_checkpoint_path: Path = MISSION_V2_SCRAPE_CHECKPOINT_PATH,
    sources: Iterable[str] | None = None,
) -> bool:
    """
    If JSONL already has candidates but scrape checkpoint is empty, mark sources completed.

    Avoids re-fetching after migrating from the old all-at-once scrape.
    """
    checkpoint = _load_scrape_checkpoint(scrape_checkpoint_path)
    if checkpoint.get("sources"):
        return False
    if not candidates_path.exists() or candidates_path.stat().st_size == 0:
        return False
    wanted = {s.strip().lower() for s in (sources or _MISSION_V2_SOURCES)}
    for key in wanted:
        _mark_source_completed(checkpoint, key, count=0)
        state = _source_state(checkpoint, key)
        state["bootstrapped_from_jsonl"] = True
    _save_scrape_checkpoint(scrape_checkpoint_path, checkpoint)
    logger.info(
        "Bootstrapped scrape checkpoint from existing %s — skipping completed sources",
        candidates_path.name,
    )
    return True
