"""Shared employer candidate pool mined from aggregators and discovery runs."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from discovery.resolve import EmployerCandidate, parse_ats_from_text

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POOL_PATH = ROOT / "data" / "employer_candidates.jsonl"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _norm_name(name: str) -> str:
    return name.strip().lower()


def mine_employer_candidate(
    norm: dict[str, Any],
    *,
    discovery_source: str,
    pool_path: Path = DEFAULT_POOL_PATH,
    website: str = "",
) -> None:
    """Append org from a normalized job listing to the candidate pool."""
    name = (norm.get("company_name") or "").strip()
    if not name or name.lower() == "unknown":
        return
    url_blob = " ".join(
        str(norm.get(k) or "")
        for k in ("url", "description_text", "apply_url")
    )
    hint = parse_ats_from_text(url_blob)
    record = {
        "company_name": name,
        "discovery_source": discovery_source,
        "mission_category": (norm.get("mission_category") or "mission").strip(),
        "website": website.strip(),
        "ats_hint": list(hint) if hint else None,
        "seen_at": _utc_now_iso(),
        "probed": False,
    }
    _append_unique(record, pool_path)


def _append_unique(record: dict[str, Any], pool_path: Path) -> None:
    pool_path.parent.mkdir(parents=True, exist_ok=True)
    key = _norm_name(record["company_name"])
    existing_keys = _load_keys(pool_path)
    if key in existing_keys:
        return
    with pool_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def append_employer_candidates(
    candidates: list[EmployerCandidate],
    *,
    pool_path: Path = DEFAULT_POOL_PATH,
) -> int:
    """Append discovery candidates to the pool; return count of new rows."""
    existing_keys = _load_keys(pool_path)
    added = 0
    for cand in candidates:
        name = cand.company_name.strip()
        if not name:
            continue
        key = _norm_name(name)
        if key in existing_keys:
            continue
        record = {
            "company_name": name,
            "discovery_source": cand.discovery_source or "discovery",
            "mission_category": (cand.mission_category or "mission").strip(),
            "website": (cand.website or "").strip(),
            "ats_hint": list(cand.ats_hint) if cand.ats_hint else None,
            "seen_at": _utc_now_iso(),
            "probed": False,
        }
        _append_unique(record, pool_path)
        existing_keys.add(key)
        added += 1
    if added:
        logger.info("Appended %s new employer candidate(s) to %s", added, pool_path.name)
    return added


def _load_keys(pool_path: Path) -> set[str]:
    if not pool_path.is_file():
        return set()
    keys: set[str] = set()
    for line in pool_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            name = _norm_name(str(obj.get("company_name") or ""))
            if name:
                keys.add(name)
        except json.JSONDecodeError:
            continue
    return keys


def load_unprobed_candidates(
    pool_path: Path = DEFAULT_POOL_PATH,
    *,
    limit: int | None = None,
) -> list[EmployerCandidate]:
    if not pool_path.is_file():
        return []
    out: list[EmployerCandidate] = []
    for line in pool_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError:
            continue
        if raw.get("probed"):
            continue
        name = (raw.get("company_name") or "").strip()
        if not name:
            continue
        hint_raw = raw.get("ats_hint")
        hint: tuple[str, str] | None = None
        if isinstance(hint_raw, list) and len(hint_raw) == 2:
            hint = (str(hint_raw[0]), str(hint_raw[1]))
        out.append(
            EmployerCandidate(
                company_name=name,
                mission_category=str(raw.get("mission_category") or "mission"),
                website=str(raw.get("website") or ""),
                discovery_source=str(raw.get("discovery_source") or "mining"),
                ats_hint=hint,
            )
        )
        if limit is not None and len(out) >= limit:
            break
    return out


def mark_probed(company_names: list[str], pool_path: Path = DEFAULT_POOL_PATH) -> None:
    """Rewrite pool file marking given companies as probed."""
    if not pool_path.is_file():
        return
    wanted = {_norm_name(n) for n in company_names}
    lines_out: list[str] = []
    for line in pool_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            lines_out.append(line)
            continue
        if _norm_name(str(obj.get("company_name") or "")) in wanted:
            obj["probed"] = True
        lines_out.append(json.dumps(obj, ensure_ascii=True))
    pool_path.write_text("\n".join(lines_out) + "\n", encoding="utf-8")
