#!/usr/bin/env python3
"""
Assign human-friendly display_name values for regions in google_earth.regions_info.

BigQuery is the source of truth. Use this after adding new subregions:

  python templates/regions_creation/build_region_display_names.py research
  python templates/regions_creation/build_region_display_names.py generate
  python templates/regions_creation/build_region_display_names.py validate
  python templates/regions_creation/build_region_display_names.py append-bq

By default only processes rows where display_name IS NULL.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=ROOT / ".env", override=False)
sys.path.insert(0, str(ROOT))

from llm_interaction.ollama_utils import send_prompt_to_ollama
from templates.regions_creation.region_display_geocoding import research_region
from utils.bq_utils import execute_sql, load_from_bigquery

TEMPLATES_DIR = Path(__file__).resolve().parent
PENDING_JSON = TEMPLATES_DIR / "region_display_names.pending.json"

PROJECT_ID = "disaster-predictor-470812"
DATASET = "google_earth"
TABLE = "regions_info"


def _load_regions_from_bq() -> pd.DataFrame:
    q = f"""
    SELECT
      region,
      parent_region,
      country,
      area_km2,
      centroid_lat,
      centroid_lon,
      lon_min,
      lat_min,
      lon_max,
      lat_max,
      display_name
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    ORDER BY parent_region, region
    """
    df = load_from_bigquery(q, project_id=PROJECT_ID)
    if df is None or df.empty:
        raise RuntimeError("No rows in regions_info")
    df["country"] = df["country"].fillna(df["parent_region"])
    return df


def _pending_targets(df: pd.DataFrame, only_missing: bool) -> pd.DataFrame:
    if only_missing:
        missing = df["display_name"].isna() | (df["display_name"].astype(str).str.strip() == "")
        return df[missing].copy()
    return df.copy()


def _load_pending() -> dict[str, dict[str, Any]]:
    if not PENDING_JSON.is_file():
        return {}
    return {r["region"]: r for r in json.loads(PENDING_JSON.read_text(encoding="utf-8"))}


def _save_pending(rows: list[dict[str, Any]]) -> None:
    PENDING_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _row_dict(r: pd.Series, display_name: str | None, source: str) -> dict[str, Any]:
    return {
        "region": r["region"],
        "parent": r["parent_region"],
        "display_name": display_name,
        "country": r["country"],
        "centroid_lon": float(r.centroid_lon),
        "centroid_lat": float(r.centroid_lat),
        "lon_min": float(r.lon_min),
        "lat_min": float(r.lat_min),
        "lon_max": float(r.lon_max),
        "lat_max": float(r.lat_max),
        "source": source,
    }


def cmd_research(args: argparse.Namespace) -> None:
    df = _load_regions_from_bq()
    targets = _pending_targets(df, only_missing=not args.force)
    pending = _load_pending()
    research: dict[str, Any] = {
        rid: row.get("research")
        for rid, row in pending.items()
        if row.get("research")
    }

    total = len(targets)
    print(f"Researching {total} region(s)...")
    for i, (_, row) in enumerate(targets.iterrows(), 1):
        rid = row["region"]
        if rid in research and not args.force:
            continue
        print(f"[{i}/{total}] {rid} ({row['country']})")
        rec = research_region(
            region=rid,
            parent_region=row["parent_region"],
            country=row["country"],
            centroid_lat=float(row["centroid_lat"]),
            centroid_lon=float(row["centroid_lon"]),
            lon_min=float(row["lon_min"]),
            lat_min=float(row["lat_min"]),
            lon_max=float(row["lon_max"]),
            lat_max=float(row["lat_max"]),
            area_km2=float(row["area_km2"]) if pd.notna(row["area_km2"]) else None,
        )
        research[rid] = rec
        entry = pending.get(rid, _row_dict(row, None, "pending"))
        entry["research"] = rec
        pending[rid] = entry

    _save_pending(list(pending.values()))
    print(f"Wrote {PENDING_JSON} ({len(pending)} pending region(s))")


def _build_naming_prompt(parent_region: str, country: str, items: list[dict[str, Any]]) -> str:
    lines = [
        "You assign short, recognizable DISPLAY NAMES for disaster-monitoring map subregions.",
        "Locals should immediately recognize the area. Use real towns, rivers, parks, or landmarks.",
        "",
        "Rules:",
        '- Return ONLY valid JSON: {"assignments": [{"region": "...", "display_name": "..."}, ...]}',
        "- One entry per region id below; display_name must be UNIQUE within this parent group.",
        "- Prefer the nearest well-known settlement or geographic feature inside/near the bbox.",
        "- 2–5 words typical; use proper diacritics.",
        "- Never use compass-only labels (North/South/East/West).",
        "",
        f"PARENT REGION: {parent_region}",
        f"COUNTRY: {country}",
        "",
        "SUBREGIONS (with geocoding research):",
    ]
    for it in items:
        lines.append(json.dumps(it, ensure_ascii=False))
    return "\n".join(lines)


def _parse_assignments(text: str) -> list[dict[str, str]]:
    text = text.strip()
    if "```" in text:
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    payload = json.loads(text)
    if isinstance(payload, list):
        return payload
    return payload.get("assignments") or payload.get("regions") or []


def cmd_generate(args: argparse.Namespace) -> None:
    df = _load_regions_from_bq()
    targets = _pending_targets(df, only_missing=not args.force)
    pending = _load_pending()

    if not pending and not args.force:
        raise SystemExit(f"No {PENDING_JSON}; run `research` first.")

    names: dict[str, dict[str, Any]] = dict(pending)
    groups = targets.groupby(["parent_region", "country"], sort=True)
    print(f"Generating names for {len(groups)} parent group(s) via Ollama...")

    for gi, ((parent, country), gdf) in enumerate(groups, 1):
        todo = [r for r in gdf["region"].tolist() if r not in names or not names[r].get("display_name") or args.force]
        if not todo:
            continue
        print(f"[{gi}/{len(groups)}] {parent} ({country}) — {len(todo)} region(s)")

        batch_items = []
        for rid in todo:
            row = gdf[gdf["region"] == rid].iloc[0]
            rec = (names.get(rid) or {}).get("research") or {}
            batch_items.append(
                {
                    "region": rid,
                    "parent_region": parent,
                    "country": country,
                    "centroid_lat": float(row.centroid_lat),
                    "centroid_lon": float(row.centroid_lon),
                    "bbox": [float(row.lon_min), float(row.lat_min), float(row.lon_max), float(row.lat_max)],
                    "area_km2": float(row.area_km2) if pd.notna(row.area_km2) else None,
                    "geocode": rec.get("centroid_geocode"),
                    "corner_geocode": rec.get("corner_geocode"),
                    "nearby_places": rec.get("nearby_places"),
                }
            )

        raw = send_prompt_to_ollama(
            model="",
            prompt=_build_naming_prompt(parent, country, batch_items),
            temperature=0.2,
            top_p=0.85,
            max_output_tokens=4096,
            num_ctx=8192,
        )
        try:
            assignments = _parse_assignments(raw)
        except json.JSONDecodeError as exc:
            print(f"  ✗ JSON parse error for {parent}: {exc}")
            print(raw[:500])
            continue

        for a in assignments:
            rid = a.get("region")
            dn = (a.get("display_name") or "").strip()
            if not rid or not dn:
                continue
            row = gdf[gdf["region"] == rid]
            if row.empty:
                continue
            r = row.iloc[0]
            entry = names.get(rid, _row_dict(r, None, "ollama"))
            entry["display_name"] = dn
            entry["source"] = "ollama"
            names[rid] = entry
            print(f"    {rid} -> {dn}")

    _save_pending(list(names.values()))
    missing = sum(1 for v in names.values() if not v.get("display_name"))
    print(f"Updated {PENDING_JSON} — {missing} still without display_name")


def _validate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    names = [r.get("display_name") for r in rows if r.get("display_name")]
    if len(names) != len(set(names)):
        from collections import Counter

        dupes = [n for n, c in Counter(names).items() if c > 1]
        issues.append({"type": "duplicate_display_name", "values": dupes})

    generic_pat = re.compile(
        r"^(north|south|east|west|central|northern|southern|eastern|western)(\s+region)?$",
        re.I,
    )
    for r in rows:
        dn = r.get("display_name") or ""
        if not dn:
            issues.append({"type": "missing", "region": r["region"]})
            continue
        if generic_pat.match(dn.strip()):
            issues.append({"type": "generic_compass", "region": r["region"], "display_name": dn})
        if len(dn) > 60:
            issues.append({"type": "too_long", "region": r["region"], "display_name": dn})

    by_parent: dict[str, list[str]] = {}
    for r in rows:
        if r.get("display_name"):
            by_parent.setdefault(r["parent"], []).append(r["display_name"])
    for parent, dnames in by_parent.items():
        if len(dnames) != len(set(dnames)):
            issues.append({"type": "duplicate_within_parent", "parent": parent})
    return issues


def cmd_validate(args: argparse.Namespace) -> None:
    pending = list(_load_pending().values())
    if not pending:
        raise SystemExit(f"No {PENDING_JSON}; run generate first.")

    issues = _validate_rows(pending)
    if issues:
        print(f"Validation: {len(issues)} issue(s)")
        for it in issues[:20]:
            print(f"  - {it}")
        if not args.allow_issues:
            raise SystemExit(1)
    else:
        print("Validation passed.")


def cmd_append_bq(args: argparse.Namespace) -> None:
    pending = list(_load_pending().values())
    if not pending:
        raise SystemExit(f"No {PENDING_JSON}")

    to_write = [r for r in pending if r.get("display_name")]
    if not to_write:
        raise SystemExit("No display_name values in pending file.")

    if not args.skip_validate:
        issues = _validate_rows(to_write)
        if issues and not args.allow_issues:
            print(f"Validation failed with {len(issues)} issue(s)")
            raise SystemExit(1)

    execute_sql(
        f"ALTER TABLE `{PROJECT_ID}.{DATASET}.{TABLE}` ADD COLUMN IF NOT EXISTS display_name STRING",
        project_id=PROJECT_ID,
    )

    temp = f"_temp_display_names_{int(time.time())}"
    df = pd.DataFrame([{"region": r["region"], "display_name": r["display_name"]} for r in to_write])
    client = bigquery.Client.from_service_account_json(
        str(ROOT / "config" / "service_account.json"), project=PROJECT_ID
    )
    job = client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{DATASET}.{temp}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            schema=[
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("display_name", "STRING"),
            ],
        ),
    )
    job.result()

    execute_sql(
        f"""
        MERGE `{PROJECT_ID}.{DATASET}.{TABLE}` t
        USING `{PROJECT_ID}.{DATASET}.{temp}` s
        ON t.region = s.region
        WHEN MATCHED THEN UPDATE SET t.display_name = s.display_name
        """,
        project_id=PROJECT_ID,
    )
    execute_sql(f"DROP TABLE IF EXISTS `{PROJECT_ID}.{DATASET}.{temp}`", project_id=PROJECT_ID)
    print(f"Updated display_name for {len(df)} region(s).")

    if PENDING_JSON.is_file():
        PENDING_JSON.unlink()
        print(f"Removed {PENDING_JSON.name}")


def cmd_all(args: argparse.Namespace) -> None:
    cmd_research(args)
    cmd_generate(args)
    cmd_validate(args)
    cmd_append_bq(args)


def main() -> None:
    p = argparse.ArgumentParser(description="Assign display_name for regions in regions_info")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--force", action="store_true", help="Re-process all regions, not only NULL display_name")

    add_common(sub.add_parser("research", help="Geocode pending regions (Nominatim)"))
    add_common(sub.add_parser("generate", help="Propose display names (Ollama)"))
    sp_v = sub.add_parser("validate", help="Check pending names")
    sp_v.add_argument("--allow-issues", action="store_true")
    sp_a = sub.add_parser("append-bq", help="MERGE pending names into regions_info")
    sp_a.add_argument("--allow-issues", action="store_true")
    sp_a.add_argument("--skip-validate", action="store_true")
    sp_all = sub.add_parser("all", help="research → generate → validate → append-bq")
    add_common(sp_all)
    sp_all.add_argument("--allow-issues", action="store_true")

    args = p.parse_args()
    {"research": cmd_research, "generate": cmd_generate, "validate": cmd_validate, "append-bq": cmd_append_bq, "all": cmd_all}[args.cmd](args)


if __name__ == "__main__":
    main()
