#!/usr/bin/env python3
"""Merge backlog rows into companies.csv when the active set is short."""

from __future__ import annotations

import argparse
import csv
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile


def _read(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fieldnames = list(r.fieldnames or [])
        rows = [dict(x) for x in r]
    return fieldnames, rows


def _enabled_count(rows: list[dict[str, str]]) -> int:
    n = 0
    for row in rows:
        if str(row.get("poll_enabled", "")).strip().lower() in {"1", "true", "yes", "y", "on"}:
            n += 1
    return n


def _key(row: dict[str, str]) -> str:
    return f"{(row.get('ats_type') or '').lower()}:{(row.get('ats_slug') or '').strip()}"


def main() -> None:
    ROOT = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description="Append backlog rows into companies.csv up to a target count")
    p.add_argument("--companies", type=Path, default=ROOT / "registry" / "companies.csv")
    p.add_argument("--backlog", type=Path, default=ROOT / "registry" / "backlog.csv")
    p.add_argument("--fill-to", type=int, default=300, help="Target minimum poll_enabled=true rows")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    fields_c, company_rows = _read(args.companies)
    fields_b, backlog_rows = _read(args.backlog)
    if fields_c != fields_b:
        raise SystemExit("companies.csv and backlog.csv must have the same header columns")

    existing = {_key(r) for r in company_rows}
    need = max(0, args.fill_to - _enabled_count(company_rows))
    added = 0
    for row in backlog_rows:
        if need <= 0:
            break
        if _key(row) in existing:
            continue
        if "REPLACE" in (row.get("ats_slug") or "").upper():
            continue
        if str(row.get("poll_enabled", "")).lower() not in {"1", "true", "yes"}:
            continue
        company_rows.append(row)
        existing.add(_key(row))
        added += 1
        need -= 1

    print(f"Would add {added} rows from backlog (target fill-to={args.fill_to}).")
    if args.dry_run or added == 0:
        return

    with NamedTemporaryFile("w", newline="", delete=False, encoding="utf-8") as tmp:
        w = csv.DictWriter(tmp, fieldnames=fields_c, extrasaction="ignore")
        w.writeheader()
        w.writerows(company_rows)
        tmp_path = Path(tmp.name)
    shutil.move(tmp_path, args.companies)
    print(f"Updated {args.companies}")


if __name__ == "__main__":
    main()
