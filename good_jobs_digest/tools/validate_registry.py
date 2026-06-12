#!/usr/bin/env python3
"""Validate ATS URLs (CSV or build_registry log)."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from discovery.validate import ValidationResult, validate_registry_entry  # noqa: E402


def _parse_log_matches(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    pat = re.compile(
        r"MATCH\s+(?P<ats>greenhouse|lever|smartrecruiters):(?P<slug>\S+)\s+"
        r"(?P<company>.+?)\s+->\s+(?P<url>\S+)",
        re.I,
    )
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = pat.search(line)
        if m:
            rows.append(
                {
                    "company_name": m.group("company").strip(),
                    "ats_type": m.group("ats").lower(),
                    "ats_slug": m.group("slug"),
                    "careers_url": m.group("url"),
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-check ATS board slugs")
    parser.add_argument("--csv", type=Path, help="Registry CSV with ats_type, ats_slug, company_name")
    parser.add_argument("--from-log", type=Path, help="Parse MATCH lines from build_registry log")
    args = parser.parse_args()

    if args.from_log:
        entries = _parse_log_matches(args.from_log)
    elif args.csv:
        entries = []
        with args.csv.open(newline="", encoding="utf-8") as f:
            for raw in csv.DictReader(f):
                if raw.get("ats_slug"):
                    entries.append(raw)
    else:
        raise SystemExit("Provide --csv or --from-log")

    ok = fail = 0
    for raw in entries:
        r = validate_registry_entry(
            company_name=raw.get("company_name", ""),
            ats_type=(raw.get("ats_type") or "").lower(),
            ats_slug=(raw.get("ats_slug") or "").strip(),
            careers_url=(raw.get("careers_url") or "").strip(),
        )
        if isinstance(r, ValidationResult) and r.ok:
            ok += 1
            print(f"OK  {raw.get('ats_type')}:{raw.get('ats_slug')} — {raw.get('company_name')}")
        else:
            fail += 1
            reason = getattr(r, "reason", r)
            print(f"FAIL {raw.get('ats_type')}:{raw.get('ats_slug')} — {reason}")
    print(f"\n{ok} ok, {fail} failed")


if __name__ == "__main__":
    main()
