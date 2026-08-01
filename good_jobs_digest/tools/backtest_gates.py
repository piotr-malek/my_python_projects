#!/usr/bin/env python3
"""Replay the current title gate over stored jobs (read-only) and report deltas.

Compares each row's stored prefilter_pass (decided at ingest time) with what
rank.prefilter.evaluate_title decides today using the live settings, so keyword
or gate changes can be validated against real data before a rescore.

Usage: python tools/backtest_gates.py [--db data/jobs.db] [--samples 12]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings  # noqa: E402
from rank.prefilter import evaluate_title  # noqa: E402


def _reason_bucket(reason: str | None) -> str:
    if not reason:
        return "passed"
    return reason.split(":", 1)[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(settings.SQLITE_PATH))
    parser.add_argument("--samples", type=int, default=12)
    args = parser.parse_args()

    uri = f"file:{args.db}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, source, title, company_name, location_text, prefilter_pass,"
        " digest_included_at FROM jobs"
    ).fetchall()
    conn.close()

    kw = dict(
        include_keywords=settings.TARGET_ROLE_KEYWORDS,
        exclude_keywords=settings.EXCLUDE_TITLE_KEYWORDS,
        seniority_exclude_keywords=settings.SENIORITY_EXCLUDE_KEYWORDS,
    )

    now_pass = 0
    gained: list[sqlite3.Row] = []
    lost: list[tuple[sqlite3.Row, str]] = []
    lost_reasons: Counter[str] = Counter()
    gained_by_source: Counter[str] = Counter()
    digested_lost: list[tuple[sqlite3.Row, str]] = []

    for row in rows:
        verdict = evaluate_title(row["title"], **kw)
        if verdict.passed:
            now_pass += 1
        old = bool(row["prefilter_pass"])
        if verdict.passed and not old:
            gained.append(row)
            gained_by_source[row["source"]] += 1
        elif old and not verdict.passed:
            reason = verdict.reason or "?"
            lost.append((row, reason))
            lost_reasons[_reason_bucket(reason)] += 1
            if row["digest_included_at"]:
                digested_lost.append((row, reason))

    total = len(rows)
    old_pass = sum(1 for r in rows if r["prefilter_pass"])
    print(f"Rows: {total} | title-gate pass: {old_pass} (stored) -> {now_pass} (new)")
    print()

    print(f"Previously DROPPED, now PASS: {len(gained)}")
    for src, n in gained_by_source.most_common():
        print(f"  {src:20s} {n}")
    for row in gained[: args.samples]:
        print(f"    + [{row['source']}] {row['title']} — {row['company_name']}")
    if len(gained) > args.samples:
        print(f"    ... and {len(gained) - args.samples} more")
    print()

    print(f"Previously PASSED, now DROPPED: {len(lost)}")
    for bucket, n in lost_reasons.most_common():
        print(f"  {bucket:20s} {n}")
    by_bucket: dict[str, list[tuple[sqlite3.Row, str]]] = defaultdict(list)
    for row, reason in lost:
        by_bucket[_reason_bucket(reason)].append((row, reason))
    for bucket, items in by_bucket.items():
        print(f"  -- {bucket} --")
        for row, reason in items[: args.samples]:
            print(f"    - [{row['source']}] {row['title']} ({reason})")
        if len(items) > args.samples:
            print(f"    ... and {len(items) - args.samples} more")
    print()

    n_digested = sum(1 for r in rows if r["digest_included_at"])
    print(
        f"Already-digested rows that the new title gate would have excluded: "
        f"{len(digested_lost)} of {n_digested}"
    )
    for row, reason in digested_lost[: args.samples]:
        print(f"    - [{row['source']}] {row['title']} ({reason})")
    if len(digested_lost) > args.samples:
        print(f"    ... and {len(digested_lost) - args.samples} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
