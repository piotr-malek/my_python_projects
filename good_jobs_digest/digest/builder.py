"""Build markdown digest from ranked job rows (two ingest flows)."""

from __future__ import annotations

from datetime import date
from typing import Any

from digest.formatting import (
    dedupe_by_company_title,
    job_block_lines,
)


def _row_dict(r: Any) -> dict[str, Any]:
    if isinstance(r, dict):
        return r
    return {k: r[k] for k in r.keys()}


def _section_jobs(lines: list[str], heading: str, jobs: list[dict[str, Any]]) -> None:
    if not jobs:
        return
    lines += [f"## {heading}", ""]
    for j in jobs:
        lines += job_block_lines(j)


def build_markdown_digest(
    curated_rows: list[Any],
    board_rows: list[Any],
    *,
    digest_date: date | None = None,
) -> str:
    """Two sections: curated employer ATS jobs, then mission job board listings."""
    digest_date = digest_date or date.today()
    curated_items = dedupe_by_company_title([_row_dict(r) for r in curated_rows])
    board_items = dedupe_by_company_title([_row_dict(r) for r in board_rows])
    total = len(curated_items) + len(board_items)

    lines = [
        f"# Job digest — {digest_date.isoformat()}",
        "",
    ]
    if total:
        lines += [
            f"**{total}** openings not sent in a previous digest "
            f"({len(curated_items)} curated employers, {len(board_items)} job boards).",
            "",
        ]
    else:
        lines += [
            "No new openings to send (all scored jobs were already emailed).",
            "",
        ]

    _section_jobs(lines, "Curated employers (ATS)", curated_items)
    _section_jobs(lines, "Mission job boards", board_items)

    return "\n".join(lines).rstrip() + "\n"
