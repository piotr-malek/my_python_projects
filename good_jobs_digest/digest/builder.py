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


def _health_footer(
    source_stats: list[Any] | None,
    llm_usage: dict[str, Any] | None,
) -> list[str]:
    """Per-source ingest results, so a silently broken board is visible in the email.

    A source fetching plenty but matching nothing is normal; a source fetching zero
    is usually blocked, rotated its API key, or died.
    """
    if not source_stats and not llm_usage:
        return []

    lines = ["---", "", "### Run health", ""]
    if source_stats:
        rows = [_row_dict(s) for s in source_stats]
        broken = [r for r in rows if r.get("error") or not int(r.get("fetched") or 0)]
        healthy = [r for r in rows if r not in broken]
        lines += ["| Source | Fetched | Matched |", "|---|---:|---:|"]
        for r in sorted(healthy, key=lambda x: -int(x.get("passed") or 0)):
            lines.append(
                f"| {r.get('source')} | {int(r.get('fetched') or 0)} | {int(r.get('passed') or 0)} |"
            )
        lines.append("")
        if broken:
            lines.append("**Sources returning nothing:**")
            lines.append("")
            for r in broken:
                reason = r.get("error") or "fetched 0 items"
                lines.append(f"- `{r.get('source')}` — {reason}")
            lines.append("")
    if llm_usage:
        used = llm_usage.get("used")
        budget = llm_usage.get("budget")
        cap = f"{used}/{budget}" if budget else str(used)
        lines += [f"Gemini requests today: **{cap}**.", ""]
    return lines


def build_markdown_digest(
    curated_rows: list[Any],
    board_rows: list[Any],
    *,
    digest_date: date | None = None,
    source_stats: list[Any] | None = None,
    llm_usage: dict[str, Any] | None = None,
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
    lines += _health_footer(source_stats, llm_usage)

    return "\n".join(lines).rstrip() + "\n"
