"""Employer row used by curated ATS ingest."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CompanyRow:
    company_name: str
    ats_type: str
    ats_slug: str
    ats_region: str = "global"
    careers_url: str = ""
    poll_enabled: bool = True
    mission_category: str | None = "mission"
    notes: str = ""


def effective_poll_enabled(row: CompanyRow, overrides: dict[str, bool]) -> bool:
    key = f"{row.ats_type.lower()}:{row.ats_slug}"
    if key in overrides:
        return bool(overrides[key])
    return row.poll_enabled
