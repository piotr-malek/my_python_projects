"""Validate registry rows: live ATS endpoint + employer/board alignment."""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from discovery.resolve import (
    _greenhouse_board,
    _lever_board,
    _smartrecruiters_board,
    employer_names_align,
    slug_aligns_with_company,
)


@dataclass
class ValidationResult:
    company_name: str
    ats_type: str
    ats_slug: str
    ok: bool
    http_ok: bool
    has_postings: bool
    name_ok: bool
    slug_ok: bool
    board_name: str = ""
    reason: str = ""

    @property
    def label(self) -> str:
        return f"{self.ats_type}:{self.ats_slug}"


def _identity_ok(company_name: str, ats_slug: str, board_name: str) -> tuple[bool, bool]:
    """Return (name_ok, slug_ok)."""
    slug_ok = slug_aligns_with_company(ats_slug, company_name)
    name_ok = bool(board_name) and employer_names_align(company_name, board_name)
    return name_ok, slug_ok


def validate_registry_entry(
    client: httpx.Client,
    *,
    company_name: str,
    ats_type: str,
    ats_slug: str,
    ats_region: str = "global",
    require_identity: bool = True,
) -> ValidationResult:
    """Check ATS is live with postings and matches the intended employer."""
    ats = ats_type.lower().strip()
    slug = ats_slug.strip()
    base = ValidationResult(
        company_name=company_name,
        ats_type=ats,
        ats_slug=slug,
        ok=False,
        http_ok=False,
        has_postings=False,
        name_ok=False,
        slug_ok=False,
    )
    if not slug or "REPLACE" in slug.upper():
        base.reason = "placeholder slug"
        return base

    board_name = ""
    try:
        if ats == "greenhouse":
            has_jobs, board_name = _greenhouse_board(client, slug)
            base.http_ok = has_jobs
            base.has_postings = has_jobs
        elif ats == "smartrecruiters":
            has_jobs, board_name = _smartrecruiters_board(client, slug)
            base.http_ok = has_jobs
            base.has_postings = has_jobs
        elif ats == "lever":
            has_jobs, board_name = _lever_board(client, slug, region=ats_region)
            if not has_jobs and ats_region != "eu":
                has_jobs, board_name = _lever_board(client, slug, region="eu")
            base.http_ok = has_jobs
            base.has_postings = has_jobs
        else:
            base.reason = f"unknown ats_type {ats}"
            return base
    except httpx.RequestError as exc:
        base.reason = str(exc)
        return base

    if not base.has_postings:
        base.reason = "no open postings (or HTTP error)"
        return base

    base.name_ok, base.slug_ok = _identity_ok(company_name, slug, board_name)
    base.board_name = board_name

    if require_identity and ats in ("lever", "smartrecruiters") and not board_name:
        if not base.slug_ok:
            base.reason = f"slug mismatch (slug {slug!r} vs {company_name!r})"
            return base
    elif require_identity and ats == "lever":
        if not base.slug_ok:
            base.reason = f"slug mismatch (slug {slug!r} vs {company_name!r})"
            return base
    elif require_identity and board_name and not base.name_ok:
        base.reason = f"board name mismatch (board={board_name!r})"
        return base
    elif require_identity and not (base.name_ok or base.slug_ok):
        base.reason = (
            f"identity mismatch (board={board_name!r})"
            if board_name
            else f"identity mismatch (slug {slug!r} vs {company_name!r})"
        )
        return base

    base.ok = True
    base.reason = "ok"
    return base
