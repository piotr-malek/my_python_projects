"""Select which curated employers to poll each ingest run (stalest-first rotation)."""

from __future__ import annotations

from core.models import CompanyRow

_EPOCH = "1970-01-01T00:00:00+00:00"


def poll_batch_size(total: int, rotation_divisor: int) -> int:
    """How many employers to poll this run (floor of total / divisor, at least 1)."""
    if total <= 0:
        return 0
    if rotation_divisor <= 1:
        return total
    return max(1, total // rotation_divisor)


def _poll_sort_key(row: CompanyRow) -> str:
    return row.last_validated_at or _EPOCH


def sort_by_stalest_first(companies: list[CompanyRow]) -> list[CompanyRow]:
    """Never-polled first, then oldest last_validated_at."""
    return sorted(companies, key=_poll_sort_key)


def select_poll_batch(
    companies: list[CompanyRow],
    *,
    rotation_divisor: int,
    limit: int | None = None,
) -> list[CompanyRow]:
    """Pick up to total/divisor employers that were checked longest ago."""
    ordered = sort_by_stalest_first(companies)
    batch_size = poll_batch_size(len(ordered), rotation_divisor)
    batch = ordered if rotation_divisor <= 1 else ordered[:batch_size]
    if limit is not None and limit > 0:
        batch = batch[:limit]
    return batch
