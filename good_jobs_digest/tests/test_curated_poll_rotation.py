"""Tests for curated poll rotation batch selection."""

from __future__ import annotations

from core.curated_poll_rotation import poll_batch_size, select_poll_batch
from core.models import CompanyRow


def _row(name: str, last_validated_at: str | None = None) -> CompanyRow:
    return CompanyRow(
        company_name=name,
        ats_type="greenhouse",
        ats_slug=name.lower().replace(" ", ""),
        last_validated_at=last_validated_at,
    )


def test_poll_batch_size_floor_division():
    assert poll_batch_size(213, 3) == 71
    assert poll_batch_size(5, 3) == 1
    assert poll_batch_size(0, 3) == 0


def test_poll_batch_size_divisor_one_polls_all():
    assert poll_batch_size(100, 1) == 100


def test_select_poll_batch_stalest_first():
    companies = [
        _row("Recent", "2026-06-20T00:00:00+00:00"),
        _row("Never"),
        _row("Old", "2026-01-01T00:00:00+00:00"),
        _row("Mid", "2026-03-01T00:00:00+00:00"),
    ]
    batch = select_poll_batch(companies, rotation_divisor=2)
    assert len(batch) == 2
    assert batch[0].company_name == "Never"
    assert batch[1].company_name == "Old"


def test_select_poll_batch_three_day_rotation():
    companies = [f"Co{i}" for i in range(9)]
    rows = [_row(name) for name in companies]

    day1 = select_poll_batch(rows, rotation_divisor=3)
    assert len(day1) == 3
    assert [r.company_name for r in day1] == ["Co0", "Co1", "Co2"]

    ts = "2026-06-23T12:00:00+00:00"
    for row in day1:
        row.last_validated_at = ts

    day2 = select_poll_batch(rows, rotation_divisor=3)
    assert [r.company_name for r in day2] == ["Co3", "Co4", "Co5"]

    for row in day2:
        row.last_validated_at = ts

    day3 = select_poll_batch(rows, rotation_divisor=3)
    assert [r.company_name for r in day3] == ["Co6", "Co7", "Co8"]

    for row in day3:
        row.last_validated_at = ts

    day4 = select_poll_batch(rows, rotation_divisor=3)
    assert [r.company_name for r in day4] == ["Co0", "Co1", "Co2"]


def test_select_poll_batch_limit_caps_further():
    companies = [_row(f"Co{i}") for i in range(9)]
    batch = select_poll_batch(companies, rotation_divisor=3, limit=2)
    assert len(batch) == 2
