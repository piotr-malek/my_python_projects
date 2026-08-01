"""Tests for batched ATS probing."""

from __future__ import annotations

from discovery.ats_registry import ATS_PROBE_ORDER, probe_batches_for


def test_probe_batches_cover_all_ats_types():
    order = list(ATS_PROBE_ORDER)
    batches = probe_batches_for(order)
    flat = [ats for wave in batches for ats in wave]
    assert flat == order


def test_probe_batches_respects_only_ats_filter():
    order = ["greenhouse", "workable", "workday"]
    batches = probe_batches_for(order)
    flat = [ats for wave in batches for ats in wave]
    assert flat == order
    assert batches[0] == ["greenhouse"]
    assert "workable" in batches[1]
