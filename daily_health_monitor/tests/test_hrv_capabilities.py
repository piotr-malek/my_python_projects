"""Tests for v5.1 capability gating and HRV proxy policy."""

from datetime import date

import pandas as pd

from garmin.capabilities import _has_nightly_hrv, _has_sleep_score
from analytics.derived_blocks import enrich_hrv_proxy_block, proxy_zscore_magnitude
from pipeline.digest_payload import DigestPayloadBuilder


def test_has_nightly_hrv_empty():
    assert _has_nightly_hrv({}) is False


def test_has_nightly_hrv_present():
    assert _has_nightly_hrv({"lastNightAvg": 42.0}) is True


def test_capability_gate_drops_unavailable_fields():
    tw = {
        "sleep_score": {"today": None},
        "sleep_stress": {"today": None},
        "hrv_rmssd_ms": {"today": 48},
        "rhr_bpm": {"today": 50},
    }
    caps = {"nightly_hrv": False, "sleep_score": False, "sleep_stress": False}
    out = DigestPayloadBuilder._capability_gate(tw, caps, "nocturnal_proxy")
    assert "sleep_score" not in out
    assert "sleep_stress" not in out
    assert "hrv_rmssd_ms" not in out
    assert "rhr_bpm" in out


def test_enrich_hrv_proxy_strips_raw_index():
    block = {"today": -17.5, "baseline": -18.0, "delta": 0.5, "baseline_label": "30d"}
    hist = pd.Series([-19.0, -18.5, -18.0, -17.8, -17.5, -17.2, -17.0])
    out = enrich_hrv_proxy_block(block, hist)
    assert "today" not in out
    assert out.get("percentile_30d") is not None
    assert out.get("source") == "nocturnal_proxy"
    assert out.get("confidence") == "medium"


def test_proxy_zscore_magnitude_bands():
    assert proxy_zscore_magnitude(0.2) == "noise"
    assert proxy_zscore_magnitude(0.7) == "mild"
    assert proxy_zscore_magnitude(1.5) == "significant"
    assert proxy_zscore_magnitude(2.5) == "strong"


def test_hrv_source_from_raw_hrv_proxy():
    target = date(2026, 5, 30)
    caps = {"nightly_hrv": False, "nocturnal_proxy": True}
    wellness = {
        "raw_hrv": pd.DataFrame(
            [{"date": "2026-05-30", "last_night_avg_ms": None, "nocturnal_proxy": -17.2}]
        )
    }
    assert DigestPayloadBuilder._hrv_source(wellness, pd.DataFrame(), target, caps) == "nocturnal_proxy"
