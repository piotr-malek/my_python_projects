"""A provider outage must reach the inbox, not silence it.

`check-llm` aborting the run is how five consecutive days produced no email at
all when Mistral stopped serving the configured model: an empty inbox reads
exactly like a quiet week. These tests pin the replacement — the check warns,
the run continues, and the digest says what happened.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

import pytest

from config import Settings
from core import llm_health
from digest.builder import build_markdown_digest


@pytest.fixture()
def settings(tmp_path) -> Settings:
    s = Settings()
    s.LLM_USAGE_PATH = tmp_path / "llm_usage.json"
    return s


def test_health_round_trip(settings):
    assert llm_health.read(settings) is None  # nothing recorded yet
    llm_health.record(settings, ok=False, detail="HTTP 429: rate limited")
    got = llm_health.read(settings)
    assert got == {"ok": False, "detail": "HTTP 429: rate limited"}
    llm_health.record(settings, ok=True)
    assert llm_health.read(settings) == {"ok": True, "detail": ""}


def test_yesterdays_outage_is_ignored(settings, tmp_path):
    """Health is per-day: a provider that failed yesterday must not colour today."""
    (tmp_path / "llm_health.json").write_text(
        json.dumps({"date": (date.today() - timedelta(days=1)).isoformat(),
                    "ok": False, "detail": "old news"}),
        encoding="utf-8",
    )
    assert llm_health.read(settings) is None


def test_health_never_raises_on_an_unwritable_path(tmp_path):
    s = Settings()
    s.LLM_USAGE_PATH = tmp_path / "nope" / "\0bad" / "usage.json"
    llm_health.record(s, ok=False, detail="whatever")  # must not raise
    assert llm_health.read(s) is None


def test_footer_reports_the_outage():
    md = build_markdown_digest(
        [], [], digest_date=date(2026, 9, 7),
        llm_usage={"used": 2, "budget": 300, "provider": "mistral",
                   "error": "all 12 scoring call(s) failed"},
    )
    assert "**Mistral was unreachable today**" in md
    assert "all 12 scoring call(s) failed" in md
    assert "retried on the next run" in md


def test_footer_reports_an_outage_with_no_usage_recorded():
    """The preflight can fail before anything is counted, so the outage line must
    not depend on a request count being present."""
    md = build_markdown_digest(
        [], [], digest_date=date(2026, 9, 7),
        llm_usage={"provider": "mistral", "error": "HTTP 429: Rate limit exceeded"},
    )
    assert "**Mistral was unreachable today**" in md
    assert "requests today" not in md


def test_healthy_footer_says_nothing_about_outages():
    md = build_markdown_digest(
        [], [], digest_date=date(2026, 9, 7),
        llm_usage={"used": 4, "budget": 300, "provider": "mistral"},
    )
    assert "Mistral requests today: **4/300**." in md
    assert "unreachable" not in md


def test_check_llm_warns_and_continues(monkeypatch, tmp_path, caplog):
    """The whole point: a dead provider must not stop the run."""
    import main

    monkeypatch.setattr(main.settings, "LLM_USAGE_PATH", tmp_path / "llm_usage.json")
    monkeypatch.setattr(
        "rank.llm.make_llm_client",
        lambda _s: (_ for _ in ()).throw(RuntimeError("HTTP 429: Rate limit exceeded")),
    )
    main.cmd_check_llm(argparse.Namespace(strict=False))  # no SystemExit

    health = llm_health.read(main.settings)
    assert health["ok"] is False
    assert "429" in health["detail"]


def test_check_llm_strict_still_fails(monkeypatch, tmp_path):
    """Kept for running by hand, where a silent pass would be useless."""
    import main

    monkeypatch.setattr(main.settings, "LLM_USAGE_PATH", tmp_path / "llm_usage.json")
    monkeypatch.setattr(
        "rank.llm.make_llm_client",
        lambda _s: (_ for _ in ()).throw(RuntimeError("HTTP 429: Rate limit exceeded")),
    )
    with pytest.raises(SystemExit, match="429"):
        main.cmd_check_llm(argparse.Namespace(strict=True))
