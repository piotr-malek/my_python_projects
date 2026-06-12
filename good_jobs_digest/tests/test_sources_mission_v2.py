"""Tests for mission-v2 org discovery parsers."""

import json
from pathlib import Path

from discovery.resolve import EmployerCandidate
from discovery.sources_mission_v2 import (
    _load_scrape_checkpoint,
    _parse_ea_funds_grantees,
    append_candidate_v2,
    bootstrap_scrape_checkpoint_from_jsonl,
    candidate_from_dict,
    candidate_to_dict,
    load_candidates_v2,
    reset_scrape_checkpoint,
)


def test_parse_ea_funds_grantees():
    html = "<p><strong>Crustacean Compassion ($137,000):</strong> welfare work</p>"
    names = _parse_ea_funds_grantees(html)
    assert "Crustacean Compassion" in names


def test_candidate_roundtrip():
    cand = EmployerCandidate(
        company_name="Rethink Priorities",
        mission_category="xrisk",
        website="https://rethinkpriorities.org",
        discovery_source="coefficient+sff",
        ats_hint=("greenhouse", "rethinkpriorities"),
        extra_slugs=["rp"],
    )
    raw = candidate_to_dict(cand)
    restored = candidate_from_dict(raw)
    assert restored.company_name == cand.company_name
    assert restored.ats_hint == cand.ats_hint
    assert restored.extra_slugs == cand.extra_slugs


def test_load_candidates_merges_append_only_jsonl(tmp_path: Path):
    path = tmp_path / "candidates_v2.jsonl"
    append_candidate_v2(
        EmployerCandidate(company_name="Rethink Priorities", discovery_source="coefficient"),
        path,
    )
    append_candidate_v2(
        EmployerCandidate(company_name="Rethink Priorities", discovery_source="sff"),
        path,
    )
    loaded = load_candidates_v2(path)
    assert len(loaded) == 1
    assert loaded[0].discovery_source == "coefficient+sff"


def test_bootstrap_scrape_checkpoint_from_jsonl(tmp_path: Path):
    jsonl = tmp_path / "orgs.jsonl"
    ckpt = tmp_path / "scrape.json"
    jsonl.write_text(
        json.dumps({"company_name": "GiveWell", "discovery_source": "givewell"}) + "\n",
        encoding="utf-8",
    )
    assert bootstrap_scrape_checkpoint_from_jsonl(
        candidates_path=jsonl,
        scrape_checkpoint_path=ckpt,
        sources=["givewell", "sff"],
    )
    state = _load_scrape_checkpoint(ckpt)
    assert state["sources"]["givewell"]["status"] == "completed"
    assert state["sources"]["sff"]["status"] == "completed"
    reset_scrape_checkpoint(ckpt)
    assert not ckpt.exists()
