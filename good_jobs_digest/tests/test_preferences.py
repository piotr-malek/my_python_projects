"""Tests for profile/preferences rendering.

Two different things are being checked here:

* The *renderer* is tested against `preferences.example.yaml`. It asserts on
  specific content ("Ideal role shape" must be absent, etc.), so pinning it to a
  fixture keeps it stable — and the personal file is gitignored, so it doesn't
  exist on a fresh checkout or in CI.
* The *real* `preferences.yaml` — the one that actually drives scoring — is
  validated by `test_real_preferences_are_usable` below, which skips when the
  file isn't there rather than failing.
"""

from pathlib import Path

import pytest

from profile.preferences import build_scoring_input, digest_remote_only, load_preferences, render_scoring_context

ROOT = Path(__file__).resolve().parents[1]
PREFS_PATH = ROOT / "profile" / "preferences.example.yaml"
PROFILE_PATH = ROOT / "profile" / "profile.example.md"
REAL_PREFS_PATH = ROOT / "profile" / "preferences.yaml"


def test_example_preferences_exist():
    """Guards the whole module: everything below is meaningless without this file."""
    assert PREFS_PATH.is_file(), f"missing {PREFS_PATH}"


def test_load_preferences_has_role_focus():
    prefs = load_preferences(PREFS_PATH)
    assert "role_focus" in prefs
    assert prefs["role_focus"]["primary"]


def test_render_includes_seniority():
    text = render_scoring_context(load_preferences(PREFS_PATH))
    assert "Seniority" in text
    assert "Role focus" in text


def test_build_scoring_input_non_empty():
    text = build_scoring_input(preferences_path=PREFS_PATH, profile_path=PROFILE_PATH)
    assert "Structured requirements" in text


def test_digest_remote_only_from_preferences():
    prefs = load_preferences(PREFS_PATH)
    assert digest_remote_only(prefs, default=False) is True
    assert digest_remote_only({"digest": {"remote_only": False}}, default=True) is False
    assert digest_remote_only({}, default=True) is True


def test_null_values_omitted_from_prompt():
    text = render_scoring_context(load_preferences(PREFS_PATH))
    assert "Ideal role shape" not in text
    assert "Organization" not in text
    assert "Minimum annual gross" not in text
    assert "Neutral:" not in text


@pytest.mark.skipif(
    not REAL_PREFS_PATH.is_file(),
    reason="personal preferences.yaml is gitignored; absent on a fresh checkout/CI",
)
def test_real_preferences_are_usable():
    """Validate the file that actually drives scoring, when it's present.

    Catches a malformed or half-edited preferences.yaml locally, before a run
    produces a silently degraded prompt.
    """
    prefs = load_preferences(REAL_PREFS_PATH)
    assert prefs, "preferences.yaml parsed to nothing — check the YAML"

    # The scorer prompt is built from these; missing ones weaken every score.
    for section in ("role_focus", "seniority", "location", "work_arrangement"):
        assert section in prefs, f"preferences.yaml is missing '{section}'"

    assert prefs["role_focus"].get("primary"), "role_focus.primary is empty"
    assert prefs["seniority"].get("target"), "seniority.target is empty"

    text = build_scoring_input(preferences_path=REAL_PREFS_PATH, profile_path=None)
    assert "Structured requirements" in text
    for expected in ("Seniority", "Role focus", "Location & timezone"):
        assert expected in text, f"'{expected}' missing from the rendered prompt"
