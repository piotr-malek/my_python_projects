"""Tests for profile/preferences rendering.

These read `preferences.example.yaml`, not the personal `preferences.yaml`: the
latter is gitignored, so it does not exist on a fresh checkout or in CI.
"""

from pathlib import Path

from profile.preferences import build_scoring_input, digest_remote_only, load_preferences, render_scoring_context

ROOT = Path(__file__).resolve().parents[1]
PREFS_PATH = ROOT / "profile" / "preferences.example.yaml"
PROFILE_PATH = ROOT / "profile" / "profile.example.md"


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
