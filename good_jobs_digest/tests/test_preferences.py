"""Tests for profile/preferences rendering."""

from pathlib import Path

from profile.preferences import build_scoring_input, digest_remote_only, load_preferences, render_scoring_context


def test_load_preferences_has_role_focus():
    prefs = load_preferences(Path(__file__).resolve().parents[1] / "profile" / "preferences.yaml")
    assert "role_focus" in prefs
    assert prefs["role_focus"]["primary"]


def test_render_includes_seniority():
    prefs = load_preferences(Path(__file__).resolve().parents[1] / "profile" / "preferences.yaml")
    text = render_scoring_context(prefs)
    assert "Seniority" in text
    assert "Role focus" in text


def test_build_scoring_input_non_empty():
    root = Path(__file__).resolve().parents[1]
    text = build_scoring_input(
        preferences_path=root / "profile" / "preferences.yaml",
        profile_path=root / "profile" / "profile.md",
    )
    assert "Structured requirements" in text


def test_digest_remote_only_from_preferences():
    prefs = load_preferences(Path(__file__).resolve().parents[1] / "profile" / "preferences.yaml")
    assert digest_remote_only(prefs, default=False) is True
    assert digest_remote_only({"digest": {"remote_only": False}}, default=True) is False
    assert digest_remote_only({}, default=True) is True


def test_null_values_omitted_from_prompt():
    prefs = load_preferences(Path(__file__).resolve().parents[1] / "profile" / "preferences.yaml")
    text = render_scoring_context(prefs)
    assert "Ideal role shape" not in text
    assert "Organization" not in text
    assert "Minimum annual gross" not in text
    assert "Neutral:" not in text
