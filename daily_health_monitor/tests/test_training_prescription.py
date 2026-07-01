from llm.digest import DigestGenerator
from analytics.training_prescription import apply_training_clearance, assess_training_clearance


def _cleared_payload():
    return {
        "health_state": "green",
        "insights": [{"id": "stress_high", "category": "stress"}],
        "scores": {
            "recovery_score": {"typical_band": "high", "direction_of_change": "improving"},
            "cognitive_readiness_score": {"typical_band": "high", "direction_of_change": "stable"},
        },
        "training_load_context": {
            "days_since_hard": 4,
            "hard_days_7d": 1,
            "expected_fatigue_today": {"level": "none"},
        },
        "load": {"load_ratio": 1.05},
        "user_context": {"threshold_hr_bpm": 170},
    }


def test_assess_clearance_high_recovery_four_days_since_hard():
    out = assess_training_clearance(_cleared_payload())
    assert out is not None
    assert out["min_recommendation"] == "hard_ok"
    assert out["session_type"] == "endurance_z2"
    assert out["duration_hours"] == "2-3"
    assert "155" in out["intensity_guidance"]


def test_assess_clearance_none_when_expected_fatigue():
    payload = _cleared_payload()
    payload["training_load_context"]["expected_fatigue_today"] = {"level": "moderate"}
    assert assess_training_clearance(payload) is None


def test_assess_clearance_none_when_days_since_hard_under_three():
    payload = _cleared_payload()
    payload["training_load_context"]["days_since_hard"] = 2
    assert assess_training_clearance(payload) is None


def test_apply_clearance_uplifts_easy_optional_to_hard_ok():
    note = {
        "recommendation": "easy_optional",
        "rationale": "Stress was high yesterday.",
        "context": "Last hard session 4 days ago.",
    }
    out = apply_training_clearance(note, _cleared_payload())
    assert out["recommendation"] == "hard_ok"
    assert out["session_type"] == "endurance_z2"
    assert out["duration_hours"] == "2-3"


def test_validate_training_uplifts_conservative_llm_output():
    payload = _cleared_payload()
    llm = {
        "health_state": "green",
        "headline": "Recovery strong.",
        "day_outlook": "Good day ahead.",
        "key_findings": [
            {"narrative": "Recovery high.", "category": "recovery", "based_on": "x"}
        ],
        "score_commentary": "Recovery is high. Cognitive readiness is high.",
        "signals_today": [],
        "actions_today": [
            {
                "theme": "maintenance",
                "steps": [{"label": "Walk", "instruction": "20 min walk before 10:00."}],
                "tied_to_signal_area": "maintenance",
                "priority": "normal",
                "why": "x",
            }
        ],
        "training_note": {
            "recommendation": "easy_optional",
            "rationale": "Stress was elevated.",
            "context": "TSS 212 five days ago.",
        },
        "risk": [],
    }
    out = DigestGenerator._validate(llm, payload)
    assert out["training_note"]["recommendation"] == "hard_ok"
    assert out["training_note"]["duration_hours"] == "2-3"


def test_render_training_includes_prescription():
    note = {
        "recommendation": "hard_ok",
        "rationale": "Recovery supports a solid session.",
        "context": "Last hard ride 4 days ago.",
        "session_type": "endurance_z2",
        "duration_hours": "2-3",
        "intensity_guidance": "Long endurance Z2 — HR cap 155 bpm",
    }
    rendered = DigestGenerator._render_training(note)
    assert "Good day for quality work" in rendered
    assert "2–3h endurance Z2" in rendered
    assert "155 bpm" in rendered
    assert "_Last hard ride 4 days ago._" in rendered
