from datetime import date

from llm.digest import DigestGenerator, generation_truncated, serialize_digest_payload


def _payload(health_state="yellow"):
    return {
        "health_state": health_state,
        "insights": [
            {
                "id": "sleep_debt_accumulating",
                "category": "sleep",
                "salience": 64,
                "confidence": "high",
                "summary": "You're 47m of sleep down across the last 7 days.",
            }
        ],
        "today_wellness": {
            "rhr_bpm": {
                "today": 49,
                "baseline": 47.7,
                "baseline_label": "30d",
                "delta": 1.3,
                "magnitude": "noise",
                "direction": "negative",
                "confidence": "high",
            },
            "sleep_minutes": {
                "today": 411,
                "today_hm": "6h51m",
                "baseline": 458,
                "baseline_hm": "7h38m",
                "baseline_label": "14d",
                "delta": -47,
                "delta_pm": "-47m",
                "unit": "minutes",
                "magnitude": "mild",
                "direction": "negative",
                "confidence": "high",
                "is_max_7d": False,
                "is_min_7d": False,
            },
            "deep_sleep_minutes": {
                "today": 36,
                "today_hm": "36m",
                "baseline": 64,
                "baseline_hm": "1h4m",
                "baseline_label": "14d",
                "delta": -28,
                "delta_pm": "-28m",
                "unit": "minutes",
                "magnitude": "significant",
                "direction": "negative",
                "confidence": "high",
                "is_max_7d": False,
                "is_min_7d": True,
            },
        },
        "garmin_status": {"hrv_status": "balanced"},
        "load": {"load_ratio": 1.17},
        "scores": {
            "recovery_score": {
                "today": 43,
                "yesterday": 54,
                "two_days_ago": 68,
                "range_7d": [43, 78],
                "typical_band": "mid",
                "trajectory": "declining",
            },
            "illness_probability_score": {
                "today": 22,
                "yesterday": 18,
                "two_days_ago": 15,
                "range_7d": [15, 28],
                "typical_band": "low",
                "direction_of_change": "stable",
            },
            "cognitive_readiness_score": {
                "today": 62,
                "yesterday": 68,
                "typical_band": "mid",
                "direction_of_change": "worsening",
            },
        },
        "patterns": [],
        "data_quality": {"garmin_status_reflects_partial_load": True, "hrv_source": "none"},
        "recent_themes_7d": ["sleep_repair", "maintenance"],
        "sleep_context": {
            "typical_bedtime": "23:45",
            "typical_wake": "07:20",
            "typical_duration_min": 460,
            "target_bedtime_tonight": "23:15",
        },
    }


def _llm_output(health_state="yellow"):
    return {
        "health_state": health_state,
        "headline": "Deep sleep was your weakest marker last night.",
        "day_outlook": "Focus should hold this morning, but deep sleep was short — batch hard thinking before midday.",
        "key_findings": [
            {
                "narrative": "You're 47m of sleep down across the last 7 days.",
                "category": "sleep",
                "based_on": "sleep_debt_accumulating",
            }
        ],
        "score_commentary": "Recovery 43 is down; illness probability 22 is stable; cognitive readiness 62 is easing.",
        "signals_today": [
            {
                "area": "sleep",
                "observation": "Deep sleep 36 vs 64 baseline.",
                "direction": "negative",
                "magnitude": "significant",
                "evidence_field": "today_wellness.deep_sleep_minutes",
                "evidence_value": 36,
                "trend_note": "lowest of week",
            },
            {
                "area": "sleep",
                "observation": "Sleep 411 vs 458 baseline.",
                "direction": "negative",
                "magnitude": "mild",
                "evidence_field": "today_wellness.sleep_minutes",
                "evidence_value": 411,
                "trend_note": None,
            },
        ],
        "actions_today": [
            {
                "theme": "sleep_repair",
                "steps": [
                    {"label": "Bedtime", "instruction": "Aim for 23:15 tonight."},
                    {"label": "Breathing", "instruction": "4-7-8 breathing, 5 cycles before bed."},
                ],
                "tied_to_signal_area": "sleep",
                "priority": "high",
                "why": "Sleep markers are down.",
            }
        ],
        "training_note": {
            "recommendation": "easy_optional",
            "rationale": "Mild sleep deficit; movement optional.",
            "context": "Last hard session was 2 days ago.",
        },
        "risk": [],
    }


def test_validate_accepts_well_formed_v4():
    out = DigestGenerator._validate(_llm_output(), _payload())
    assert out["health_state"] == "yellow"
    assert len(out["signals_today"]) == 2
    assert out["actions_today"][0]["theme"] == "sleep_repair"
    assert out["signals_today"][0]["observation"] == "deep sleep minutes 36m vs 1h4m (14d, -28m)."


def test_validate_drops_noise_signal_by_payload_magnitude(caplog):
    bad = _llm_output()
    bad["signals_today"].append(
        {
            "area": "cardio",
            "observation": "RHR slightly up.",
            "direction": "negative",
            "magnitude": "mild",
            "evidence_field": "today_wellness.rhr_bpm",
            "evidence_value": 49,
            "trend_note": None,
        }
    )
    with caplog.at_level("WARNING"):
        out = DigestGenerator._validate(bad, _payload())
    assert all(s["evidence_field"] != "today_wellness.rhr_bpm" for s in out["signals_today"])
    assert any("noise_canary" in rec.message for rec in caplog.records)


def test_validate_drops_nested_evidence_field(caplog):
    bad = _llm_output()
    bad["signals_today"].append(
        {
            "area": "sleep",
            "observation": "Sleep delta bad.",
            "direction": "negative",
            "magnitude": "mild",
            "evidence_field": "today_wellness.sleep_minutes.delta",
            "evidence_value": -47,
            "trend_note": None,
        }
    )
    with caplog.at_level("INFO"):
        out = DigestGenerator._validate(bad, _payload())
    assert all(s["evidence_field"] != "today_wellness.sleep_minutes.delta" for s in out["signals_today"])
    assert any("invalid evidence_field" in rec.message for rec in caplog.records)


def test_validate_drops_actions_with_wrong_tied_area_and_falls_back():
    bad = _llm_output()
    bad["actions_today"] = [
        {
            "theme": "stress_reset",
            "steps": [
                {"label": "Breathe", "instruction": "Coherent 5/5 for 10 min."},
                {"label": "Walk", "instruction": "Walk 10 min at midday."},
            ],
            "tied_to_signal_area": "stress",
            "priority": "normal",
            "why": "x",
        }
    ]
    out = DigestGenerator._validate(bad, _payload())
    assert out["actions_today"][0]["theme"] == "maintenance"


def test_validate_drops_generic_sleep_tracking_step():
    bad = _llm_output()
    bad["actions_today"] = [
        {
            "theme": "sleep_repair",
            "steps": [
                {"label": "Tracking", "instruction": "Use sleep tracking to identify patterns."},
                {"label": "Breathing", "instruction": "4-7-8 breathing, 5 cycles before bed."},
            ],
            "tied_to_signal_area": "sleep",
            "priority": "high",
            "why": "x",
        }
    ]
    out = DigestGenerator._validate(bad, _payload())
    assert out["actions_today"][0]["theme"] == "maintenance"


def test_render_markdown_includes_theme_sections_and_scores():
    md = DigestGenerator._render_markdown(date(2026, 5, 25), _llm_output())
    assert "## Today" in md
    assert "## What I'm seeing" in md
    assert "\n- " in md.split("## What I'm seeing")[1].split("## Scores")[0]
    assert "## Scores" in md
    assert "## What changed" in md
    assert "## Tonight's sleep" in md
    assert "## Training" in md
    assert "↓" in md


def test_render_markdown_red_state_renders_watch():
    red = _llm_output(health_state="red")
    red["risk"] = [{"type": "illness", "severity": "elevated", "why": "Strong RHR signal."}]
    md = DigestGenerator._render_markdown(date(2026, 5, 25), red)
    assert "🔴" in md
    assert "## Watch" in md


def test_render_training_ensures_punctuation_before_context():
    note = {
        "recommendation": "easy_only",
        "rationale": "Short sleep duration with improving but still low recovery score",
        "context": "Yesterday was rest",
    }
    rendered = DigestGenerator._render_training(note)
    assert "score.\n_Yesterday was rest_" in rendered


def test_fallback_json_passes_validation_v5():
    payload = _payload(health_state="red")
    payload["training_load_context"] = {
        "expected_fatigue_today": {"level": "moderate", "source_session": "Hard ride", "clears_by": "2026-06-06"},
        "pattern_note": "You trained 4 days in the last week with 4 hard sessions — a normal load week for you.",
    }
    out = DigestGenerator._fallback_json(payload)
    validated = DigestGenerator._validate(out, payload)
    assert validated["health_state"] == "red"
    assert len(validated["actions_today"]) >= 1
    assert validated["key_findings"]
    assert "recovery" in validated["score_commentary"].lower()
    assert "personalized generation" not in validated["training_note"]["rationale"].lower()
    assert "Hard ride" in validated["training_note"]["rationale"]


def test_serialize_digest_payload_is_compact():
    payload = {"date": "2026-06-05", "insights": [{"id": "x", "summary": "test"}]}
    s = serialize_digest_payload(payload)
    assert "\n" not in s
    assert '"id":"x"' in s


def test_generation_truncated_detects_length():
    assert generation_truncated({"done_reason": "length"})
    assert generation_truncated({"prompt_eval_count": 8134, "eval_count": 58, "num_ctx": 8192})
    assert not generation_truncated({"done_reason": "stop", "prompt_eval_count": 4000, "eval_count": 800, "num_ctx": 32768})
