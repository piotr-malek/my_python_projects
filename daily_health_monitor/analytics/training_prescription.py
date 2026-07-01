"""Deterministic training clearance and prescription hints for the daily digest."""

from __future__ import annotations

from typing import Any, Dict, Optional

REC_RANK = {
    "rest": 0,
    "easy_optional": 1,
    "easy_only": 2,
    "normal": 3,
    "hard_ok": 4,
}

VALID_SESSION_TYPES = frozenset(
    {"endurance_z2", "tempo", "intervals", "recovery_spin", "rest"}
)
VALID_DURATION_HOURS = frozenset({"1.5", "2-3", "3+"})


def _has_illness_watch(payload: dict) -> bool:
    return any(
        isinstance(i, dict) and i.get("id") == "illness_watch"
        for i in (payload.get("insights") or [])
    )


def _hr_cap(threshold_hr: Optional[int], offset: int) -> str:
    if threshold_hr:
        return f"HR cap {int(threshold_hr) - offset} bpm"
    return "steady aerobic pace"


def assess_training_clearance(payload: dict) -> Optional[Dict[str, Any]]:
    """
    When recovery and load say the athlete is cleared, return a floor recommendation
    plus structured prescription fields. Returns None when no uplift applies.
    """
    state = payload.get("health_state")
    if state != "green" or _has_illness_watch(payload):
        return None

    tlc = payload.get("training_load_context") or {}
    ef = tlc.get("expected_fatigue_today") or {}
    ef_level = ef.get("level") or "none"
    if ef_level not in ("none", None):
        return None

    days_since_hard = tlc.get("days_since_hard")
    if days_since_hard is not None and days_since_hard < 3:
        return None

    scores = payload.get("scores") or {}
    rec_band = (scores.get("recovery_score") or {}).get("typical_band")
    cog_band = (scores.get("cognitive_readiness_score") or {}).get("typical_band")
    if rec_band not in ("high", "mid"):
        return None

    load = payload.get("load") or {}
    load_ratio = load.get("load_ratio") or 0
    hard_days_7d = tlc.get("hard_days_7d") or 0
    threshold_hr = (payload.get("user_context") or {}).get("threshold_hr_bpm")

    if load_ratio >= 1.3:
        min_rec = "normal"
    elif rec_band == "high" and cog_band in ("high", "mid"):
        min_rec = "hard_ok"
    else:
        min_rec = "normal"

    if min_rec == "hard_ok":
        if hard_days_7d >= 2:
            session_type = "tempo"
            duration_hours = "1.5"
            intensity_guidance = f"Tempo Z3 — {_hr_cap(threshold_hr, 10)}"
        elif days_since_hard is not None and days_since_hard >= 4:
            session_type = "endurance_z2"
            duration_hours = "2-3"
            intensity_guidance = f"Long endurance Z2 — {_hr_cap(threshold_hr, 15)}"
        else:
            session_type = "endurance_z2"
            duration_hours = "2-3"
            intensity_guidance = f"Endurance Z2 — {_hr_cap(threshold_hr, 15)}"
    else:
        session_type = "endurance_z2"
        duration_hours = "1.5"
        intensity_guidance = f"Moderate endurance — {_hr_cap(threshold_hr, 15)}"

    return {
        "min_recommendation": min_rec,
        "session_type": session_type,
        "duration_hours": duration_hours,
        "intensity_guidance": intensity_guidance,
    }


def apply_training_clearance(note: dict, payload: dict) -> dict:
    """Uplift conservative LLM training notes when clearance gates pass."""
    clearance = assess_training_clearance(payload)
    rec = note.get("recommendation")
    if rec not in REC_RANK:
        rec = "easy_optional"

    out = {
        "recommendation": rec,
        "rationale": note.get("rationale", "") if isinstance(note.get("rationale"), str) else "",
        "context": note.get("context", "") if isinstance(note.get("context"), str) else "",
    }

    if clearance:
        min_rec = clearance["min_recommendation"]
        if REC_RANK.get(rec, 0) < REC_RANK.get(min_rec, 0):
            out["recommendation"] = min_rec

    for field in ("session_type", "duration_hours", "intensity_guidance"):
        val = note.get(field)
        if field == "session_type" and val not in VALID_SESSION_TYPES:
            val = clearance.get(field) if clearance else None
        elif field == "duration_hours" and val not in VALID_DURATION_HOURS:
            val = clearance.get(field) if clearance else None
        elif field == "intensity_guidance":
            if not isinstance(val, str) or not val.strip():
                val = (clearance or {}).get(field) or ""
        if val:
            out[field] = val

    return out
