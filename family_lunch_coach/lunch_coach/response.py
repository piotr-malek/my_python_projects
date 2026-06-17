from __future__ import annotations

import json
from datetime import datetime

from lunch_coach.ollama_client import OllamaClient
from lunch_coach.profile import get_family_profile_summary

RESPONSE_SYSTEM = """You are a personal family lunch and recovery coach on Telegram.
You know this person's training history, recipe corpus, family constraints, and behavioural patterns.

HARD RULES:
- NEVER invent, generate, or suggest any recipe that is not in the provided candidate list. Retrieval only.
- Respond strictly in the mode given by MODE:
  * shortlist: present 2-3 of the provided candidates as options. For EACH: the dish name plus ONE concrete reason it fits right now. End by asking which they want. Never more than 3.
  * decisive: pick the single best candidate. Give ONE clear instruction. No options. No questions back.
- If FAMILY_MEAL is true, for each dish state: (a) what the kids eat, (b) the adult upgrade, (c) ONE cook — name the effort level.
- Always ground "why now" in the concrete data provided. Never be generic.
- Under 150 words. Plain text. No markdown headers, no bullet characters."""


def generate_response(
    llm: OllamaClient,
    mode: str,
    family_meal: bool,
    training_summary: str,
    rotation_gaps: str,
    candidates: list[dict],
    user_message: str,
    time_available: int | None = None,
    days_to_cover: int | None = None,
    family_profile: str = "",
) -> str:
    if not candidates:
        return (
            "Nothing in the corpus fits right now — try the fallback stack "
            "or say 'just something easy'."
        )

    slim = []
    for c in candidates:
        slim.append(
            {
                "title": c.get("title"),
                "cuisine": c.get("cuisine"),
                "estimated_minutes": c.get("estimated_minutes"),
                "nutrition_profile": c.get("nutrition_profile"),
                "kid_version": c.get("kid_acceptance_notes") or "mild base portion",
                "adult_upgrade": c.get("adult_upgrade"),
                "adult_upgrade_effort": c.get("adult_upgrade_effort"),
                "base_works_for_kids": c.get("base_works_for_kids"),
            }
        )

    user = (
        f"MODE: {mode}\n"
        f"FAMILY_MEAL: {family_meal}\n"
        f"FAMILY_PROFILE:\n{family_profile}\n"
        f"TODAY: {datetime.now().date().isoformat()}\n"
        f"TRAINING_SUMMARY: {training_summary}\n"
        f"ROTATION_GAPS: {rotation_gaps}\n"
        f"TIME_AVAILABLE_MIN: {time_available}\n"
        f"DAYS_TO_COVER: {days_to_cover}\n"
        f"CANDIDATES:\n{json.dumps(slim, indent=2)}\n"
        f"USER_MESSAGE: {user_message}"
    )
    return llm.chat_text(RESPONSE_SYSTEM, user, thinking=False)
