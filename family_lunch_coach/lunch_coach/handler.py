from __future__ import annotations

from lunch_coach.config import Settings
from lunch_coach.context import (
    assemble_context,
    handle_log_food,
    handle_rating,
    handle_shopping,
    pick_fallback,
)
from lunch_coach.db import Database
from lunch_coach.intent import classify_intent, select_mode
from lunch_coach.ollama_client import OllamaClient
from lunch_coach.profile import get_family_profile_summary
from lunch_coach.response import generate_response


def handle_coach_message(
    db: Database,
    settings: Settings,
    llm: OllamaClient,
    message: str,
) -> str:
    intent = classify_intent(llm, message)
    it = intent.get("intent", "other")
    mode = select_mode(intent)
    family_profile = get_family_profile_summary(db)

    if it == "fallback":
        family_meal = intent.get("family_meal") or db.get_profile("has_kids") == "true"
        return pick_fallback(db, family_meal)

    if it == "rating":
        return handle_rating(db, intent)

    if it == "log_food":
        return handle_log_food(db, intent, message)

    if it == "shopping":
        family_meal = intent.get("family_meal") or db.get_profile("has_kids") == "true"
        return handle_shopping(db, settings, family_meal)

    if it == "discover_recipe":
        target = intent.get("dish_to_discover") or "something new"
        return f"Researching {target} — I'll message you when I have something good. (Discovery pipeline coming in phase 2.)"

    if it in ("cooking_now", "planning", "reflection", "other"):
        if it == "other" and not any(
            w in message.lower() for w in ("cook", "lunch", "eat", "food", "recipe", "meal")
        ):
            return "I'm your lunch coach — ask what to cook, plan the week, or say 'just something easy'."
        candidates, train_str, gaps_str, family_meal = assemble_context(
            db, settings, intent, mode
        )
        if it == "fallback" or (not candidates and mode == "decisive"):
            return pick_fallback(db, family_meal)
        return generate_response(
            llm,
            mode,
            family_meal,
            train_str,
            gaps_str,
            candidates,
            message,
            intent.get("time_available_minutes"),
            intent.get("days_to_cover"),
            family_profile,
        )

    return "Not sure how to help with that — try 'what should I cook for lunch?'"
