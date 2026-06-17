from __future__ import annotations

INTENT_SCHEMA = {
    "type": "object",
    "properties": {
        "intent": {
            "type": "string",
            "enum": [
                "planning",
                "cooking_now",
                "fallback",
                "shopping",
                "rating",
                "log_food",
                "reflection",
                "add_recipe",
                "discover_recipe",
                "other",
            ],
        },
        "decisiveness_signal": {
            "type": "string",
            "enum": ["wants_options", "wants_decision", "unspecified"],
        },
        "family_meal": {"type": "boolean"},
        "time_available_minutes": {"type": ["integer", "null"]},
        "days_to_cover": {"type": ["integer", "null"]},
        "ingredients_mentioned": {"type": "array", "items": {"type": "string"}},
        "energy_self_report": {"type": ["string", "null"]},
        "recipe_rating": {"type": ["number", "null"]},
        "recipe_reference": {"type": ["string", "null"]},
        "kid_reaction": {"type": ["string", "null"]},
        "dish_to_discover": {"type": ["string", "null"]},
        "free_notes": {"type": ["string", "null"]},
    },
    "required": ["intent", "decisiveness_signal", "family_meal", "ingredients_mentioned"],
}

INTENT_SYSTEM = """You are the intent parser for a family lunch coach on Telegram.
Read the user's message and return ONLY a JSON object matching the provided schema.
No prose, no markdown, no code fences.

Field rules:
- intent: pick the single best fit.
  * fallback     = wants something effortless right now, low/no energy.
  * cooking_now  = wants to cook/eat now and is open to ideas.
  * planning     = planning ahead, the week, or a batch cook.
  * discover_recipe = wants to find something NEW.
  * rating       = rating a recipe just cooked.
  * log_food     = stating what they ate.
  * add_recipe   = manually adding a recipe to the corpus.
  * shopping     = wants a shopping list.
  * reflection   = wants to review how the week/eating went.
  * other        = general chat or a profile fact to remember.
- decisiveness_signal: wants_options | wants_decision | unspecified
- energy_self_report: low/normal/high if stated, else null.
- family_meal: true if they mention kids, family, everyone, or feeding others.
Be literal. Do not infer facts that are not present."""


def classify_intent(llm, message: str) -> dict:
    try:
        return llm.chat_json(INTENT_SYSTEM, message, INTENT_SCHEMA, thinking=False)
    except RuntimeError:
        return {
            "intent": "other",
            "decisiveness_signal": "unspecified",
            "family_meal": False,
            "ingredients_mentioned": [],
        }


def select_mode(intent: dict) -> str:
    if intent.get("intent") == "fallback":
        return "decisive"
    if intent.get("decisiveness_signal") == "wants_decision":
        return "decisive"
    if intent.get("energy_self_report") == "low":
        return "decisive"
    return "shortlist"
