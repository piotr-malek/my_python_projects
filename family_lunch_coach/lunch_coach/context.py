from __future__ import annotations

import json
from datetime import datetime, timedelta

from lunch_coach.config import Settings
from lunch_coach.db import Database
from lunch_coach.profile import get_household_allergies, get_pantry_staples
from lunch_coach.strava import fetch_activities, training_summary


def compute_rotation_gaps(db: Database) -> tuple[list[str], list[str]]:
    logs = db.get_food_log_recent(21)
    seen_cuisines: set[str] = set()
    seen_ingredients: set[str] = set()
    for entry in logs:
        if entry.get("cuisine"):
            seen_cuisines.add(entry["cuisine"].lower())
        raw = entry.get("main_ingredients")
        if raw:
            try:
                for ing in json.loads(raw):
                    seen_ingredients.add(ing.lower())
            except (json.JSONDecodeError, TypeError):
                pass

    all_recipes = db.fetch_recipes()
    gap_cuisines = []
    gap_ingredients = []
    for r in all_recipes:
        c = (r.get("cuisine") or "").lower()
        if c and c not in seen_cuisines:
            gap_cuisines.append(c)
        for ing in r.get("main_ingredients") or []:
            if ing.lower() not in seen_ingredients:
                gap_ingredients.append(ing.lower())
    return list(set(gap_cuisines))[:5], list(set(gap_ingredients))[:8]


def recipe_has_allergen(recipe: dict, allergies: list[str]) -> bool:
    if not allergies:
        return False
    ings = [i.lower() for i in (recipe.get("main_ingredients") or [])]
    for a in allergies:
        al = a.lower()
        if any(al in ing or ing in al for ing in ings):
            return True
    return False


def load_match(nutrition: str, high_load: bool) -> float:
    if high_load:
        return 1.0 if nutrition in ("carb_heavy", "balanced", "protein_heavy") else 0.3
    return 1.0 if nutrition in ("light", "balanced") else 0.5


def rank_recipes(
    db: Database,
    settings: Settings,
    family_meal: bool,
    time_available: int | None,
    days_to_cover: int | None,
    high_load: bool,
    gap_cuisines: list[str],
    gap_ingredients: list[str],
    limit: int = 3,
) -> list[dict]:
    allergies = get_household_allergies(db)
    buffer_min = int(db.get_profile("cook_time_drift_min", str(settings.cook_time_buffer_min)))
    w = settings.ranking_weights

    recipes = db.fetch_recipes(
        "(your_rating >= 3.5 OR your_rating IS NULL) AND "
        "(last_cooked_date IS NULL OR last_cooked_date < date('now','-14 day'))"
    )

    filtered = []
    for r in recipes:
        if recipe_has_allergen(r, allergies):
            continue
        if days_to_cover and days_to_cover > 2 and not r.get("batch_friendly"):
            continue
        if time_available is not None:
            est = (r.get("estimated_minutes") or 30) + buffer_min
            if est > time_available:
                continue
        if family_meal:
            if not r.get("base_works_for_kids"):
                continue
            if r.get("adult_upgrade_effort") not in ("table", "separate"):
                continue
        filtered.append(r)

    scored = []
    for r in filtered:
        cuisine = (r.get("cuisine") or "").lower()
        gap_score = 0.0
        if cuisine in gap_cuisines:
            gap_score += 0.6
        ings = [i.lower() for i in (r.get("main_ingredients") or [])]
        gap_score += min(1.0, sum(1 for g in gap_ingredients if g in ings) * 0.2)

        rating = r.get("your_rating")
        rating_score = (rating / 5.0) if rating else 0.7
        load = load_match(r.get("nutrition_profile") or "balanced", high_load)
        novelty = 0.3 if (r.get("times_cooked") or 0) < 2 else 0.1
        if r.get("is_shared_base_staple") and family_meal:
            novelty += 0.15

        score = (
            w["w_gap"] * gap_score
            + w["w_rating"] * rating_score
            + w["w_load"] * load
            + w["w_novelty"] * novelty
        )
        scored.append((score, r))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:limit]]


def pick_fallback(db: Database, family_meal: bool) -> str:
    pantry = {p.lower() for p in get_pantry_staples(db)}
    for item in db.fetch_fallback_stack():
        needs = [n.lower() for n in (item.get("needs_from_staples") or [])]
        if all(n in pantry or any(n in p for p in pantry) for n in needs):
            text = item["instruction"]
            if family_meal and item.get("kid_version"):
                text += f"\n\nKids: {item['kid_version']}. One cook."
            return text
    item = db.fetch_fallback_stack()[0]
    return item["instruction"]


def assemble_context(
    db: Database,
    settings: Settings,
    intent: dict,
    mode: str,
) -> tuple[list[dict], str, str, bool]:
    activities = fetch_activities(settings, db)
    train_str, high_load = training_summary(activities)
    gap_c, gap_i = compute_rotation_gaps(db)
    gaps_str = f"Cuisine gaps: {', '.join(gap_c) or 'none'}. Ingredient gaps: {', '.join(gap_i) or 'none'}."

    family_meal = intent.get("family_meal") or db.get_profile("has_kids") == "true"
    limit = 1 if mode == "decisive" else 3
    candidates = rank_recipes(
        db,
        settings,
        family_meal,
        intent.get("time_available_minutes"),
        intent.get("days_to_cover"),
        high_load,
        gap_c,
        gap_i,
        limit=limit,
    )
    return candidates, train_str, gaps_str, family_meal


def handle_rating(db: Database, intent: dict) -> str:
    ref = intent.get("recipe_reference") or ""
    rating = intent.get("recipe_rating")
    if not ref or rating is None:
        return "Tell me which recipe and rating (e.g. 'miso pasta 4/5')."
    recipe = db.find_recipe_fuzzy(ref)
    if not recipe:
        return f"Couldn't find '{ref}' in your corpus."
    db.update_recipe_rating(
        recipe["id"], float(rating), kid_reaction=intent.get("kid_reaction")
    )
    return f"Logged {rating}/5 for {recipe['title']}. Thanks — this helps the rotation."


def handle_log_food(db: Database, intent: dict, message: str) -> str:
    recipe = db.find_recipe_fuzzy(message) if message else None
    db.insert_food_log(
        "lunch",
        recipe_id=recipe["id"] if recipe else None,
        free_text=message if not recipe else recipe["title"],
        energy_note=intent.get("energy_self_report"),
    )
    today = datetime.now().date().isoformat()
    key = f"lunch_reminder:{today}"
    if db.get_nudge_by_key(key):
        db.mark_nudge_responded(key)
    name = recipe["title"] if recipe else message[:60]
    return f"Logged lunch: {name}."


def handle_shopping(db: Database, settings: Settings, family_meal: bool) -> str:
    gap_c, gap_i = compute_rotation_gaps(db)
    candidates = rank_recipes(
        db, settings, family_meal, None, None, False, gap_c, gap_i, limit=3
    )
    pantry = {p.lower() for p in get_pantry_staples(db)}
    needed: set[str] = set()
    for r in candidates:
        for ing in r.get("main_ingredients") or []:
            if ing.lower() not in pantry:
                needed.add(ing)
    if not needed:
        return "Pantry looks stocked for the top rotation picks. Maybe just fresh veg or protein."
    lines = ["Shopping list for this week's lunches:"]
    for ing in sorted(needed):
        lines.append(f"- {ing}")
    return "\n".join(lines)
