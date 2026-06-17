from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from lunch_coach.config import Settings
from lunch_coach.db import Database


DEFAULT_PANTRY = [
    "olive oil",
    "garlic",
    "rice",
    "pasta",
    "canned chickpeas",
    "canned tomatoes",
    "eggs",
    "soy sauce",
]


def get_pantry_staples(db: Database) -> list[str]:
    raw = db.get_profile("pantry_staples", "[]")
    try:
        items = json.loads(raw)
        return items if items else DEFAULT_PANTRY
    except json.JSONDecodeError:
        return DEFAULT_PANTRY


def get_household_allergies(db: Database) -> list[str]:
    raw = db.get_profile("allergies_household", "[]")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def get_family_profile_summary(db: Database) -> str:
    members = db.get_family_members()
    if not members:
        kids = db.get_profile("has_kids", "unknown")
        diet = db.get_profile("diet", "")
        return f"Household: kids={kids}. Diet: {diet}."
    lines = []
    for m in members:
        band = m.get("age_band") or m.get("role", "")
        age = m.get("age_years")
        age_str = f"{age}yo" if age else band
        allergies = ", ".join(m.get("allergies", [])) or "none"
        accepts = ", ".join(m.get("accepts", [])[:5]) or "unspecified"
        rejects = ", ".join(m.get("rejects", [])[:5]) or "unspecified"
        lines.append(
            f"{m.get('role', 'member')} ({age_str}): allergies={allergies}; "
            f"accepts={accepts}; rejects={rejects}"
        )
    diet = db.get_profile("diet", "")
    pantry = ", ".join(get_pantry_staples(db)[:8])
    return "Family:\n" + "\n".join(lines) + f"\nDiet: {diet}\nPantry: {pantry}"


def sync_user_md(db: Database, settings: Settings) -> None:
    path = Path(settings.user_md_path)
    if not path.parent.exists():
        return
    summary = get_family_profile_summary(db)
    diet = db.get_profile("diet", "")
    pantry = ", ".join(get_pantry_staples(db))
    block = f"""## Lunch Coach Profile (auto-synced {datetime.now().date().isoformat()})

{summary}

- Diet: {diet}
- Pantry staples: {pantry}
- Picky eater mode: {db.get_profile('picky_eater_mode', 'shortlist')}
- Batch Sunday: {db.get_profile('batch_sunday', 'true')}
- Cook time drift: +{db.get_profile('cook_time_drift_min', '12')} min
"""
    if path.is_file():
        text = path.read_text()
        marker = "## Lunch Coach Profile"
        if marker in text:
            before = text.split(marker)[0].rstrip()
            path.write_text(before + "\n\n" + block.strip() + "\n")
            return
        path.write_text(text.rstrip() + "\n\n" + block.strip() + "\n")
    else:
        path.write_text("# USER.md\n\n" + block.strip() + "\n")


def apply_profile_from_answers(db: Database, answers: dict, settings: Settings) -> None:
    if answers.get("diet"):
        db.set_profile("diet", answers["diet"])
    if answers.get("pantry_staples"):
        db.set_profile("pantry_staples", json.dumps(answers["pantry_staples"]))
    if answers.get("picky_eater_mode"):
        db.set_profile("picky_eater_mode", answers["picky_eater_mode"])
    if answers.get("batch_sunday") is not None:
        db.set_profile("batch_sunday", "true" if answers["batch_sunday"] else "false")
    if answers.get("cook_time_drift_min") is not None:
        db.set_profile("cook_time_drift_min", str(answers["cook_time_drift_min"]))
    if answers.get("has_kids") is not None:
        db.set_profile("has_kids", "true" if answers["has_kids"] else "false")
    allergies: list[str] = answers.get("allergies_household") or []
    if answers.get("family_members"):
        db.clear_family_members()
        for m in answers["family_members"]:
            db.insert_family_member(m)
            allergies.extend(m.get("allergies") or [])
    if allergies:
        db.set_profile("allergies_household", json.dumps(sorted(set(allergies))))
    sync_user_md(db, settings)
