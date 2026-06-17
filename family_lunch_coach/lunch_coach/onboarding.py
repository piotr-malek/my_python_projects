from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

from lunch_coach.config import Settings
from lunch_coach.db import Database, utcnow
from lunch_coach.ollama_client import OllamaClient
from lunch_coach.profile import apply_profile_from_answers, get_family_profile_summary

SKIP_PATTERNS = re.compile(
    r"\b(skip setup|skip onboarding|just help|just decide|help me with lunch)\b",
    re.I,
)

ONBOARDING_EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "has_kids": {"type": "boolean"},
        "family_members": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "role": {"type": "string"},
                    "age_years": {"type": ["number", "null"]},
                    "age_band": {"type": ["string", "null"]},
                    "allergies": {"type": "array", "items": {"type": "string"}},
                    "accepts": {"type": "array", "items": {"type": "string"}},
                    "rejects": {"type": "array", "items": {"type": "string"}},
                    "notes": {"type": "string"},
                },
            },
        },
        "diet": {"type": ["string", "null"]},
        "pantry_staples": {"type": "array", "items": {"type": "string"}},
        "batch_sunday": {"type": ["boolean", "null"]},
        "picky_eater_mode": {"type": ["string", "null"]},
        "allergies_household": {"type": "array", "items": {"type": "string"}},
        "next_step": {"type": "string"},
        "profile_complete": {"type": "boolean"},
    },
    "required": ["profile_complete", "next_step"],
}

EXTRACT_SYSTEM = """Extract structured household lunch profile facts from the user's message.
Merge with existing partial profile. Never invent allergies or ages not stated.
Set profile_complete=true only when you have: household composition, diet, pantry staples,
and kid food preferences (accepts/rejects) OR confirmed no kids.
Set next_step to the single best follow-up question topic if incomplete: household|allergies|accepts|rejects|diet|pantry|batch|done.
Age bands: infant (<1), toddler (1-3), child (4-11), teen (12-17), adult (18+)."""


STEPS = {
    "household": "Who am I cooking lunch for — just you, or kids too? How many and what ages?",
    "allergies": "Any food allergies or intolerances for anyone in the house?",
    "accepts": "What do the kids (or you) reliably eat and enjoy at lunch?",
    "rejects": "Anything they consistently reject — textures, spices, visible veg?",
    "diet": "Any diet preferences for you — plant-forward, meat/fish ok, etc.?",
    "pantry": "What's usually in your pantry? (e.g. rice, pasta, eggs, canned tomatoes, olive oil)",
    "batch": "Do you batch-cook on Sundays for the week?",
}


def is_skip_message(message: str) -> bool:
    return bool(SKIP_PATTERNS.search(message))


def _merge_answers(existing: dict, extracted: dict) -> dict:
    merged = {**existing}
    for k, v in extracted.items():
        if k in ("next_step", "profile_complete"):
            continue
        if v is None:
            continue
        if k == "family_members" and v:
            merged["family_members"] = v
        elif isinstance(v, list) and v:
            merged[k] = v
        elif isinstance(v, (str, bool, int, float)) and v != "":
            merged[k] = v
    return merged


def _pick_next_question(answers: dict) -> str:
    members = answers.get("family_members") or []
    has_kids = answers.get("has_kids")
    if has_kids is None and not members:
        return STEPS["household"]
    if has_kids or members:
        all_allergies = answers.get("allergies_household") or []
        if not all_allergies and not any(m.get("allergies") for m in members):
            if "allergies" not in answers.get("_asked", []):
                return STEPS["allergies"]
        if not any(m.get("accepts") for m in members) and not answers.get("accepts"):
            return STEPS["accepts"]
        if not any(m.get("rejects") for m in members) and not answers.get("rejects"):
            return STEPS["rejects"]
    if not answers.get("diet"):
        return STEPS["diet"]
    if not answers.get("pantry_staples"):
        return STEPS["pantry"]
    if answers.get("batch_sunday") is None:
        return STEPS["batch"]
    return ""


def _core_complete(answers: dict) -> bool:
    if not answers.get("diet"):
        return False
    if not answers.get("pantry_staples"):
        return False
    has_kids = answers.get("has_kids")
    members = answers.get("family_members") or []
    if has_kids is None and not members:
        return False
    if has_kids or members:
        has_accepts = any(m.get("accepts") for m in members) or answers.get("accepts")
        has_rejects = any(m.get("rejects") for m in members) or answers.get("rejects")
        if not (has_accepts and has_rejects):
            return False
    return True


class OnboardingFSM:
    def __init__(self, db: Database, settings: Settings, llm: OllamaClient):
        self.db = db
        self.settings = settings
        self.llm = llm

    def intro_message(self) -> str:
        return (
            "Hi — I'm your lunch coach. I'll learn your household so suggestions "
            "fit from day one (one kid vs three infants with allergies = very different plans).\n\n"
            + STEPS["household"]
        )

    def should_handle(self, message: str) -> bool:
        state = self.db.get_onboarding()
        status = state.get("status", "not_started")
        if status == "complete":
            return False
        if is_skip_message(message):
            return False
        return status in ("not_started", "in_progress", "skipped")

    def handle(self, message: str) -> str | None:
        state = self.db.get_onboarding()
        status = state.get("status", "not_started")

        if status == "not_started":
            self.db.update_onboarding(status="in_progress", current_step="household")
            if message.strip().lower() in ("/start", "start", "hi", "hello", "hey"):
                return self.intro_message()
            # fall through to process first real answer

        try:
            existing = json.loads(state.get("answers_json") or "{}")
        except json.JSONDecodeError:
            existing = {}

        user_prompt = (
            f"Existing profile: {json.dumps(existing)}\n"
            f"User message: {message}\n"
            "Extract and merge profile facts."
        )
        try:
            extracted = self.llm.chat_json(
                EXTRACT_SYSTEM, user_prompt, ONBOARDING_EXTRACT_SCHEMA, thinking=False
            )
        except RuntimeError:
            extracted = {"profile_complete": False, "next_step": "household"}

        answers = _merge_answers(existing, extracted)
        complete = extracted.get("profile_complete") or _core_complete(answers)

        self.db.update_onboarding(
            status="in_progress",
            answers_json=json.dumps(answers),
            current_step=extracted.get("next_step", ""),
        )

        if complete:
            apply_profile_from_answers(self.db, answers, self.settings)
            self.db.update_onboarding(
                status="complete",
                completed_at=utcnow(),
                current_step="done",
            )
            from lunch_coach.handler import handle_coach_message

            return (
                "Got it — I've saved your household profile. "
                "Here's a first personalized suggestion:\n\n"
                + handle_coach_message(
                    self.db, self.settings, self.llm, "what should I cook for lunch today?"
                )
            )

        q = _pick_next_question(answers)
        if not q:
            apply_profile_from_answers(self.db, answers, self.settings)
            self.db.update_onboarding(status="complete", completed_at=utcnow())
            return "Profile saved. Ask me anytime what to cook for lunch."

        asked = answers.get("_asked", [])
        topic = extracted.get("next_step", "followup")
        if topic not in asked:
            asked.append(topic)
            answers["_asked"] = asked
            self.db.update_onboarding(answers_json=json.dumps(answers))

        return q + "\n\n(Say 'skip setup' anytime — I'll still help with lunch.)"

    def skip(self) -> None:
        state = self.db.get_onboarding()
        skip_count = (state.get("skip_count") or 0) + 1
        self.db.update_onboarding(status="skipped", skip_count=skip_count)

    def maybe_renudge(self) -> str | None:
        state = self.db.get_onboarding()
        status = state.get("status", "not_started")
        if status == "complete":
            return None
        if (state.get("skip_count") or 0) >= 3:
            return None
        last = state.get("last_nudge_at")
        if last:
            try:
                t = datetime.fromisoformat(last.replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - t < timedelta(hours=24):
                    return None
            except ValueError:
                pass
        prompts = [
            "Quick one — any allergies I should know about?",
            "Who's usually eating lunch with you — just you or kids too?",
            "What's always in your pantry — rice, pasta, eggs?",
        ]
        idx = (state.get("skip_count") or 0) % len(prompts)
        self.db.update_onboarding(last_nudge_at=utcnow())
        return prompts[idx]
