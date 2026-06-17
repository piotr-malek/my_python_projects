from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from lunch_coach.config import Settings
from lunch_coach.db import Database
from lunch_coach.onboarding import OnboardingFSM, is_skip_message, _core_complete, _pick_next_question
from lunch_coach.context import recipe_has_allergen, load_match, pick_fallback
from lunch_coach.intent import select_mode
from lunch_coach.nudges import _occurrence_key, nudge_still_relevant
from lunch_coach.ollama_client import OllamaClient


class TestOnboarding(unittest.TestCase):
    def test_skip_detection(self):
        self.assertTrue(is_skip_message("skip setup please"))
        self.assertTrue(is_skip_message("just decide for me"))
        self.assertFalse(is_skip_message("pasta for lunch"))

    def test_core_complete(self):
        self.assertFalse(_core_complete({"diet": "vegan"}))
        complete = {
            "has_kids": False,
            "diet": "plant-forward",
            "pantry_staples": ["rice", "eggs"],
        }
        self.assertTrue(_core_complete(complete))

    def test_pick_next_question(self):
        q = _pick_next_question({"has_kids": True, "family_members": [{"role": "child"}]})
        self.assertIn("allerg", q.lower())


class TestRanking(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=str(Path(self.tmp.name) / "test.db"))
        self.db = Database(self.settings)
        self.db.init_schema()
        self.db.seed_if_empty()

    def tearDown(self):
        self.tmp.cleanup()

    def test_allergen_filter(self):
        recipe = {"main_ingredients": ["peanut butter", "noodles"]}
        self.assertTrue(recipe_has_allergen(recipe, ["peanut"]))

    def test_load_match(self):
        self.assertEqual(load_match("carb_heavy", True), 1.0)
        self.assertEqual(load_match("light", True), 0.3)
        self.assertEqual(load_match("light", False), 1.0)

    def test_fallback_picks_staples(self):
        text = pick_fallback(self.db, False)
        self.assertIn("egg", text.lower())


class TestIntent(unittest.TestCase):
    def test_decisive_mode(self):
        self.assertEqual(select_mode({"intent": "fallback"}), "decisive")
        self.assertEqual(
            select_mode({"intent": "cooking_now", "energy_self_report": "low"}), "decisive"
        )
        self.assertEqual(select_mode({"intent": "cooking_now"}), "shortlist")


class TestNudges(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(db_path=str(Path(self.tmp.name) / "test.db"))
        self.db = Database(self.settings)
        self.db.init_schema()

    def tearDown(self):
        self.tmp.cleanup()

    def test_occurrence_key(self):
        from datetime import datetime
        k = _occurrence_key("lunch_reminder", datetime(2026, 6, 17))
        self.assertEqual(k, "lunch_reminder:2026-06-17")

    def test_lunch_reminder_relevant_without_log(self):
        from datetime import datetime
        self.assertTrue(
            nudge_still_relevant(self.db, self.settings, "lunch_reminder", datetime.now())
        )


if __name__ == "__main__":
    unittest.main()
