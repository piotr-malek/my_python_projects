from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from lunch_coach.config import Settings


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.path = Path(settings.db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        schema_path = Path(__file__).resolve().parent.parent / "schema.sql"
        with open(schema_path) as f:
            sql = f.read()
        with self.connect() as conn:
            conn.executescript(sql)
            conn.execute(
                "INSERT OR IGNORE INTO onboarding_state (id, status) VALUES (1, 'not_started')"
            )

    def seed_if_empty(self) -> None:
        pkg = Path(__file__).resolve().parent.parent / "seeds"
        with self.connect() as conn:
            n = conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]
            if n == 0:
                with open(pkg / "recipes.json") as f:
                    recipes = json.load(f)
                today = datetime.now().date().isoformat()
                for i, r in enumerate(recipes, 1):
                    conn.execute(
                        """INSERT INTO recipes (
                            id, title, source_url, cuisine, nutrition_profile, estimated_minutes,
                            main_ingredients, batch_friendly, portions_yield, base_works_for_kids,
                            adult_upgrade, adult_upgrade_effort, is_shared_base_staple,
                            your_rating, discovery_source, added_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?)""",
                        (
                            i,
                            r["title"],
                            r.get("source_url", ""),
                            r.get("cuisine", ""),
                            r.get("nutrition_profile", "balanced"),
                            r.get("estimated_minutes", 20),
                            json.dumps(r.get("main_ingredients", [])),
                            int(r.get("batch_friendly", 0)),
                            r.get("portions_yield", 2),
                            int(r.get("base_works_for_kids", 0)),
                            r.get("adult_upgrade", ""),
                            r.get("adult_upgrade_effort", "table"),
                            int(r.get("is_shared_base_staple", 0)),
                            r.get("your_rating"),
                            today,
                        ),
                    )

            n = conn.execute("SELECT COUNT(*) FROM fallback_stack").fetchone()[0]
            if n == 0:
                with open(pkg / "fallback_stack.json") as f:
                    stack = json.load(f)
                for i, item in enumerate(stack, 1):
                    conn.execute(
                        """INSERT INTO fallback_stack (id, rank, title, instruction, needs_from_staples, kid_version)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            i,
                            item["rank"],
                            item["title"],
                            item["instruction"],
                            json.dumps(item["needs_from_staples"]),
                            item.get("kid_version", ""),
                        ),
                    )

            defaults = {
                "pantry_staples": json.dumps(
                    [
                        "olive oil",
                        "garlic",
                        "rice",
                        "pasta",
                        "canned chickpeas",
                        "canned tomatoes",
                        "eggs",
                        "soy sauce",
                    ]
                ),
                "diet": "plant-forward, occasional chicken and fish",
                "picky_eater_mode": "shortlist",
                "batch_sunday": "true",
                "cook_time_drift_min": "12",
            }
            for key, val in defaults.items():
                conn.execute(
                    """INSERT OR IGNORE INTO profile_cache (key, value, updated_at)
                       VALUES (?, ?, ?)""",
                    (key, val, utcnow()),
                )

    def get_profile(self, key: str, default: str = "") -> str:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM profile_cache WHERE key = ?", (key,)
            ).fetchone()
            return row["value"] if row else default

    def set_profile(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO profile_cache (key, value, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (key, value, utcnow()),
            )

    def get_meta(self, key: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO app_meta (key, value, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (key, value, utcnow()),
            )

    def row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        d = dict(row)
        for k in ("main_ingredients", "needs_from_staples"):
            if k in d and isinstance(d[k], str):
                try:
                    d[k] = json.loads(d[k])
                except json.JSONDecodeError:
                    pass
        return d

    def fetch_recipes(self, where: str = "1=1", params: tuple = ()) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM recipes WHERE {where}", params).fetchall()
            return [self.row_to_dict(r) for r in rows]

    def fetch_fallback_stack(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM fallback_stack ORDER BY rank").fetchall()
            return [self.row_to_dict(r) for r in rows]

    def food_log_today(self, meal: str = "lunch") -> bool:
        today = datetime.now().date().isoformat()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM food_log WHERE date = ? AND meal = ? LIMIT 1",
                (today, meal),
            ).fetchone()
            return row is not None

    def insert_food_log(
        self,
        meal: str,
        recipe_id: int | None = None,
        free_text: str = "",
        energy_note: str | None = None,
    ) -> None:
        today = datetime.now().date().isoformat()
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO food_log (date, meal, recipe_id, free_text, energy_note, logged_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (today, meal, recipe_id, free_text, energy_note, utcnow()),
            )

    def weekly_plan_exists(self, week_start: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM weekly_plans WHERE week_start = ? LIMIT 1", (week_start,)
            ).fetchone()
            return row is not None

    def get_food_log_recent(self, days: int = 21) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT fl.*, r.title, r.cuisine, r.main_ingredients
                   FROM food_log fl
                   LEFT JOIN recipes r ON fl.recipe_id = r.id
                   WHERE fl.date >= date('now', ?)
                   ORDER BY fl.date DESC""",
                (f"-{days} day",),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_nudge_by_key(self, occurrence_key: str) -> dict | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM nudge_log WHERE occurrence_key = ?", (occurrence_key,)
            ).fetchone()
            return dict(row) if row else None

    def upsert_nudge(
        self,
        nudge_type: str,
        occurrence_key: str,
        scheduled_at: str,
        status: str,
        was_delayed: int = 0,
        context_json: str = "{}",
    ) -> None:
        now = utcnow()
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM nudge_log WHERE occurrence_key = ?", (occurrence_key,)
            ).fetchone()
            if existing:
                conn.execute(
                    """UPDATE nudge_log SET sent_at=?, status=?, was_delayed=?, context_json=?
                       WHERE occurrence_key=?""",
                    (now, status, was_delayed, context_json, occurrence_key),
                )
            else:
                conn.execute(
                    """INSERT INTO nudge_log
                       (nudge_type, occurrence_key, scheduled_at, sent_at, was_delayed, status, context_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (nudge_type, occurrence_key, scheduled_at, now, was_delayed, status, context_json),
                )

    def mark_nudge_responded(self, occurrence_key: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """UPDATE nudge_log SET user_responded=1, status='responded'
                   WHERE occurrence_key=?""",
                (occurrence_key,),
            )

    def get_onboarding(self) -> dict:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM onboarding_state WHERE id = 1").fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO onboarding_state (id, status) VALUES (1, 'not_started')"
                )
                return {"status": "not_started", "answers_json": "{}", "skip_count": 0}
            d = dict(row)
            return d

    def update_onboarding(self, **fields: Any) -> None:
        allowed = {
            "status",
            "current_step",
            "answers_json",
            "skip_count",
            "last_nudge_at",
            "completed_at",
        }
        parts = []
        vals: list[Any] = []
        for k, v in fields.items():
            if k in allowed:
                parts.append(f"{k} = ?")
                vals.append(v)
        if not parts:
            return
        with self.connect() as conn:
            conn.execute(
                f"UPDATE onboarding_state SET {', '.join(parts)} WHERE id = 1", vals
            )

    def clear_family_members(self) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM family_members")

    def insert_family_member(self, member: dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO family_members
                   (role, age_years, age_band, allergies_json, accepts_json, rejects_json, notes)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    member.get("role", "child"),
                    member.get("age_years"),
                    member.get("age_band"),
                    json.dumps(member.get("allergies", [])),
                    json.dumps(member.get("accepts", [])),
                    json.dumps(member.get("rejects", [])),
                    member.get("notes", ""),
                ),
            )

    def get_family_members(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM family_members").fetchall()
            out = []
            for r in rows:
                d = dict(r)
                for k in ("allergies_json", "accepts_json", "rejects_json"):
                    d[k.replace("_json", "")] = json.loads(d.pop(k) or "[]")
                out.append(d)
            return out

    def update_recipe_rating(
        self,
        recipe_id: int,
        rating: float,
        would_repeat: bool | None = None,
        kid_reaction: str | None = None,
    ) -> None:
        today = datetime.now().date().isoformat()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
            if not row:
                return
            times = (row["times_cooked"] or 0) + 1
            notes = row["kid_acceptance_notes"] or ""
            if kid_reaction:
                notes = f"{notes}; {kid_reaction}".strip("; ")
            wr = int(would_repeat) if would_repeat is not None else row["would_repeat"]
            conn.execute(
                """UPDATE recipes SET your_rating=?, would_repeat=?, times_cooked=?,
                   last_cooked_date=?, kid_acceptance_notes=? WHERE id=?""",
                (rating, wr, times, today, notes, recipe_id),
            )
            # shared-base rule
            r = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
            if (
                r["your_rating"]
                and r["your_rating"] >= 4
                and r["base_works_for_kids"]
                and r["adult_upgrade_effort"] == "table"
                and times >= 2
            ):
                conn.execute(
                    "UPDATE recipes SET is_shared_base_staple=1 WHERE id=?", (recipe_id,)
                )

    def find_recipe_fuzzy(self, ref: str) -> dict | None:
        ref_lower = ref.lower()
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM recipes").fetchall()
            for r in rows:
                d = self.row_to_dict(r)
                if ref_lower in (d.get("title") or "").lower():
                    return d
                if ref_lower in (d.get("source_url") or "").lower():
                    return d
            return None
