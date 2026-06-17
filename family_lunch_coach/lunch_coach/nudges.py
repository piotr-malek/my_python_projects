from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from lunch_coach.config import Settings
from lunch_coach.db import Database, utcnow
from lunch_coach.strava import fetch_activities, recent_ride_finished

NUDGE_DEFS = {
    "sunday_planning": {
        "cron": "0 13 * * 0",
        "window_hours": 8,
        "message": "Hey — it's Sunday. Want to sort the week? Tell me how long you've got to cook and I'll plan lunches.",
    },
    "lunch_reminder": {
        "cron": "30 12 * * 1-5",
        "window_hours": 2,
        "message": "Lunch? Tell me what's in the fridge or how long you've got — or say 'just decide' and I'll pick.",
    },
    "friday_reflection": {
        "cron": "0 18 * * 5",
        "window_hours": 12,
        "message": "End of week — how did eating go? Tell me how your energy was and I'll show you what the data says.",
    },
}


def _week_start(d: datetime) -> str:
    monday = d - timedelta(days=d.weekday())
    return monday.date().isoformat()


def _occurrence_key(nudge_type: str, when: datetime) -> str:
    return f"{nudge_type}:{when.date().isoformat()}"


def _scheduled_today(nudge_type: str, now: datetime) -> datetime | None:
    if nudge_type == "lunch_reminder" and now.weekday() < 5:
        return now.replace(hour=12, minute=30, second=0, microsecond=0)
    if nudge_type == "sunday_planning" and now.weekday() == 6:
        return now.replace(hour=13, minute=0, second=0, microsecond=0)
    if nudge_type == "friday_reflection" and now.weekday() == 4:
        return now.replace(hour=18, minute=0, second=0, microsecond=0)
    return None


def nudge_still_relevant(db: Database, settings: Settings, nudge_type: str, now: datetime) -> bool:
    if nudge_type == "lunch_reminder":
        return not db.food_log_today("lunch")
    if nudge_type == "sunday_planning":
        return not db.weekly_plan_exists(_week_start(now))
    if nudge_type == "friday_reflection":
        logs = db.get_food_log_recent(7)
        return len(logs) >= 3
    if nudge_type == "post_ride":
        activities = fetch_activities(settings, db)
        ride = recent_ride_finished(activities)
        if not ride:
            return False
        return not db.food_log_today("lunch")
    return True


def nudge_message(db: Database, settings: Settings, nudge_type: str) -> str:
    base = NUDGE_DEFS.get(nudge_type, {}).get("message", "Lunch check-in?")
    members = db.get_family_members()
    infants = sum(1 for m in members if m.get("age_band") in ("infant", "toddler"))
    if infants and nudge_type == "lunch_reminder":
        return f"Lunch for you and the little ones? {base}"
    if nudge_type == "post_ride":
        activities = fetch_activities(settings, db)
        ride = recent_ride_finished(activities)
        if ride:
            return (
                f"You finished a {ride.get('duration_minutes', '?')}min "
                f"{ride.get('activity_type', 'ride')}. Eat in the next ~30 min — "
                "tell me what you've got."
            )
    return base


def evaluate_nudge(db: Database, settings: Settings, nudge_type: str) -> str | None:
    now = datetime.now()
    scheduled = _scheduled_today(nudge_type, now)
    if not scheduled:
        return None
    if not nudge_still_relevant(db, settings, nudge_type, now):
        return None

    key = _occurrence_key(nudge_type, now)
    existing = db.get_nudge_by_key(key)
    if existing and existing.get("user_responded"):
        return None
    if existing and existing.get("status") in ("sent", "reasked"):
        return None

    window = NUDGE_DEFS.get(nudge_type, {}).get("window_hours", 2)
    if now > scheduled + timedelta(hours=window):
        if not existing:
            db.upsert_nudge(nudge_type, key, scheduled.isoformat(), "missed")
        return None

    delayed = now > scheduled
    db.upsert_nudge(
        nudge_type, key, scheduled.isoformat(), "sent", was_delayed=int(delayed)
    )
    return nudge_message(db, settings, nudge_type)


def reconcile_missed(db: Database, settings: Settings) -> list[str]:
    now = datetime.now()
    out: list[str] = []
    for nudge_type, meta in NUDGE_DEFS.items():
        scheduled = _scheduled_today(nudge_type, now)
        if not scheduled:
            continue
        key = _occurrence_key(nudge_type, now)
        window = meta.get("window_hours", 2)
        deadline = scheduled + timedelta(hours=window)
        if now > deadline:
            existing = db.get_nudge_by_key(key)
            if not existing and nudge_still_relevant(db, settings, nudge_type, now):
                db.upsert_nudge(nudge_type, key, scheduled.isoformat(), "missed")
            continue

        existing = db.get_nudge_by_key(key)
        if existing:
            if existing.get("user_responded"):
                continue
            if existing.get("status") in ("sent", "reasked"):
                if now <= deadline:
                    sent = existing.get("sent_at", "")
                    out.append(
                        f"Pinged you earlier — {nudge_message(db, settings, nudge_type)}"
                    )
                    db.upsert_nudge(
                        nudge_type, key, scheduled.isoformat(), "reasked",
                        context_json=json.dumps({"reask": True}),
                    )
            continue

        if nudge_still_relevant(db, settings, nudge_type, now):
            msg = evaluate_nudge(db, settings, nudge_type)
            if msg:
                out.append(msg)

    # post_ride via heartbeat
    if nudge_still_relevant(db, settings, "post_ride", now):
        key = f"post_ride:{now.date().isoformat()}"
        if not db.get_nudge_by_key(key):
            msg = nudge_message(db, settings, "post_ride")
            db.upsert_nudge("post_ride", key, utcnow(), "sent")
            out.append(msg)

    return out


def run_nudge(db: Database, settings: Settings, nudge_type: str) -> str:
    msg = evaluate_nudge(db, settings, nudge_type)
    return msg or "NO_REPLY"
