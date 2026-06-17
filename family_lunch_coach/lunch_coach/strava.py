from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from lunch_coach.config import Settings
from lunch_coach.db import Database, utcnow


def fetch_activities(settings: Settings, db: Database, days: int = 14) -> list[dict]:
    cached = _read_cache(db)
    if cached and _cache_fresh(cached):
        return cached.get("activities", [])

    if not settings.bq_project:
        return []

    try:
        import pandas as pd
        import pandas_gbq
    except ImportError:
        return []

    q = f"""
    SELECT
      DATE(start_date) AS date,
      sport_type AS activity_type,
      CAST(moving_time / 60.0 AS FLOAT64) AS duration_minutes,
      CAST(distance / 1000.0 AS FLOAT64) AS distance_km,
      CAST(suffer_score AS FLOAT64) AS relative_effort
    FROM `{settings.bq_table_fqn}`
    WHERE DATE(start_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    ORDER BY start_date DESC
    """
    try:
        df = pandas_gbq.read_gbq(q, project_id=settings.bq_project)
        activities = df.to_dict(orient="records")
        for a in activities:
            if hasattr(a.get("date"), "isoformat"):
                a["date"] = a["date"].isoformat()
        _write_cache(db, activities)
        return activities
    except Exception:
        if cached:
            return cached.get("activities", [])
        return []


def _read_cache(db: Database) -> dict | None:
    with db.connect() as conn:
        row = conn.execute("SELECT fetched_at, data_json FROM activities_cache WHERE id=1").fetchone()
        if not row:
            return None
        try:
            return {"fetched_at": row["fetched_at"], "activities": json.loads(row["data_json"] or "[]")}
        except json.JSONDecodeError:
            return None


def _write_cache(db: Database, activities: list[dict]) -> None:
    with db.connect() as conn:
        conn.execute(
            """INSERT INTO activities_cache (id, fetched_at, data_json) VALUES (1, ?, ?)
               ON CONFLICT(id) DO UPDATE SET fetched_at=excluded.fetched_at, data_json=excluded.data_json""",
            (utcnow(), json.dumps(activities)),
        )


def _cache_fresh(cached: dict, max_age_hours: int = 6) -> bool:
    try:
        t = datetime.fromisoformat(cached["fetched_at"].replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - t < timedelta(hours=max_age_hours)
    except (ValueError, KeyError):
        return False


def training_summary(activities: list[dict]) -> tuple[str, bool]:
    if not activities:
        return "No recent training data.", False

    efforts = [a.get("relative_effort") or 0 for a in activities if a.get("relative_effort")]
    last7 = activities[:7]
    sum7 = sum(a.get("relative_effort") or 0 for a in last7)
    median = sorted(efforts)[len(efforts) // 2] if efforts else 0
    high_load = sum7 > median * 3 if median else False

    recent = activities[0]
    parts = [
        f"Last activity: {recent.get('activity_type', '?')} "
        f"{recent.get('duration_minutes', '?')}min on {recent.get('date', '?')}."
    ]
    parts.append(f"7-day effort sum: {sum7:.0f}. High load week: {high_load}.")
    return " ".join(parts), high_load


def recent_ride_finished(activities: list[dict], within_minutes: int = 90) -> dict | None:
    now = datetime.now(timezone.utc)
    for a in activities:
        date_str = a.get("date")
        if not date_str:
            continue
        # BQ date only — treat as today if date matches
        if date_str != now.date().isoformat():
            continue
        dur = a.get("duration_minutes") or 0
        if dur >= 20:
            return a
    return None
