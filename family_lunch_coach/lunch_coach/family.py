"""Shared-base and kid preference rules."""

from lunch_coach.db import Database


def apply_kid_downrank(recipes: list[dict], db: Database) -> list[dict]:
    """Down-rank recipes with repeated kid rejections in acceptance notes."""
    scored = []
    for r in recipes:
        notes = (r.get("kid_acceptance_notes") or "").lower()
        penalty = 0.0
        if "reject" in notes:
            penalty += 0.3
        if notes.count("reject") >= 2:
            penalty += 0.5
        scored.append((penalty, r))
    scored.sort(key=lambda x: x[0])
    return [r for _, r in scored]
