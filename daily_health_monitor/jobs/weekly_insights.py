"""Weekly refresh of correlation-based insight cache (v5.2 — reads wellness_daily_complete)."""

from datetime import date

from analytics.correlation_engine import run_weekly_correlations
from analytics.insight_detectors import _finding_dict
from config import settings
from storage import repo


def run_weekly_insights(target=None, window_days=90):
    target = target or date.today()
    complete = repo.load_wellness_daily_complete(window_days)
    activities = repo.load_activities_for_analysis(window_days)
    findings, diagnostics = run_weekly_correlations(complete, activities, target)
    payload = [_finding_dict(f) for f in findings]
    if payload:
        repo.save_insight_cache(target, payload, window_days=window_days)
    if diagnostics:
        repo.save_insight_diagnostics(target, [d.to_row(target) for d in diagnostics])
    return len(payload)


if __name__ == "__main__":
    n = run_weekly_insights()
    print(f"Weekly insight cache refreshed ({n} findings).")
