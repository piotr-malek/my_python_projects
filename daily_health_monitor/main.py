import argparse
import sys
import traceback
from datetime import date

from config import settings
from pipeline.runner import Pipeline


def parse_args():
    p = argparse.ArgumentParser(description="Health monitoring daily pipeline")
    p.add_argument("--date", help="Target date YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true", help="Skip email; print digest")
    p.add_argument("--skip-llm", action="store_true", help="Skip Ollama")
    p.add_argument("--skip-ingest", action="store_true", help="Skip API ingest (analyze only)")
    p.add_argument("--garmin-backfill", type=int, metavar="DAYS", help="Backfill Garmin wellness")
    p.add_argument(
        "--strava-backfill",
        action="store_true",
        help="Backfill Strava activities for ANALYSIS_DAYS (see .env)",
    )
    p.add_argument("--init-bq", action="store_true", help="Create BQ dataset and tables")
    p.add_argument("--materialize-history", action="store_true", help="Rebuild wellness_daily_complete from raw tables")
    return p.parse_args()


def main():
    args = parse_args()
    pipeline = Pipeline()

    if args.init_bq:
        pipeline.init_bigquery()
        print("BigQuery schema initialized.")
        return 0

    target = date.fromisoformat(args.date) if args.date else date.today()

    if args.garmin_backfill:
        pipeline.ingest_garmin(target=target, backfill_days=args.garmin_backfill)
        print(f"Garmin backfill complete ({args.garmin_backfill} days).")
        return 0

    if args.materialize_history:
        from jobs.materialize_history import run_materialize_history
        n = run_materialize_history(pipeline._repo, target=target, window_days=settings.ANALYSIS_DAYS)
        print(f"Materialized {n} days into wellness_daily_complete.")
        from jobs.weekly_insights import run_weekly_insights
        wf = run_weekly_insights(target, window_days=settings.ANALYSIS_DAYS)
        print(f"Weekly insights refreshed ({wf} findings).")
        return 0

    if args.strava_backfill:
        n = pipeline.sync_strava(backfill=True)
        print(f"Strava backfill synced {n} activities (last {settings.ANALYSIS_DAYS} days).")
        return 0

    try:
        return pipeline.run_daily(
            target=target,
            skip_ingest=args.skip_ingest,
            skip_llm=args.skip_llm,
            dry_run=args.dry_run,
        )
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
