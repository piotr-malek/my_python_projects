from datetime import date, timedelta

from config import settings
from garmin.wellness import WellnessFetcher
from llm.digest import DigestGenerator
from mail.mailer import DigestMailer
from pipeline.analyzer import Analyzer
from storage import bq, repo
from strava.enricher import GarminEnricher
from strava.sync import StravaSync
from util.timing import RunLog


class Pipeline:
    def __init__(self):
        self._settings = settings
        self._repo = repo
        self._wellness = WellnessFetcher(settings)
        self._strava = StravaSync(settings, repo)
        self._enricher = GarminEnricher(settings, repo, bq)
        self._analyzer = Analyzer(settings, repo)
        self._digest = DigestGenerator(settings, repo)
        self._mailer = DigestMailer(settings)

    def init_bigquery(self):
        bq.init_schema()

    def ingest_garmin(self, target=None, backfill_days=0, log=None):
        end = target or date.today()
        start = end - timedelta(days=backfill_days - 1 if backfill_days > 0 else 6)
        if backfill_days > 0:
            skip_dates = set()
        else:
            # Re-fetch recent days (sleep can arrive late) and any day missing sleep.
            skip_dates = self._repo.wellness_dates_complete(start, end)
            refresh_cutoff = end - timedelta(days=1)
            skip_dates = {d for d in skip_dates if d < refresh_cutoff}
        tables = self._wellness.fetch_range(start, end, skip_dates=skip_dates)
        for table_name, df in tables.items():
            if not df.empty:
                self._repo.replace_dates(table_name, df)
        print(
            f"  Garmin: {self._wellness.stats['fetched_days']} days fetched, "
            f"{self._wellness.stats['skipped_days']} skipped (already in BQ)"
        )

    def sync_strava(self, log, backfill=False):
        if backfill:
            n = self._strava.backfill_days()
        else:
            n = self._strava.sync_incremental(fetch_streams=True)
            self._enricher.enrich_pending(limit=50)
        s = self._strava.stats
        print(
            f"  Strava: {n} new activities merged | listed={s.get('listed', 0)} "
            f"skip_existing={s.get('skipped_existing', 0)} "
            f"detail_fetched={s.get('fetched_detail', 0)} "
            f"streams_skip={s.get('skipped_streams', 0)} "
            f"streams_fetched={s.get('fetched_streams', 0)}"
        )
        return n

    def run_daily(self, target=None, skip_ingest=False, skip_llm=False, dry_run=False):
        target = target or date.today()
        log = RunLog()
        self._settings.LOG_DIR.mkdir(parents=True, exist_ok=True)

        log.step("pipeline log (BQ)")
        self._repo.log_pipeline_run(target, "running")

        try:
            if not skip_ingest:
                log.step("Garmin wellness ingest")
                self.ingest_garmin(target=target)

                try:
                    from jobs.materialize_history import run_materialize_history
                    n = run_materialize_history(self._repo, target=target)
                    print(f"  Materialized {n} days into wellness_daily_complete")
                except Exception as exc:
                    print(f"  Materialize history skipped: {exc}")

                if date.today().weekday() == 0:
                    try:
                        from jobs.weekly_insights import run_weekly_insights
                        run_weekly_insights(target)
                    except Exception as exc:
                        print(f"  Weekly insights skipped: {exc}")

                log.step("Strava sync")
                self.sync_strava(log)

            log.step("analytics")
            analysis = self._analyzer.run(target)
            a = self._analyzer.stats
            print(
                f"  {a.get('activities', 0)} activities | "
                f"metrics computed={a.get('metrics_computed', 0)} "
                f"skipped={a.get('metrics_skipped', 0)}"
            )

            if skip_llm:
                digest = "(LLM skipped)"
                log.step("LLM digest (skipped)")
            else:
                log.step(f"LLM digest ({self._settings.OLLAMA_MODEL})")
                digest = self._digest.generate(analysis, target)

            if dry_run:
                log.step("output")
                print("\n" + digest + "\n")
            else:
                log.step("email")
                self._mailer.send(digest, analysis, target)

            log.step("pipeline log (success)")
            self._repo.log_pipeline_run(target, "success")
            log.finish()
            return 0
        except Exception as e:
            self._repo.log_pipeline_run(target, "failed", error=str(e))
            log.finish()
            raise
