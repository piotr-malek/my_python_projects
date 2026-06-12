import pandas as pd

from garmin.client import GarminClient
from util.json_util import to_json


class GarminEnricher:
    def __init__(self, settings, repo, bq, garmin=None):
        self._settings = settings
        self._repo = repo
        self._bq = bq
        self._garmin = garmin or GarminClient.get(settings)

    def enrich_pending(self, limit=20):
        q = f"""
        SELECT a.strava_activity_id, a.garmin_activity_id
        FROM {self._settings.table_id('activities')} a
        LEFT JOIN {self._settings.table_id('garmin_activity_enrichment')} e
          ON a.strava_activity_id = e.strava_activity_id
        WHERE a.garmin_activity_id IS NOT NULL
          AND e.strava_activity_id IS NULL
        LIMIT {limit}
        """
        try:
            pending = self._bq.load(q)
        except Exception:
            return 0
        if pending.empty:
            return 0

        rows = []
        for _, r in pending.iterrows():
            try:
                act = self._garmin.call(self._garmin.api.get_activity, str(r["garmin_activity_id"]))
                zones = {f"zone_{z}": act.get(f"hrTimeInZone_{z}") for z in range(6)}
                rows.append(
                    {
                        "strava_activity_id": int(r["strava_activity_id"]),
                        "aerobic_te": act.get("aerobicTrainingEffect"),
                        "anaerobic_te": act.get("anaerobicTrainingEffect"),
                        "activity_load": act.get("activityTrainingLoad"),
                        "hr_zones_json": to_json(zones),
                    }
                )
            except Exception:
                continue

        if rows:
            self._repo.merge("garmin_activity_enrichment", pd.DataFrame(rows), ["strava_activity_id"])
        return len(rows)
