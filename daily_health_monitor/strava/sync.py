import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from strava import transforms
from strava.client import StravaClient


class StravaSync:
    def __init__(self, settings, repo, client=None):
        self._settings = settings
        self._repo = repo
        self._client = client or StravaClient.get(settings)
        self.stats = {}

    def _reset_stats(self):
        self.stats = {
            "listed": 0,
            "skipped_existing": 0,
            "fetched_detail": 0,
            "skipped_streams": 0,
            "fetched_streams": 0,
        }

    def _after_epoch(self):
        val = self._repo.get_sync_state("strava_last_sync")
        if val:
            return int(val)
        return int(
            (datetime.now(timezone.utc) - timedelta(days=self._settings.STRAVA_LOOKBACK_DAYS)).timestamp()
        )

    def _set_after_epoch(self, epoch):
        self._repo.set_sync_state("strava_last_sync", str(epoch))

    @staticmethod
    def _parse_start_ts(start):
        return int(datetime.fromisoformat(start.replace("Z", "+00:00")).timestamp())

    def _sync_activity(self, aid, fetch_streams, existing_ids, has_streams):
        self.stats["listed"] += 1
        if aid in existing_ids:
            self.stats["skipped_existing"] += 1
            activity_row = None
        else:
            detail = self._client.get_activity(aid)
            activity_row = transforms.activity_to_row(detail)
            self.stats["fetched_detail"] += 1
            time.sleep(0.3)

        stream_row = None
        if fetch_streams:
            if aid in has_streams:
                self.stats["skipped_streams"] += 1
            else:
                try:
                    stream_row = transforms.streams_to_row(aid, self._client.get_streams(aid))
                    self.stats["fetched_streams"] += 1
                    time.sleep(0.3)
                except Exception:
                    pass
        return activity_row, stream_row

    def sync_incremental(self, fetch_streams=True):
        self._reset_stats()
        days = self._settings.ANALYSIS_DAYS
        existing_ids = self._repo.get_activity_ids_in_window(days)
        has_streams = self._repo.get_activity_ids_with_streams(days)

        after = self._after_epoch()
        all_acts = []
        page = 1
        while True:
            batch = self._client.list_activities(after=after, page=page)
            if not batch:
                break
            all_acts.extend(batch)
            if len(batch) < 200:
                break
            page += 1
            time.sleep(0.5)

        if not all_acts:
            return 0

        max_start = after
        activity_rows = []
        stream_rows = []

        for act in all_acts:
            aid = int(act["id"])
            start = act.get("start_date")
            if start:
                try:
                    max_start = max(max_start, self._parse_start_ts(start))
                except ValueError:
                    pass
            row, stream_row = self._sync_activity(aid, fetch_streams, existing_ids, has_streams)
            if row:
                activity_rows.append(row)
            if stream_row:
                stream_rows.append(stream_row)
                has_streams.add(aid)

        if activity_rows:
            self._repo.merge("activities", pd.DataFrame(activity_rows), ["strava_activity_id"])
        if stream_rows:
            self._repo.merge("activity_streams", pd.DataFrame(stream_rows), ["strava_activity_id"])

        self._set_after_epoch(max_start + 1)
        return len(activity_rows)

    def backfill_days(self, days=None, max_activities=None):
        self._reset_stats()
        days = days or self._settings.ANALYSIS_DAYS
        self._repo.set_sync_state("strava_backfill_page", "")
        existing_ids = self._repo.get_activity_ids_in_window(days)
        has_streams = self._repo.get_activity_ids_with_streams(days)
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        count = 0
        page = 1

        while True:
            batch = self._client.list_activities(page=page)
            if not batch:
                self._repo.set_sync_state("strava_backfill_page", "")
                break

            stop = False
            activity_rows = []
            stream_rows = []

            for act in batch:
                start = act.get("start_date")
                if start:
                    try:
                        if datetime.fromisoformat(start.replace("Z", "+00:00")) < cutoff:
                            stop = True
                            break
                    except ValueError:
                        pass

                aid = int(act["id"])
                row, stream_row = self._sync_activity(aid, True, existing_ids, has_streams)
                if row:
                    activity_rows.append(row)
                    existing_ids.add(aid)
                    count += 1
                if stream_row:
                    stream_rows.append(stream_row)
                    has_streams.add(aid)

                if max_activities and count >= max_activities:
                    stop = True
                    break

            if activity_rows:
                self._repo.merge("activities", pd.DataFrame(activity_rows), ["strava_activity_id"])
            if stream_rows:
                self._repo.merge("activity_streams", pd.DataFrame(stream_rows), ["strava_activity_id"])

            self._repo.set_sync_state("strava_backfill_page", str(page + 1))
            page += 1

            if stop or len(batch) < 200:
                self._repo.set_sync_state("strava_backfill_page", "")
                break

        return count
