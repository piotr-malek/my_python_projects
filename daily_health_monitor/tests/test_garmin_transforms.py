from datetime import datetime, timezone

from garmin.transforms import ms_local_to_iso, ms_to_iso


def test_ms_local_to_iso_decodes_garmin_wall_clock_as_utc():
    # Friday 00:51 local encoded by Garmin as 2026-06-05 00:51:00 UTC epoch ms.
    ms = int(datetime(2026, 6, 5, 0, 51, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert ms_local_to_iso(ms) == "2026-06-05T00:51:00"


def test_ms_to_iso_uses_system_local_timezone():
    ms = int(datetime(2026, 6, 5, 0, 51, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert ms_to_iso(ms) == datetime.fromtimestamp(ms / 1000).isoformat()
