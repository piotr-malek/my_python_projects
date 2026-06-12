from datetime import datetime, timezone

from util.json_util import to_json


def ms_to_iso(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000).isoformat()


def ms_local_to_iso(ms):
    """Decode Garmin *TimestampLocal fields (wall-clock time stored as UTC epoch ms)."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None).isoformat()


def extract_profile_id(training_status):
    m = training_status.get("mostRecentTrainingLoadBalance") or {}
    dto_map = m.get("metricsTrainingLoadBalanceDTOMap") or {}
    if not dto_map:
        return None
    return next(iter(dto_map.keys()))
