from util.json_util import to_json


def parse_garmin_activity_id(external_id):
    if not external_id:
        return None
    if external_id.startswith("garmin_ping_"):
        return external_id.split("_")[-1]
    return None


def activity_to_row(act):
    ext = act.get("external_id")
    return {
        "strava_activity_id": int(act["id"]),
        "name": act.get("name"),
        "sport_type": act.get("sport_type") or act.get("type"),
        "start_date": act.get("start_date"),
        "start_date_local": act.get("start_date_local"),
        "moving_time": act.get("moving_time"),
        "elapsed_time": act.get("elapsed_time"),
        "distance": act.get("distance"),
        "elevation_gain": act.get("total_elevation_gain"),
        "avg_hr": act.get("average_heartrate"),
        "max_hr": act.get("max_heartrate"),
        "avg_speed": act.get("average_speed"),
        "max_speed": act.get("max_speed"),
        "avg_cadence": act.get("average_cadence"),
        "avg_watts": act.get("average_watts"),
        "weighted_avg_watts": act.get("weighted_average_watts"),
        "kilojoules": act.get("kilojoules"),
        "suffer_score": act.get("suffer_score"),
        "calories": act.get("calories"),
        "trainer": bool(act.get("trainer")),
        "device_name": act.get("device_name"),
        "external_id": ext,
        "garmin_activity_id": parse_garmin_activity_id(ext),
        "gear_id": act.get("gear_id"),
        "elev_high": act.get("elev_high"),
        "elev_low": act.get("elev_low"),
        "details_json": to_json(act),
    }


def streams_to_row(activity_id, streams):
    if isinstance(streams, list):
        by_type = {s.get("type"): s.get("data") for s in streams if isinstance(s, dict)}
    else:
        by_type = {k: v.get("data") if isinstance(v, dict) else v for k, v in streams.items()}
    return {"strava_activity_id": activity_id, "streams_json": to_json(by_type)}


def is_garmin_device(device_name, garmin_devices):
    if not device_name:
        return False
    low = device_name.lower()
    return any(g in low for g in garmin_devices)
