def evaluate_patterns(
    wellness_flags,
    scores,
    load,
    garmin_readiness,
    recent_hr_drift_high,
    poor_sleep,
):
    alerts = []
    flag_text = " ".join(wellness_flags).lower()

    if "rhr elevated" in flag_text and recent_hr_drift_high and poor_sleep:
        alerts.append(
            {
                "pattern": "recovery_deficit",
                "interpretation": "Elevated RHR, higher HR at similar effort, and poor sleep suggest recovery deficit",
            }
        )

    if load.get("load_ratio", 0) > 1.3 and ("sleep" in flag_text or "stress" in flag_text):
        alerts.append(
            {
                "pattern": "overreaching",
                "interpretation": "Rising Strava training load with degrading Garmin sleep/stress signals",
            }
        )

    if load.get("atl", 0) > 0 and scores.get("recovery_score", 0) >= 70:
        alerts.append(
            {
                "pattern": "healthy_adaptation",
                "interpretation": "High external load with normal physiological recovery metrics",
            }
        )

    if garmin_readiness is not None and garmin_readiness < 50 and load.get("atl", 0) > 80:
        alerts.append(
            {
                "pattern": "garmin_low_readiness_high_atl",
                "interpretation": "Garmin readiness low while Strava acute load is elevated",
            }
        )

    if "sleep fragmentation" in flag_text and "stress" in flag_text:
        alerts.append(
            {
                "pattern": "burnout_accumulation",
                "interpretation": "Poor sleep consistency with chronic stress accumulation",
            }
        )

    return alerts
