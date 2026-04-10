"""Pure helpers for trend / outlook labels and map styling (unit-testable)."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from risk_monitor.config import RISK_HEX


def risk_color_hex(score: Optional[int]) -> str:
    if score is None or score < 0:
        return RISK_HEX[0]
    return RISK_HEX.get(min(3, max(0, int(score))), RISK_HEX[0])


def trend_label(current: Optional[int], previous: Optional[int]) -> str:
    if current is None or previous is None:
        return "—"
    if current > previous:
        return "↑ Up"
    if current < previous:
        return "↓ Down"
    return "→ Stable"


def _mean_percentile_diff(recent: Dict[str, Any], forecast: Dict[str, Any]) -> Optional[float]:
    diffs = []
    for k in set(recent or {}) & set(forecast or {}):
        ra = recent.get(k)
        fa = forecast.get(k)
        if not isinstance(ra, dict) or not isinstance(fa, dict):
            continue
        rp = ra.get("percentile_approx")
        fp = fa.get("percentile_approx")
        if rp is None or fp is None:
            continue
        try:
            diffs.append(float(fp) - float(rp))
        except (TypeError, ValueError):
            continue
    if not diffs:
        return None
    return sum(diffs) / len(diffs)


def outlook_heuristic(
    recent_outlook: Optional[Dict],
    forecast_outlook: Optional[Dict],
    trend_fallback: str,
) -> str:
    """
    Compare shared keys' percentile_approx between recent and forecast JSON.
    Positive mean delta → conditions trending worse vs climatology in forecast window.
    """
    r = recent_outlook or {}
    f = forecast_outlook or {}
    m = _mean_percentile_diff(r, f)
    if m is None:
        return trend_fallback
    if m > 3.0:
        return "Outlook: worsening"
    if m < -3.0:
        return "Outlook: improving"
    return "Outlook: stable"


def default_hazard_focus(
    scores: Dict[str, int],
    tie_order: Tuple[str, ...],
) -> str:
    """Pick hazard with max score; tie-break by tie_order."""
    if not scores:
        return tie_order[0]
    best = max(scores.values())
    for h in tie_order:
        if scores.get(h, -1) == best:
            return h
    return tie_order[0]


def parse_outlook_json(raw: Any) -> Dict:
    import json

    if raw is None or (isinstance(raw, float) and str(raw) == "nan"):
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s or s == "{}":
            return {}
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return {}
    return {}


def extract_percentile_series(
    history_rows: list,
    metric_key: str,
) -> list:
    """history_rows: list of dicts with 'recent_outlook' JSON string or dict."""
    out = []
    for row in history_rows:
        o = parse_outlook_json(row.get("recent_outlook"))
        entry = o.get(metric_key)
        if isinstance(entry, dict) and entry.get("percentile_approx") is not None:
            try:
                out.append(float(entry["percentile_approx"]))
            except (TypeError, ValueError):
                out.append(None)
        else:
            out.append(None)
    return out


def nearest_region(
    lat: float,
    lon: float,
    regions: list,
) -> Optional[str]:
    """regions: iterable of dicts with centroid_lat, centroid_lon, region."""
    if lat is None or lon is None or not regions:
        return None
    best_r = None
    best_d = None
    for r in regions:
        try:
            plat = float(r["centroid_lat"])
            plon = float(r["centroid_lon"])
        except (KeyError, TypeError, ValueError):
            continue
        d = (plat - lat) ** 2 + (plon - lon) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best_r = r["region"]
    return best_r
