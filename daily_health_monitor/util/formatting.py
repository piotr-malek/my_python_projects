"""Human-readable duration strings for digest payloads."""

from __future__ import annotations

from typing import List, Optional, Sequence, Union


def format_minutes_hm(total_minutes: Optional[float]) -> Optional[str]:
    """
    Format a non-negative duration in minutes as compact HhMm.
    Examples: 250 -> '4h10m', 45 -> '45m', 120 -> '2h', 0 -> '0m'.
    """
    if total_minutes is None:
        return None
    m = int(round(abs(float(total_minutes))))
    h = m // 60
    rem = m % 60
    if h > 0 and rem > 0:
        return f"{h}h{rem}m"
    if h > 0:
        return f"{h}h"
    return f"{rem}m"


def format_delta_minutes_pm(delta_minutes: Optional[float]) -> Optional[str]:
    """Signed delta vs baseline, e.g. '-47m', '+15m', '+1h30m', '0m'."""
    if delta_minutes is None:
        return None
    v = int(round(float(delta_minutes)))
    if v == 0:
        return "0m"
    sign = "+" if v > 0 else "-"
    inner = format_minutes_hm(abs(v))
    return f"{sign}{inner}" if inner else None


def format_recharge_rate(rate: Optional[float]) -> Optional[str]:
    """Body Battery points gained per hour of sleep, e.g. '7.7 pts/h'."""
    if rate is None:
        return None
    return f"{float(rate):.1f} pts/h"


def format_recharge_delta(delta: Optional[float]) -> Optional[str]:
    """Signed delta for recharge rate, e.g. '-6.0 pts/h'."""
    if delta is None:
        return None
    v = float(delta)
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f} pts/h"


def format_minutes_pair_hm(pair: Optional[Sequence[Union[float, int]]]) -> Optional[List[str]]:
    """Format a [low, high] minute pair for ranges (e.g. typical sleep band)."""
    if not pair or len(pair) != 2:
        return None
    a, b = format_minutes_hm(pair[0]), format_minutes_hm(pair[1])
    if a is None or b is None:
        return None
    return [a, b]
