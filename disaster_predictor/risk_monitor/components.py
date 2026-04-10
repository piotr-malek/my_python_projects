"""Reusable Streamlit / Folium UI pieces."""

from __future__ import annotations

import html
import json
from typing import Dict, List, Optional

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from risk_monitor.config import HAZARDS, METRIC_LABELS, RISK_HEX
from risk_monitor.heuristics import risk_color_hex


def inject_sticky_header_css() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlock"] > div:has(> div.sticky-risk-bar) {
            position: sticky;
            top: 0.5rem;
            z-index: 999;
            background: var(--background-color, #0e1117);
            padding-bottom: 0.5rem;
            border-bottom: 1px solid rgba(128,128,128,0.25);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def quadrant_div_icon(
    scores: Dict[str, int],
    size: int = 28,
) -> folium.DivIcon:
    """Four quadrants: flood TL, fire TR, drought BL, landslide BR."""
    quad_style = (
        ("flood", "left:0;top:0;width:50%;height:50%;"),
        ("fire", "left:50%;top:0;width:50%;height:50%;"),
        ("drought", "left:0;top:50%;width:50%;height:50%;"),
        ("landslide", "left:50%;top:50%;width:50%;height:50%;"),
    )
    parts = []
    for qh, pos in quad_style:
        sc = scores.get(qh, 0)
        col = RISK_HEX.get(min(3, max(0, int(sc))), RISK_HEX[0])
        parts.append(
            f'<div style="position:absolute;{pos}background:{col};opacity:0.92;"></div>'
        )
    inner = "".join(parts)
    html_str = (
        f'<div style="width:{size}px;height:{size}px;position:relative;'
        f"border-radius:50%;overflow:hidden;border:1px solid #333;box-sizing:border-box;\">{inner}</div>"
    )
    return folium.DivIcon(html=html_str, icon_size=(size, size), icon_anchor=(size // 2, size // 2))


def single_hazard_icon(disaster_type: str, score: int, size: int = 22) -> folium.DivIcon:
    col = risk_color_hex(score)
    html_str = (
        f'<div style="width:{size}px;height:{size}px;border-radius:50%;background:{col};'
        'border:2px solid #222;box-sizing:border-box;"></div>'
    )
    return folium.DivIcon(html=html_str, icon_size=(size, size), icon_anchor=(size // 2, size // 2))


def build_risk_map(
    regions_df: pd.DataFrame,
    scores_by_region: Dict[str, Dict[str, int]],
    hazard_filter: str,
    selected_region: Optional[str],
    height: int = 420,
    map_key: str = "risk_map",
):
    if regions_df.empty:
        st.info("No region coordinates to show.")
        return None
    lats = regions_df["centroid_lat"].astype(float)
    lons = regions_df["centroid_lon"].astype(float)
    center = [float(lats.mean()), float(lons.mean())]
    m = folium.Map(location=center, zoom_start=5, tiles="cartodbpositron")
    for _, row in regions_df.iterrows():
        r = row["region"]
        lat, lon = float(row["centroid_lat"]), float(row["centroid_lon"])
        sc = scores_by_region.get(r, {})
        if hazard_filter == "all":
            icon = quadrant_div_icon(sc)
            tip = "<br/>".join(f"{h}: {sc.get(h, '—')}" for h in HAZARDS)
        else:
            hs = int(sc.get(hazard_filter, 0))
            icon = single_hazard_icon(hazard_filter, hs)
            tip = f"{hazard_filter}: {hs}"
        folium.Marker(
            [lat, lon],
            icon=icon,
            tooltip=html.escape(r),
            popup=folium.Popup(html.escape(tip), max_width=220),
        ).add_to(m)
    return st_folium(
        m,
        height=height,
        width="100%",
        returned_objects=["last_object_clicked"],
        key=map_key,
    )


def sparkline(values: List[Optional[float]], width: int = 120, height: int = 32) -> str:
    """Tiny inline SVG sparkline; skips None."""
    ys = [v for v in values if v is not None]
    if len(ys) < 2:
        return "—"
    mn, mx = min(ys), max(ys)
    if mx - mn < 1e-9:
        mx = mn + 1.0
    pts = []
    n = len(ys)
    for i, v in enumerate(ys):
        x = (i / (n - 1)) * width if n > 1 else width / 2
        y = height - ((v - mn) / (mx - mn)) * (height - 4) - 2
        pts.append(f"{x:.1f},{y:.1f}")
    d = "M " + " L ".join(pts)
    return (
        f'<svg width="{width}" height="{height}" style="vertical-align:middle">'
        f'<path d="{d}" fill="none" stroke="#90caf9" stroke-width="2"/></svg>'
    )


def render_driver_block(
    metric_key: str,
    entry: dict,
    delta_pct: Optional[float],
    history_percentiles: List[Optional[float]],
) -> None:
    label = METRIC_LABELS.get(metric_key, metric_key.replace("_", " "))
    pct = entry.get("percentile_approx")
    unit = entry.get("unit", "")
    val = entry.get("value")
    c1, c2, c3 = st.columns([2, 1, 2])
    with c1:
        st.caption(f"**{label}**")
        if pct is not None:
            st.write(f"Percentile (approx): **{pct}**")
        if val is not None and unit:
            st.caption(f"Value: {val} {unit}")
    with c2:
        if delta_pct is not None:
            st.caption("Δ vs prior eval")
            st.write(f"{delta_pct:+.1f} pct pts")
    with c3:
        st.markdown(sparkline(history_percentiles), unsafe_allow_html=True)
