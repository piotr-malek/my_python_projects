"""
Disaster risk monitor — single-page Streamlit app.

Run from ``disaster_predictor/`` root::

  streamlit run risk_monitor/app.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from risk_monitor.bq_loaders import (
    clear_loader_cache,
    load_change_log,
    load_era5_window,
    load_evaluations_for_date,
    load_evaluation_history,
    load_forecast_weather,
    load_latest_evaluation_date,
    load_openmeteo_window,
    load_regions,
    load_weather_outlook_for_assessments,
)
from risk_monitor.components import (
    build_risk_map,
    inject_sticky_header_css,
    render_driver_block,
)
from risk_monitor.config import CACHE_TTL_SEC, HAZARDS, HAZARD_TIE_ORDER, RISK_HEX
from risk_monitor.heuristics import (
    default_hazard_focus,
    extract_percentile_series,
    nearest_region,
    outlook_heuristic,
    parse_outlook_json,
    trend_label,
)

st.set_page_config(page_title="Disaster risk monitor", layout="wide", initial_sidebar_state="collapsed")

inject_sticky_header_css()

st.title("Disaster risk monitor")
st.caption(
    f"Data: `risk_assessment`, `google_earth`, `daily_ingestion`. "
    f"Cache TTL {CACHE_TTL_SEC}s. Narrative is generated for elevated risk (scores 2–3)."
)

regions_df = load_regions()
if regions_df.empty:
    st.error("Could not load regions from BigQuery (`google_earth.regions_info`).")
    st.stop()

countries = sorted(regions_df["country"].dropna().unique().tolist())
if "selected_country" not in st.session_state and countries:
    st.session_state.selected_country = countries[0]
if "hazard_filter" not in st.session_state:
    st.session_state.hazard_filter = "all"
if "hazard_focus" not in st.session_state:
    st.session_state.hazard_focus = "flood"

latest_d = load_latest_evaluation_date()
if latest_d is None:
    st.warning("No rows in `risk_assessment.daily_evaluation`.")
    st.stop()

eval_df = load_evaluations_for_date(latest_d.strftime("%Y-%m-%d"))
if eval_df.empty:
    st.warning(f"No evaluations for latest date {latest_d.date()}.")
    st.stop()

scores_by_region: dict = {}
meta_by_region_hazard: dict = {}
for _, row in eval_df.iterrows():
    r = row["region"]
    h = row["disaster_type"]
    scores_by_region.setdefault(r, {})[h] = int(row["risk_score"])
    meta_by_region_hazard[(r, h)] = row.to_dict()

# --- Sticky selector bar (first, so session state is current) ---
bar = st.container()
with bar:
    st.markdown('<div class="sticky-risk-bar"></div>', unsafe_allow_html=True)
    c0, c1, c2, c3, c4 = st.columns([1, 2, 2, 2, 1])
    with c0:
        if len(countries) > 1:
            st.session_state.selected_country = st.selectbox(
                "Country",
                countries,
                index=countries.index(st.session_state.selected_country)
                if st.session_state.selected_country in countries
                else 0,
                key="sb_country",
            )
        else:
            st.caption(f"**{countries[0]}**")
            st.session_state.selected_country = countries[0]
    filtered_regions = regions_df[regions_df["country"] == st.session_state.selected_country]
    if filtered_regions.empty:
        filtered_regions = regions_df
    rlist = filtered_regions["region"].tolist()
    with c1:
        search = st.text_input("Search region", "", key="region_search")
        rsub = [r for r in rlist if search.lower() in r.lower()] if search else rlist
        if not rsub:
            rsub = rlist
        cur = st.session_state.get("selected_region")
        idx = rsub.index(cur) if cur in rsub else 0
        st.session_state.selected_region = st.selectbox("Region", rsub, index=idx, key="sb_region")
    with c2:
        opts = ["all"] + list(HAZARDS)
        hi = opts.index(st.session_state.hazard_filter) if st.session_state.hazard_filter in opts else 0
        st.session_state.hazard_filter = st.radio(
            "Hazard filter", opts, horizontal=True, index=hi, key="sb_hazard_filter"
        )
    with c3:
        if st.button("Refresh data", key="refresh_bq"):
            clear_loader_cache()
            st.rerun()
    with c4:
        st.caption(f"Latest eval **{latest_d.date()}**")

sel = st.session_state.selected_region
scores_sel = scores_by_region.get(sel, {})

hist_short = load_evaluation_history(
    sel,
    (latest_d - pd.Timedelta(days=10)).strftime("%Y-%m-%d"),
    latest_d.strftime("%Y-%m-%d"),
)
prev_scores: dict = {}
if not hist_short.empty:
    dates = sorted(hist_short["date"].unique())
    if len(dates) >= 2:
        prev_date = dates[-2]
        sub = hist_short[hist_short["date"] == prev_date]
        for h in HAZARDS:
            hh = sub[sub["disaster_type"] == h]
            if not hh.empty:
                prev_scores[h] = int(hh.iloc[0]["risk_score"])

# --- Tiles ---
st.subheader("Risk snapshot")
tc = st.columns(4)
for i, h in enumerate(HAZARDS):
    sc = scores_sel.get(h, 0)
    tr = trend_label(sc, prev_scores.get(h))
    row_h = hist_short[(hist_short["region"] == sel) & (hist_short["disaster_type"] == h)]
    row_h = row_h.sort_values("date")
    recent_o = {}
    forecast_o = {}
    if not row_h.empty:
        last = row_h.iloc[-1]
        recent_o = parse_outlook_json(last.get("recent_outlook"))
        forecast_o = parse_outlook_json(last.get("forecast_outlook"))
    out_l = outlook_heuristic(recent_o, forecast_o, tr)
    col = RISK_HEX.get(sc, RISK_HEX[0])
    with tc[i]:
        st.markdown(
            f"<div style='padding:10px;border-radius:8px;border-left:6px solid {col};"
            f"background:{col}18;'>",
            unsafe_allow_html=True,
        )
        if st.button(f"{h.title()} · {sc}", key=f"tile_{h}", width="stretch"):
            st.session_state.hazard_focus = h
            st.rerun()
        st.caption(tr)
        st.caption(out_l)
        st.markdown("</div>", unsafe_allow_html=True)

focus = st.session_state.get("hazard_focus", default_hazard_focus(scores_sel, HAZARD_TIE_ORDER))
if focus not in HAZARDS:
    focus = default_hazard_focus(scores_sel, HAZARD_TIE_ORDER)

# --- Priority banner (any hazard score >= 2) ---
elevated = [h for h in HAZARDS if scores_sel.get(h, 0) >= 2]
if elevated:
    banner_h = max(
        elevated,
        key=lambda h: (scores_sel[h], -HAZARD_TIE_ORDER.index(h)),
    )
    row_b = meta_by_region_hazard.get((sel, banner_h))
    aid = str(row_b["assessment_id"]) if row_b else ""
    wo = load_weather_outlook_for_assessments((aid,) if aid else ())
    narrative = ""
    if not wo.empty:
        narrative = str(wo.iloc[0].get("recent_weather_interpretation") or "")
    st.markdown("### Priority")
    if narrative:
        st.info(narrative)
    else:
        st.warning(
            "Elevated risk (≥2) but no LLM narrative yet — run the interpretation DAG or use the detail panel below."
        )

# --- Map ---
st.subheader("Map")
map_out = build_risk_map(
    regions_df,
    scores_by_region,
    st.session_state.hazard_filter,
    sel,
    map_key=f"map_{st.session_state.hazard_filter}",
)
if map_out and isinstance(map_out, dict):
    lc = map_out.get("last_object_clicked")
    if lc and isinstance(lc, dict):
        lat, lng = lc.get("lat"), lc.get("lng")
        if lat is not None and lng is not None:
            nr = nearest_region(
                float(lat),
                float(lng),
                regions_df.to_dict("records"),
            )
            if nr and nr != st.session_state.selected_region:
                st.session_state.selected_region = nr
                st.rerun()

# --- Detail ---
st.subheader("Hazard detail")
meta = meta_by_region_hazard.get((sel, focus))
if not meta:
    st.info("No row for this hazard on the latest date.")
else:
    rs = int(meta["risk_score"])
    recent_o = parse_outlook_json(meta.get("recent_outlook"))
    forecast_o = parse_outlook_json(meta.get("forecast_outlook"))
    st.markdown(f"**{focus.title()}** · score **{rs}** ({meta.get('risk_level')})")

    if rs > 1:
        aid = str(meta["assessment_id"])
        wo = load_weather_outlook_for_assessments((aid,))
        if not wo.empty:
            st.markdown("**Interpretation (stored LLM text)**")
            st.write(wo.iloc[0].get("recent_weather_interpretation") or "*Interpretation pending.*")
        else:
            st.caption("*Interpretation pending — no `weather_outlook` row for this assessment.*")
    elif rs == 1:
        st.markdown("**Outlook (JSON)** — narrative is not generated for score 1.")
        st.json(recent_o if recent_o else {})
    else:
        st.caption("No outlook JSON for score 0.")

    rd_raw = meta.get("rolling_diagnostics")
    if rd_raw and str(rd_raw).strip() not in ("", "{}", "nan"):
        try:
            rd = json.loads(rd_raw) if isinstance(rd_raw, str) else rd_raw
            if isinstance(rd, dict) and rd:
                with st.expander("Rolling diagnostics"):
                    st.json(rd)
        except json.JSONDecodeError:
            pass

    st.markdown("**Drivers** (from `recent_outlook`, max 4)")
    keys = [k for k in recent_o if isinstance(recent_o.get(k), dict)][:4]
    hist7 = load_evaluation_history(
        sel,
        (latest_d - pd.Timedelta(days=8)).strftime("%Y-%m-%d"),
        latest_d.strftime("%Y-%m-%d"),
    )
    hist7_f = hist7[hist7["disaster_type"] == focus].sort_values("date")
    hist_records = hist7_f.tail(7).to_dict("records")
    prev_eval_o = {}
    if len(hist_records) >= 2:
        prev_eval_o = parse_outlook_json(hist_records[-2].get("recent_outlook"))
    for k in keys:
        entry = recent_o[k]
        delta = None
        pe = prev_eval_o.get(k)
        if isinstance(pe, dict) and isinstance(entry, dict):
            pp, cp = pe.get("percentile_approx"), entry.get("percentile_approx")
            if pp is not None and cp is not None:
                delta = float(cp) - float(pp)
        series = extract_percentile_series(hist_records, k)
        render_driver_block(k, entry, delta, series)

# --- 7-day timeline ---
st.subheader("Seven-day context")
c1, c2 = st.columns(2)
with c1:
    st.markdown("**Past risk scores** (from `daily_evaluation`)")
    h14 = load_evaluation_history(
        sel,
        (latest_d - pd.Timedelta(days=14)).strftime("%Y-%m-%d"),
        latest_d.strftime("%Y-%m-%d"),
    )
    if h14.empty:
        st.caption("No history.")
    else:
        pvt = h14.pivot_table(
            index="date", columns="disaster_type", values="risk_score", aggfunc="max"
        )
        st.dataframe(pvt.tail(7), width="stretch")
with c2:
    st.markdown("**Next ~7 days — weather only** (Open-Meteo forecast, not ML risk)")
    fw = load_forecast_weather(sel, latest_d.strftime("%Y-%m-%d"))
    if fw.empty:
        st.caption("No forecast rows in `daily_ingestion.openmeteo_forecast`.")
    else:
        cols = [c for c in ("date", "precipitation_sum", "temperature_2m_max", "sm1_mean") if c in fw.columns]
        st.dataframe(fw.head(7)[cols], width="stretch")

# --- Change log ---
st.subheader("Recent risk changes")
clog = load_change_log(sel, latest_d.strftime("%Y-%m-%d"), st.session_state.hazard_filter)
if clog.empty:
    st.caption("No transitions in the recent window.")
else:
    st.dataframe(clog, width="stretch")

# --- Deep dive ---
with st.expander("Deep dive (30d weather + comparison)"):
    start30 = (latest_d - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
    end30 = latest_d.strftime("%Y-%m-%d")
    era = load_era5_window(sel, start30, end30)
    if not era.empty:
        st.markdown("**ERA5 (daily_ingestion)**")
        chart = era.set_index("date")[["temp_2m_mean_C", "precipitation_sum_mm"]].rename(
            columns={"temp_2m_mean_C": "Temp °C", "precipitation_sum_mm": "Precip mm"}
        )
        st.line_chart(chart)
    om = load_openmeteo_window(sel, start30, end30)
    if not om.empty and "river_discharge" in om.columns:
        st.markdown("**River discharge (openmeteo_weather)**")
        st.line_chart(om.set_index("date")[["river_discharge"]])
    if meta and int(meta["risk_score"]) > 1:
        aid = str(meta["assessment_id"])
        wo = load_weather_outlook_for_assessments((aid,))
        if not wo.empty:
            st.markdown("**Full stored LLM text**")
            st.write(wo.iloc[0].get("recent_weather_interpretation") or "")
    if not era.empty and "temp_2m_mean_C" in era.columns:
        recent = era.tail(7)["temp_2m_mean_C"].mean()
        baseline = era["temp_2m_mean_C"].mean()
        st.caption(f"Mean temp last 7d: {recent:.2f} °C vs 30d mean {baseline:.2f} °C")

st.divider()
st.caption(
    "**Legend** · Risk score: gray 0 (none), yellow 1 (low), orange 2 (medium), red 3 (high). "
    "No green band by design."
)
