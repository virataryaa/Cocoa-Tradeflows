"""
Cocoa TDM Trade Flow Dashboard
================================
Run: streamlit run app.py
"""

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="Cocoa TDM Trade Flow Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  [data-testid="stHeader"]         { background: transparent !important; }
  .block-container { padding-top: 3.5rem !important; padding-bottom: 1.5rem; max-width: 1400px; }
  html, body, [class*="css"]       { font-family: -apple-system, "Helvetica Neue", sans-serif; }
  h1, h2, h3                       { font-weight: 500 !important; }
  hr  { border: none !important; border-top: 1px solid #e8e8ed !important; margin: 0.5rem 0 !important; }
  [data-testid="stExpander"]       { border: 1px solid #e8e8ed !important; border-radius: 8px !important; }
  [data-testid="stDataFrame"]      { border-radius: 8px; overflow: visible !important; }
  .stCaption, [data-testid="stCaptionContainer"] p { font-size: 0.7rem !important; }
  [data-testid="stRadio"] label, [data-testid="stRadio"] label p, [data-testid="stRadio"] label div { font-size: 0.74rem !important; }
  .stTabs [data-baseweb="tab-list"]{ gap: 8px; }
  .stTabs [data-baseweb="tab"], .stTabs [data-baseweb="tab"] p, .stTabs [data-baseweb="tab"] span { font-size: 0.78rem !important; font-weight: 500; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    _CY_PRESETS  = ["Oct\u2013Sep", "Jan\u2013Dec", "Custom"]
    _cy_basis    = st.radio("Crop Year Basis", _CY_PRESETS, index=0, key="cy_basis")
    _MONTH_ABBRS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    if _cy_basis == "Oct\u2013Sep":
        crop_start_month = 10
    elif _cy_basis == "Jan\u2013Dec":
        crop_start_month = 1
    else:
        _cs = st.selectbox("Start month", _MONTH_ABBRS, index=9, key="cy_custom_start")
        crop_start_month = _MONTH_ABBRS.index(_cs) + 1
    st.markdown("---")
    unit_choice = st.radio(
        "Unit",
        ["k MT (BEQ)", "k MT (Raw)", "Lots (BEQ)", "Lots (Raw)"],
        index=0, key="unit_choice_global",
    )

# ── Derived unit format (all units have large numbers — always use commas) ────
_num_fmt  = ",.1f"
_TC       = "#1d1d1f"
_GC       = "#f0f0f0"
_TMPL     = "plotly_white"
_HT_BAG   = f"%{{y:{_num_fmt}}}<extra>%{{fullData.name}}</extra>"
_HT_BAG_H = f"%{{x:{_num_fmt}}}<extra>%{{fullData.name}}</extra>"
_HT_PCT   = "%{y:.1f}%<extra>%{fullData.name}</extra>"

# ── Constants ─────────────────────────────────────────────────────────────────
_ALL_MONTHS  = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_CAL_MONTH_NAMES = {i + 1: m for i, m in enumerate(_ALL_MONTHS)}
MONTH_ORDER  = [_ALL_MONTHS[(crop_start_month - 1 + i) % 12] for i in range(12)]
NUM_TO_MONTH = {i + 1: m for i, m in enumerate(MONTH_ORDER)}

_DATA = Path(__file__).parent / "data"

FLOW_PATHS = {
    "Cocoa Exports":           str(_DATA / "tdm_cocoa_exports.parquet"),
    "Cocoa Imports":           str(_DATA / "tdm_cocoa_imports.parquet"),
    "Cocoa Imports (EU Only)": str(_DATA / "tdm_cocoa_imports_eu.parquet"),
}

import datetime as _dt
with st.sidebar:
    st.markdown("---")
    for _lbl, _fp in FLOW_PATHS.items():
        _p = Path(_fp)
        if _p.exists():
            _mt = _dt.datetime.fromtimestamp(_p.stat().st_mtime).strftime("%d %b %Y %H:%M")
            st.caption(f"**{_lbl}**  \n{_mt}")

_D = dict(
    template=_TMPL,
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="-apple-system, Helvetica Neue, sans-serif", color=_TC, size=10),
)
_PAL = ["#7bafd4","#f4a460","#82c982","#c9a0dc","#e8c96a","#7ec8c0","#e89090","#a0aad4",
        "#c8a06e","#90b8a0","#d4c0a0","#a8c0d8"]

CHART_H    = 300
CHART_H_LG = 225


def _fmt_list(lst: list, max_show: int = 5) -> str:
    if not lst:
        return "\u2014"
    if len(lst) <= max_show:
        return ", ".join(str(x) for x in lst)
    return ", ".join(str(x) for x in lst[:max_show]) + f" +{len(lst) - max_show} more"


def lbl(text: str, sub: str = "") -> str:
    out = (
        f"<div style='background:#0a2463;padding:5px 13px;border-radius:5px;"
        f"margin:0 0 2px 0;text-align:center'>"
        f"<span style='font-size:0.78rem;font-weight:500;letter-spacing:0.07em;"
        f"text-transform:uppercase;color:#dde4f0'>{text}</span></div>"
    )
    if sub:
        out += (
            f"<div style='font-size:0.66rem;color:#8e8e93;font-style:italic;"
            f"text-align:center;margin:0 0 5px'>{sub}</div>"
        )
    return out


def apply_crop_year(df: pd.DataFrame, start_month: int) -> pd.DataFrame:
    mn = df["MONTH_NUM"]
    year_start = df["YEAR"] - (mn < start_month).astype(int)
    if start_month == 1:
        df["CROP_YEAR"] = year_start.astype(str)
    else:
        df["CROP_YEAR"] = (
            (year_start % 100).astype(str).str.zfill(2) + "/"
            + ((year_start + 1) % 100).astype(str).str.zfill(2)
        )
    df["CROP_MONTH_NUM"] = ((mn - start_month) % 12) + 1
    return df


@st.cache_data(ttl=600)
def _load_parquet_raw(path: str) -> pd.DataFrame:
    _p = path.replace("\\", "/")
    df = duckdb.sql(f"SELECT * FROM '{_p}'").df()
    if "MONTH" in df.columns and "MONTH_NUM" not in df.columns:
        df = df.rename(columns={"MONTH": "MONTH_NUM"})
    if "REPORTER_REGION" not in df.columns:
        df["REPORTER_REGION"] = "Other"
    return df


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["TDM", "Source", "Divergence", "Mirror"])


# =============================================================================
# TAB 1 — TDM FLOW
# =============================================================================
with tab1:

    flow_choice = st.radio(
        "Flow", list(FLOW_PATHS.keys()), index=0, horizontal=True,
        label_visibility="collapsed",
    )

    # Per-flow namespace prefix — every widget/session key in the drilldown tab is
    # prefixed with this so switching flows never leaks stale state across them.
    _fk = {"Cocoa Exports": "exp", "Cocoa Imports": "imp", "Cocoa Imports (EU Only)": "eu"}[flow_choice]

    _is_exports     = flow_choice == "Cocoa Exports"
    flow_label      = "Exports" if _is_exports else "Imports"
    _dest_noun      = "Destination" if _is_exports else "Origin"
    _dest_arrow_tab = "Rep \u2192 Dest" if _is_exports else "Rep \u2190 Origin"
    unit_label      = unit_choice

    data_path = Path(FLOW_PATHS[flow_choice])
    try:
        df = _load_parquet_raw(str(data_path)).copy()
        df["DATE"] = pd.to_datetime(df[["YEAR","MONTH_NUM"]].rename(columns={"MONTH_NUM":"MONTH"}).assign(DAY=1))
        df = apply_crop_year(df, crop_start_month)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Unit computation — all use BEQ or QTY1 directly (already in MT)
    if unit_choice == "k MT (BEQ)":
        df["BAGS"] = df["BEQ"] / 1000
    elif unit_choice == "k MT (Raw)":
        df["BAGS"] = df["QTY1"] / 1000
    elif unit_choice == "Lots (BEQ)":
        df["BAGS"] = df["BEQ"] / 10       # 1 Lot = 10 MT
    else:  # Lots (Raw)
        df["BAGS"] = df["QTY1"] / 10

    st_ov, st_dest = st.tabs(["Overview", "Drilldown"])

    # =========================================================================
    # OVERVIEW SUBTAB
    # =========================================================================
    with st_ov:
        with st.expander("Filters", expanded=False):
            fc0, fc1, fc2, fc3, fc4 = st.columns([1.5, 1.5, 1.5, 1.5, 2])

            all_reporter_regions = ["All"] + sorted(df["REPORTER_REGION"].dropna().unique())
            with fc0:
                sel_rep_region = st.selectbox("Reporter Region", all_reporter_regions, index=0)

            reporters_in_scope = (
                sorted(df["REPORTER"].dropna().unique()) if sel_rep_region == "All"
                else sorted(df[df["REPORTER_REGION"] == sel_rep_region]["REPORTER"].dropna().unique())
            )
            with fc1:
                _rep_default = (
                    [r for r in ["Cote d'Ivoire"] if r in reporters_in_scope] if _is_exports
                    else [r for r in ["United States"] if r in reporters_in_scope]
                ) or reporters_in_scope
                sel_reporters = st.multiselect("Reporter", reporters_in_scope, default=_rep_default)

            all_tags = sorted(df["COMMODITY_TAG"].dropna().unique()) if "COMMODITY_TAG" in df.columns else []
            with fc2:
                _tag_default = [t for t in ["Beans"] if t in all_tags] or all_tags
                sel_tags = st.multiselect("Type", all_tags, default=_tag_default) if all_tags else []

            all_regions = sorted(df["REGION"].dropna().unique())
            with fc3:
                sel_partner_region = st.selectbox("Partner Region", ["All"] + all_regions, index=0)

            partners_in_scope = (
                sorted(df["PARTNER"].dropna().unique()) if sel_partner_region == "All"
                else sorted(df[df["REGION"] == sel_partner_region]["PARTNER"].dropna().unique())
            )
            sel_regions = all_regions if sel_partner_region == "All" else [sel_partner_region]
            with fc4:
                sel_partners = st.multiselect("Partner", partners_in_scope, default=partners_in_scope)

        mask = (
            df["REPORTER"].isin(sel_reporters or reporters_in_scope)
            & df["REGION"].isin(sel_regions)
            & df["PARTNER"].isin(sel_partners or partners_in_scope)
        )
        if all_tags:
            mask &= df["COMMODITY_TAG"].isin(sel_tags or all_tags)

        dff = df[mask].copy()

        _t1_rep  = _fmt_list(sel_reporters or reporters_in_scope)
        _t1_type = (" \u00b7 " + _fmt_list(sel_tags or all_tags)) if all_tags else ""
        _t1_dest = (
            f" \u2192 {sel_partner_region} ({_fmt_list(sel_partners or partners_in_scope)})"
            if sel_partner_region != "All" else " \u2192 All Regions"
        )
        _t1_sub = _t1_rep + _t1_type + _t1_dest

        if not dff.empty:
            latest_cy         = sorted(dff["CROP_YEAR"].unique())[-1]
            lm_per_rep        = dff[dff["CROP_YEAR"] == latest_cy].groupby("REPORTER")["CROP_MONTH_NUM"].max()
            latest_common_num = int(lm_per_rep.min()) if len(lm_per_rep) else 12
            latest_common_label = NUM_TO_MONTH[latest_common_num]
            dff_disp = dff[
                (dff["CROP_YEAR"] < latest_cy) |
                ((dff["CROP_YEAR"] == latest_cy) & (dff["CROP_MONTH_NUM"] <= latest_common_num))
            ].copy()
        else:
            latest_cy = ""; latest_common_num = 12; latest_common_label = MONTH_ORDER[-1]
            dff_disp = dff.copy()

        if not dff_disp.empty and dff_disp["CROP_YEAR"].nunique() > 1:
            oldest_cy = sorted(dff_disp["CROP_YEAR"].unique())[0]
            dff_disp  = dff_disp[dff_disp["CROP_YEAR"] != oldest_cy].copy()

        _sorted_cy = sorted(dff_disp["CROP_YEAR"].unique()) if not dff_disp.empty else []
        prev_cy    = _sorted_cy[-2] if len(_sorted_cy) >= 2 else None

        def cy_style(cy):
            if cy == latest_cy: return _TC, 2.5
            if cy == prev_cy:   return "#c0392b", 2.0
            return None, 1.4

        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown(
            f"### Cocoa Trade Flows &nbsp;"
            f"<span style='font-size:0.85rem;font-weight:400;color:#6e6e73'>{unit_label}</span>",
            unsafe_allow_html=True,
        )
        st.markdown("<hr>", unsafe_allow_html=True)

        if dff_disp.empty:
            st.warning("No data for the current selection.")
            st.stop()

        _fmt = lambda x: f"{x:{_num_fmt}}" if pd.notna(x) else ""

        def build_pivot(data: pd.DataFrame):
            grp = data.groupby(["CROP_YEAR","CROP_MONTH_NUM"])["BAGS"].sum().reset_index()
            grp["CROP_MONTH"] = grp["CROP_MONTH_NUM"].map(NUM_TO_MONTH)
            pivot = (
                grp.pivot(index="CROP_YEAR", columns="CROP_MONTH", values="BAGS")
                .reindex(columns=MONTH_ORDER).fillna(0).sort_index(ascending=True)
            )
            complete = (pivot > 0).sum(axis=1) == 12
            return pivot, complete

        pivot, complete = build_pivot(dff_disp)
        complete_years  = sorted(complete[complete].index.tolist())

        ytd = (
            dff_disp[dff_disp["CROP_MONTH_NUM"] <= latest_common_num]
            .groupby("CROP_YEAR")["BAGS"].sum().reset_index()
            .sort_values("CROP_YEAR").rename(columns={"BAGS": "YTD_BAGS"})
        )
        ytd["YOY_PCT"] = ytd["YTD_BAGS"].pct_change() * 100

        sea = (
            dff_disp.groupby(["CROP_YEAR","CROP_MONTH_NUM"])["BAGS"]
            .sum().reset_index().sort_values(["CROP_YEAR","CROP_MONTH_NUM"])
        )
        sea["CROP_MONTH"] = sea["CROP_MONTH_NUM"].map(NUM_TO_MONTH)
        all_sea_cy = sorted(sea["CROP_YEAR"].unique())

        if len(all_sea_cy) >= 2:
            cy_range = st.select_slider(
                "Crop year range", options=all_sea_cy,
                value=(all_sea_cy[0], all_sea_cy[-1]),
            )
            sel_sea_cy = [cy for cy in all_sea_cy if cy_range[0] <= cy <= cy_range[1]]
        else:
            sel_sea_cy = all_sea_cy

        ref_band: dict = {}
        if complete_years:
            last10   = complete_years[-10:]
            ref_s    = pivot.loc[last10, MONTH_ORDER]
            ref_band = {"max": ref_s.max(), "min": ref_s.min(), "avg": ref_s.mean(), "n": len(last10)}

        ytd_disp = ytd[ytd["CROP_YEAR"].isin(sel_sea_cy)].copy()
        ytd_disp["YOY_PCT"] = ytd_disp["YTD_BAGS"].pct_change() * 100

        # ── Scenario Projection ───────────────────────────────────────────────
        tdm_remaining_cms = list(range(latest_common_num + 1, 13))
        tdm_proj_vals: dict = {}

        if tdm_remaining_cms and latest_cy and latest_cy in pivot.index:
            st.markdown(
                f"<div style='margin:10px 0 4px;font-size:0.82rem;font-weight:600;color:#3a3a3c'>"
                f"Scenario Projection &nbsp;"
                f"<span style='font-weight:400;color:#888'>remaining months of {latest_cy} "
                f"({NUM_TO_MONTH.get(latest_common_num+1 if latest_common_num<12 else 12,'?')}\u2013{MONTH_ORDER[-1]})</span></div>",
                unsafe_allow_html=True,
            )
            _tpc1, _tpc2 = st.columns([2, 4])
            with _tpc1:
                tdm_proj_method = st.radio(
                    "Method", ["YTD Method","Proportions","Manual (per Month)","Manual (Yearly)"],
                    horizontal=True, key="tdm_proj_method",
                )
            with _tpc2:
                _tdm_act_mls = [NUM_TO_MONTH.get(c) for c in range(1, latest_common_num + 1) if NUM_TO_MONTH.get(c)]
                if tdm_proj_method == "YTD Method":
                    _tdm_default_yoy = 0.0
                    if prev_cy and prev_cy in pivot.index and _tdm_act_mls:
                        _py_ytd = pivot.loc[prev_cy, _tdm_act_mls].sum()
                        _cy_ytd = pivot.loc[latest_cy, _tdm_act_mls].sum()
                        if _py_ytd > 0:
                            _tdm_default_yoy = round((_cy_ytd / _py_ytd - 1) * 100, 1)
                    _tdm_sel_hash = str(sorted(sel_reporters)) + str(sorted(sel_tags or [])) + str(sorted(sel_partners))
                    if st.session_state.get("_tdm_proj_hash") != _tdm_sel_hash:
                        st.session_state["tdm_proj_yoy"]    = float(_tdm_default_yoy)
                        st.session_state["_tdm_proj_hash"]  = _tdm_sel_hash
                    tdm_proj_yoy = st.number_input(
                        f"YoY % vs {prev_cy} (auto-filled from current YTD)",
                        value=float(_tdm_default_yoy), step=0.5, format="%.1f", key="tdm_proj_yoy",
                    )
                    for _cm in tdm_remaining_cms:
                        _ml = NUM_TO_MONTH.get(_cm)
                        if not _ml: continue
                        _base = pivot.loc[prev_cy, _ml] if prev_cy and prev_cy in pivot.index else 0.0
                        if _base <= 0 and complete_years:
                            _base = float(pivot.loc[complete_years[-min(5, len(complete_years)):], _ml].mean())
                        if _base > 0:
                            tdm_proj_vals[_cm] = _base * (1 + tdm_proj_yoy / 100)

                elif tdm_proj_method == "Proportions":
                    if complete_years:
                        _ref_cys    = complete_years[-min(5, len(complete_years)):]
                        _ref_df     = pivot.loc[_ref_cys, MONTH_ORDER].astype(float)
                        _avg_props  = _ref_df.div(_ref_df.sum(axis=1), axis=0).mean()
                        _act_sum_p  = _avg_props[_tdm_act_mls].sum() if _tdm_act_mls else 0
                        _cy_ytd_val = pivot.loc[latest_cy, _tdm_act_mls].sum() if _tdm_act_mls else 0
                        _implied    = _cy_ytd_val / _act_sum_p if _act_sum_p > 0 else _ref_df.sum(axis=1).mean()
                        for _cm in tdm_remaining_cms:
                            _ml = NUM_TO_MONTH.get(_cm)
                            if _ml:
                                tdm_proj_vals[_cm] = float(_implied * _avg_props.get(_ml, 0))
                        st.markdown(
                            f"<div style='background:#e8f0fe;border-left:4px solid #4a7fb5;padding:8px 14px;"
                            f"border-radius:4px;margin:4px 0'>"
                            f"<span style='font-size:1.05rem;font-weight:700;color:#0a2463'>"
                            f"Implied full-year total: {_implied:{_num_fmt}} {unit_label}</span>"
                            f"<span style='font-size:0.85rem;color:#444;margin-left:12px'>"
                            f"Based on avg proportions of {len(_ref_cys)} complete years</span></div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.info("No complete crop years for proportions method.")

                elif tdm_proj_method == "Manual (per Month)":
                    tdm_proj_manual = st.number_input(
                        f"Value per remaining month ({unit_label})",
                        min_value=0.0, value=0.0, step=0.01, format="%.2f", key="tdm_proj_manual",
                    )
                    for _cm in tdm_remaining_cms:
                        tdm_proj_vals[_cm] = float(tdm_proj_manual)

                else:  # Manual (Yearly)
                    _ref_cys_y   = complete_years[-min(5, len(complete_years)):] if complete_years else []
                    _avg_props_y = None
                    if _ref_cys_y:
                        _ref_df_y    = pivot.loc[_ref_cys_y, MONTH_ORDER].astype(float)
                        _avg_props_y = _ref_df_y.div(_ref_df_y.sum(axis=1), axis=0).mean()
                    _cy_ytd_actual = float(pivot.loc[latest_cy, _tdm_act_mls].sum()) if _tdm_act_mls and latest_cy in pivot.index else 0.0
                    _default_yearly = 0.0
                    if _avg_props_y is not None and _tdm_act_mls:
                        _act_sum_p = _avg_props_y[_tdm_act_mls].sum()
                        if _act_sum_p > 0:
                            _default_yearly = round(_cy_ytd_actual / _act_sum_p, 0)
                    tdm_proj_yearly = st.number_input(
                        f"Full crop year target ({unit_label})  [YTD actual: {_cy_ytd_actual:{_num_fmt}}]",
                        min_value=0.0, value=float(_default_yearly), step=0.1, format="%.2f",
                        key="tdm_proj_yearly",
                    )
                    _rem_budget = max(0.0, float(tdm_proj_yearly) - _cy_ytd_actual)
                    if _avg_props_y is not None and _rem_budget > 0:
                        _rem_mls      = [NUM_TO_MONTH.get(c) for c in tdm_remaining_cms if NUM_TO_MONTH.get(c)]
                        _rem_prop_sum = _avg_props_y[_rem_mls].sum() if _rem_mls else 0
                        for _cm in tdm_remaining_cms:
                            _ml = NUM_TO_MONTH.get(_cm)
                            if _ml and _rem_prop_sum > 0:
                                tdm_proj_vals[_cm] = float(_rem_budget * _avg_props_y.get(_ml, 0) / _rem_prop_sum)
                    st.caption(f"Remaining budget: {_rem_budget:{_num_fmt}} {unit_label}")

        col_l, col_c, col_r = st.columns(3)

        with col_l:
            st.markdown(lbl(f"Seasonal ({unit_label}) \u00b7 Monthly {flow_label}", _t1_sub), unsafe_allow_html=True)
            fig3 = go.Figure()
            if ref_band:
                fig3.add_trace(go.Scatter(x=MONTH_ORDER, y=ref_band["max"].values, name=f"Max (L{ref_band['n']}Y)", mode="lines", line=dict(color="#5a9e6f", width=1.2)))
                fig3.add_trace(go.Scatter(x=MONTH_ORDER, y=ref_band["min"].values, name=f"Min (L{ref_band['n']}Y)", mode="lines", line=dict(color="#e07b39", width=1.2), fill="tonexty", fillcolor="rgba(180,180,180,0.08)"))
                fig3.add_trace(go.Scatter(x=MONTH_ORDER, y=ref_band["avg"].values, name=f"Avg (L{ref_band['n']}Y)", mode="lines", line=dict(dash="dot", color="#aaaaaa", width=1.2)))
            pal_i = 0
            for cy in sorted(sel_sea_cy):
                color, width = cy_style(cy)
                if color is None:
                    color = _PAL[pal_i % len(_PAL)]; pal_i += 1
                d = sea[sea["CROP_YEAR"] == cy].sort_values("CROP_MONTH_NUM")
                fig3.add_trace(go.Scatter(x=d["CROP_MONTH"], y=d["BAGS"], name=cy, mode="lines+markers", line=dict(color=color, width=width), marker=dict(size=3)))
            if tdm_proj_vals and latest_cy and latest_cy in pivot.index and latest_cy in sel_sea_cy:
                _last_m = NUM_TO_MONTH.get(latest_common_num)
                _last_v = float(pivot.loc[latest_cy, _last_m]) if _last_m and pivot.loc[latest_cy, _last_m] > 0 else None
                _px3 = ([_last_m] if _last_m and _last_v else []) + [NUM_TO_MONTH.get(c) for c in tdm_remaining_cms if c in tdm_proj_vals and NUM_TO_MONTH.get(c)]
                _py3 = ([_last_v] if _last_m and _last_v else []) + [tdm_proj_vals[c] for c in tdm_remaining_cms if c in tdm_proj_vals and NUM_TO_MONTH.get(c)]
                if _px3:
                    fig3.add_trace(go.Scatter(x=_px3, y=_py3, name=f"{latest_cy} (proj)", mode="lines+markers",
                        line=dict(color=_TC, width=2.0, dash="dot"), marker=dict(size=5, symbol="circle-open", color=_TC)))
            fig3.update_traces(hovertemplate=_HT_BAG)
            fig3.update_layout(
                height=CHART_H,
                xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                legend=dict(orientation="h", y=-0.22, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                margin=dict(t=10, b=80, l=4, r=4), **_D,
            )
            st.plotly_chart(fig3, use_container_width=True)

            st.markdown(lbl(f"Min / Max / Avg vs Latest ({unit_label})", _t1_sub), unsafe_allow_html=True)
            if complete_years:
                last10_mm = complete_years[-10:]
                ref_mm    = pivot.loc[last10_mm, MONTH_ORDER]
                mn, mx, avg = ref_mm.min(), ref_mm.max(), ref_mm.mean()
                latest_x = MONTH_ORDER[:latest_common_num]
                latest_y = [
                    pivot.loc[latest_cy, m] if latest_cy in pivot.index and pivot.loc[latest_cy, m] > 0 else None
                    for m in latest_x
                ]
                fig_mm = go.Figure()
                fig_mm.add_trace(go.Scatter(x=MONTH_ORDER, y=mx.values,  name=f"Max (L{len(last10_mm)}Y)", mode="lines", line=dict(color="#5a9e6f", width=1.4)))
                fig_mm.add_trace(go.Scatter(x=MONTH_ORDER, y=mn.values,  name=f"Min (L{len(last10_mm)}Y)", mode="lines", line=dict(color="#e07b39", width=1.4), fill="tonexty", fillcolor="rgba(180,180,180,0.10)"))
                fig_mm.add_trace(go.Scatter(x=MONTH_ORDER, y=avg.values, name=f"Avg (L{len(last10_mm)}Y)", mode="lines", line=dict(dash="dot", color="#aaaaaa", width=1.4)))
                fig_mm.add_trace(go.Scatter(x=latest_x, y=latest_y, name=latest_cy, mode="lines+markers", line=dict(color=_TC, width=2.5), marker=dict(size=5)))
                if tdm_proj_vals and latest_cy in pivot.index:
                    _last_m_mm = NUM_TO_MONTH.get(latest_common_num)
                    _last_v_mm = float(pivot.loc[latest_cy, _last_m_mm]) if _last_m_mm and pivot.loc[latest_cy, _last_m_mm] > 0 else None
                    _pmx = ([_last_m_mm] if _last_m_mm and _last_v_mm else []) + [NUM_TO_MONTH.get(c) for c in tdm_remaining_cms if c in tdm_proj_vals and NUM_TO_MONTH.get(c)]
                    _pmy = ([_last_v_mm] if _last_m_mm and _last_v_mm else []) + [tdm_proj_vals[c] for c in tdm_remaining_cms if c in tdm_proj_vals and NUM_TO_MONTH.get(c)]
                    if _pmx:
                        fig_mm.add_trace(go.Scatter(x=_pmx, y=_pmy, name=f"{latest_cy} (proj)", mode="lines+markers",
                            line=dict(color=_TC, width=2.0, dash="dot"), marker=dict(size=5, symbol="circle-open", color=_TC)))
                fig_mm.update_traces(hovertemplate=_HT_BAG)
                fig_mm.update_layout(
                    height=CHART_H,
                    xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                    yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                    legend=dict(orientation="h", y=-0.22, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                    margin=dict(t=10, b=80, l=4, r=4), **_D,
                )
                st.plotly_chart(fig_mm, use_container_width=True)
            else:
                st.info("No complete crop years for reference.")

        with col_c:
            st.markdown(lbl(f"YTD Trend \u00b7 {MONTH_ORDER[0]}\u2013{latest_common_label} ({unit_label})", _t1_sub), unsafe_allow_html=True)
            fig6 = go.Figure(go.Scatter(
                x=ytd_disp["CROP_YEAR"], y=ytd_disp["YTD_BAGS"],
                mode="lines+markers", line=dict(color="#4a7fb5", width=1.8), marker=dict(size=4),
            ))
            fig6.update_traces(hovertemplate=_HT_BAG)
            fig6.update_layout(
                height=CHART_H,
                xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=8, color=_TC)),
                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                margin=dict(t=4, b=7, l=4, r=4), **_D,
            )
            st.plotly_chart(fig6, use_container_width=True)

            st.markdown(lbl(f"YoY % Change ({unit_label})", _t1_sub), unsafe_allow_html=True)
            pct_df = ytd_disp.dropna(subset=["YOY_PCT"]).copy()
            pct_df["COLOR"] = pct_df["YOY_PCT"].apply(lambda x: "#5a9e6f" if x >= 0 else "#c0392b")
            fig7 = go.Figure(go.Bar(
                x=pct_df["CROP_YEAR"], y=pct_df["YOY_PCT"],
                marker_color=pct_df["COLOR"],
                text=pct_df["YOY_PCT"].map(lambda x: f"{x:+.1f}%"),
                textposition="outside", textfont=dict(size=8),
            ))
            fig7.add_hline(y=0, line_color="#cccccc", line_width=1)
            fig7.update_traces(hovertemplate=_HT_PCT)
            fig7.update_layout(
                height=CHART_H,
                xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=8, color=_TC)),
                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                margin=dict(t=10, b=7, l=4, r=4), **_D,
            )
            st.plotly_chart(fig7, use_container_width=True)

        with col_r:
            st.markdown(lbl(f"Cumulative {flow_label} ({unit_label})", _t1_sub), unsafe_allow_html=True)
            fig4 = go.Figure()
            pal_i = 0
            for cy in sorted(sel_sea_cy):
                color, width = cy_style(cy)
                if color is None:
                    color = _PAL[pal_i % len(_PAL)]; pal_i += 1
                d = sea[sea["CROP_YEAR"] == cy].sort_values("CROP_MONTH_NUM").copy()
                d["CUM_BAGS"] = d["BAGS"].cumsum()
                fig4.add_trace(go.Scatter(x=d["CROP_MONTH"], y=d["CUM_BAGS"], name=cy, mode="lines+markers", line=dict(color=color, width=width), marker=dict(size=3)))
            if tdm_proj_vals and latest_cy and latest_cy in pivot.index and latest_cy in sel_sea_cy:
                _cum_last = sum(
                    pivot.loc[latest_cy, NUM_TO_MONTH.get(c)]
                    for c in range(1, latest_common_num + 1)
                    if NUM_TO_MONTH.get(c) and pivot.loc[latest_cy, NUM_TO_MONTH.get(c)] > 0
                )
                _last_m4 = NUM_TO_MONTH.get(latest_common_num)
                _pcx4 = [_last_m4] if _last_m4 else []
                _pcy4 = [_cum_last]
                _run4 = _cum_last
                for _cm in tdm_remaining_cms:
                    _ml = NUM_TO_MONTH.get(_cm)
                    if _ml and _cm in tdm_proj_vals:
                        _run4 += tdm_proj_vals[_cm]
                        _pcx4.append(_ml)
                        _pcy4.append(_run4)
                if len(_pcx4) > 1:
                    fig4.add_trace(go.Scatter(x=_pcx4, y=_pcy4, name=f"{latest_cy} (proj)", mode="lines+markers",
                        line=dict(color=_TC, width=2.0, dash="dot"), marker=dict(size=5, symbol="circle-open", color=_TC)))
            fig4.update_traces(hovertemplate=_HT_BAG)
            fig4.update_layout(
                height=CHART_H * 2,
                xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                legend=dict(orientation="h", y=-0.1, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                margin=dict(t=10, b=80, l=4, r=4), **_D,
            )
            st.plotly_chart(fig4, use_container_width=True)

        st.markdown("<hr>", unsafe_allow_html=True)

        # ── Heatmap ───────────────────────────────────────────────────────────
        st.markdown(lbl(f"Flow Heatmap ({unit_label}) \u00b7 Monthly {flow_label} by Crop Year", _t1_sub), unsafe_allow_html=True)
        st.caption(
            f"Latest crop year ({latest_cy}) capped at {latest_common_label}  \u00b7  "
            f"Light grey = no data  \u00b7  Total shown only for complete {MONTH_ORDER[0]}\u2013{MONTH_ORDER[-1]} years  \u00b7  "
            f"Min / Max / Avg rows based on last 10 complete crop years"
        )

        disp = pivot[MONTH_ORDER].astype(float)
        disp[disp == 0] = np.nan
        disp_sel     = disp.loc[[cy for cy in sel_sea_cy if cy in disp.index]].copy()
        complete_sel = complete.reindex(disp_sel.index)
        disp_sel["Total"] = np.where(complete_sel, disp_sel[MONTH_ORDER].sum(axis=1), np.nan)
        main_idx  = disp_sel.index.tolist()
        _REF_ROWS = []

        _va_label_t1 = None
        _va_data_t1: dict = {}
        if latest_cy and latest_cy in disp.index and complete_years:
            _va_ref  = pivot.loc[complete_years[-10:], MONTH_ORDER].astype(float)
            _va_avg  = _va_ref.mean()
            _va_n    = len(complete_years[-10:])
            _va_label_t1 = f"\u2195 vs Avg (L{_va_n}Y)%"
            for _m in MONTH_ORDER:
                _c = disp.loc[latest_cy, _m]; _a = _va_avg[_m]
                _va_data_t1[_m] = (_c / _a - 1) * 100 if pd.notna(_c) and _a > 0 else np.nan
            _va_data_t1["Total"] = np.nan

        if complete_years:
            last10_hm     = complete_years[-10:]
            ref_hm        = pivot.loc[last10_hm, MONTH_ORDER].astype(float)
            annual_totals = ref_hm.sum(axis=1)
            n             = len(last10_hm)
            sep     = pd.Series({m: np.nan for m in MONTH_ORDER + ["Total"]}, name="  ")
            row_min = pd.Series({**ref_hm.min().to_dict(),  "Total": annual_totals.min()},  name=f"Min (L{n}Y)")
            row_max = pd.Series({**ref_hm.max().to_dict(),  "Total": annual_totals.max()},  name=f"Max (L{n}Y)")
            row_avg = pd.Series({**ref_hm.mean().to_dict(), "Total": annual_totals.mean()}, name=f"Avg (L{n}Y)")
            disp_full = pd.concat([disp_sel, sep.to_frame().T, row_min.to_frame().T, row_max.to_frame().T, row_avg.to_frame().T])
            _REF_ROWS = [f"Min (L{n}Y)", f"Max (L{n}Y)", f"Avg (L{n}Y)"]
        else:
            disp_full = disp_sel.copy()

        if _va_label_t1 and latest_cy in disp_full.index:
            _va_sep  = pd.Series({m: np.nan for m in MONTH_ORDER + ["Total"]}, name=" ")
            _va_s    = pd.Series(_va_data_t1, name=_va_label_t1)
            _va_pos  = disp_full.index.tolist().index(latest_cy) + 1
            disp_full = pd.concat([disp_full.iloc[:_va_pos], _va_sep.to_frame().T, _va_s.to_frame().T, disp_full.iloc[_va_pos:]])

        _t1_pct_rows = [r for r in [_va_label_t1] if r and r in disp_full.index]

        styled = (
            disp_full.style
            .background_gradient(cmap="Blues", axis=None, subset=pd.IndexSlice[main_idx, MONTH_ORDER])
            .highlight_null(color=_GC)
            .format(_fmt, subset=pd.IndexSlice[:, MONTH_ORDER + ["Total"]])
            .set_properties(**{"text-align": "center", "font-size": "8px"})
            .set_properties(
                subset=pd.IndexSlice[main_idx, ["Total"]],
                **{"font-weight": "700", "background-color": "#f5f5f7",
                   "border-left": "2px solid #d8d8e0", "font-size": "8px"},
            )
            .set_table_styles([
                {"selector": "th", "props": [("text-align","center"),("font-size","8px"),("font-weight","600")]},
                {"selector": "td", "props": [("text-align","center"),("font-size","8px")]},
            ])
        )
        if _REF_ROWS:
            styled = styled.set_properties(
                subset=pd.IndexSlice[_REF_ROWS, :],
                **{"background-color": "#eef3fb", "font-weight": "600", "font-style": "italic", "color": "#2c3e6e"},
            )
        if _t1_pct_rows:
            _va_vals_t1 = disp_full.loc[_t1_pct_rows[0], MONTH_ORDER].dropna()
            _va_abs_t1  = max(abs(_va_vals_t1.min()), abs(_va_vals_t1.max())) if not _va_vals_t1.empty else 20
            styled = (styled
                .background_gradient(cmap="RdYlGn", subset=pd.IndexSlice[_t1_pct_rows, MONTH_ORDER], vmin=-_va_abs_t1, vmax=_va_abs_t1)
                .format(lambda x: f"{x:+.1f}%" if pd.notna(x) else "", subset=pd.IndexSlice[_t1_pct_rows, MONTH_ORDER])
                .set_properties(subset=pd.IndexSlice[_t1_pct_rows, :],
                    **{"font-style": "italic", "font-size": "5px", "font-weight": "600"})
            )

        st.dataframe(styled, use_container_width=True, height=min(35 * (len(disp_full.index) + 3), 900))
        st.markdown("<hr>", unsafe_allow_html=True)

        # ── Rolling + YTD table ───────────────────────────────────────────────
        col_roll, _, col_ytd = st.columns([1.2, 0.3, 1.2])

        with col_roll:
            st.markdown(lbl(f"Rolling {flow_label} ({unit_label})", _t1_sub), unsafe_allow_html=True)
            roll_choice = st.radio("Window", ["1m","3m","6m","12m"], index=3, horizontal=True)
            window = {"1m":1,"3m":3,"6m":6,"12m":12}[roll_choice]
            monthly = dff_disp.groupby("DATE")["BAGS"].sum().reset_index().sort_values("DATE")
            monthly["ROLLING"] = monthly["BAGS"].rolling(window).sum()
            fig5 = go.Figure(go.Scatter(
                x=monthly["DATE"], y=monthly["ROLLING"],
                mode="lines", line=dict(color="#4a7fb5", width=1.8),
                fill="tozeroy", fillcolor="rgba(74,127,181,0.07)",
            ))
            fig5.update_traces(hovertemplate=_HT_BAG)
            fig5.update_layout(
                height=CHART_H_LG,
                xaxis=dict(showgrid=False, tickfont=dict(size=9, color=_TC)),
                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                margin=dict(t=4, b=7, l=4, r=4), **_D,
            )
            st.plotly_chart(fig5, use_container_width=True)

        with col_ytd:
            st.markdown(lbl(f"YTD \u00b7 {MONTH_ORDER[0]}\u2013{latest_common_label} ({unit_label})", _t1_sub), unsafe_allow_html=True)
            tbl2 = ytd_disp.copy()
            tbl2["YTD_FMT"] = tbl2["YTD_BAGS"].map(lambda x: f"{x:{_num_fmt}}")
            tbl2["YOY_FMT"] = tbl2["YOY_PCT"].map(lambda x: f"{x:+.1f}%" if pd.notna(x) else "\u2014")
            st.dataframe(
                tbl2[["CROP_YEAR","YTD_FMT","YOY_FMT"]].rename(columns={
                    "CROP_YEAR": "Crop Year",
                    "YTD_FMT":  f"YTD {unit_label} ({MONTH_ORDER[0]}\u2013{latest_common_label})",
                    "YOY_FMT":  "YoY %",
                }),
                use_container_width=True, hide_index=True,
                height=min(35 * (len(tbl2.index) + 2), 900),
            )

        st.markdown("<hr>", unsafe_allow_html=True)

    # =========================================================================
    # DRILLDOWN SUBTAB
    # =========================================================================
    with st_dest:

        dest_fc1, dest_fc2, dest_fc3, dest_fc4, dest_fc5 = st.columns([1.2, 1.2, 1.2, 1.2, 2])

        _dest_all_rep_regions = ["All"] + sorted(df["REPORTER_REGION"].dropna().unique())
        with dest_fc1:
            dest_rep_region = st.selectbox("Reporter Region", _dest_all_rep_regions, index=0, key=f"{_fk}_dest_rep_region")

        _dest_reporters_scope = (
            sorted(df["REPORTER"].dropna().unique()) if dest_rep_region == "All"
            else sorted(df[df["REPORTER_REGION"] == dest_rep_region]["REPORTER"].dropna().unique())
        )
        with dest_fc2:
            _dest_rep_default = (
                [r for r in ["Cote d'Ivoire"] if r in _dest_reporters_scope] if _is_exports
                else [r for r in ["United States"] if r in _dest_reporters_scope]
            ) or _dest_reporters_scope[:1]
            dest_reporters = st.multiselect(
                "Reporter", _dest_reporters_scope, default=_dest_rep_default, key=f"{_fk}_dest_reporters",
            )

        _dest_active_reps = dest_reporters or _dest_reporters_scope
        _dest_df_rep = df[df["REPORTER"].isin(_dest_active_reps)].copy()

        if not _dest_df_rep.empty:
            _dest_ym          = _dest_df_rep["YEAR"] * 100 + _dest_df_rep["MONTH_NUM"]
            _dest_rep_max_ym  = _dest_df_rep.assign(_YM=_dest_ym).groupby("REPORTER")["_YM"].max()
            _dest_common_ym   = int(_dest_rep_max_ym.min())
            _dest_common_cal_m = _dest_common_ym % 100
            _dest_common_cm   = ((_dest_common_cal_m - crop_start_month) % 12) + 1
            _dest_cutoff_label = _CAL_MONTH_NAMES.get(_dest_common_cal_m, "?")
        else:
            _dest_common_cm    = latest_common_num
            _dest_cutoff_label = latest_common_label

        _dest_all_types = sorted(_dest_df_rep["COMMODITY_TAG"].dropna().unique()) if "COMMODITY_TAG" in _dest_df_rep.columns else []
        with dest_fc3:
            dest_tags = st.multiselect("Type", _dest_all_types, default=_dest_all_types, key=f"{_fk}_dest_tags") if _dest_all_types else []

        _dest_all_regions = sorted(_dest_df_rep["REGION"].dropna().unique())
        with dest_fc4:
            dest_partner_region = st.selectbox("Partner Region", ["All"] + _dest_all_regions, index=0, key=f"{_fk}_dest_partner_region")

        _dest_partners_scope = (
            sorted(_dest_df_rep["PARTNER"].dropna().unique()) if dest_partner_region == "All"
            else sorted(_dest_df_rep[_dest_df_rep["REGION"] == dest_partner_region]["PARTNER"].dropna().unique())
        )
        with dest_fc5:
            dest_partners = st.multiselect("Partners", _dest_partners_scope, default=_dest_partners_scope, key=f"{_fk}_dest_partners")

        _dest_all_cy = sorted(_dest_df_rep["CROP_YEAR"].dropna().unique())
        if len(_dest_all_cy) >= 2:
            dest_cy_range = st.select_slider(
                "Crop Year Range", options=_dest_all_cy,
                value=(_dest_all_cy[max(0, len(_dest_all_cy) - 6)], _dest_all_cy[-1]),
                key=f"{_fk}_dest_cy_range",
            )
            dest_active_cy = [cy for cy in _dest_all_cy if dest_cy_range[0] <= cy <= dest_cy_range[1]]
        else:
            dest_active_cy = _dest_all_cy

        dest_basis = st.radio(
            "Basis", ["YTD Basis","Full Year Basis"], horizontal=True, key=f"{_fk}_dest_basis",
            help=f"YTD = {MONTH_ORDER[0]}\u2013{_dest_cutoff_label} (common cutoff)  \u00b7  Full Year = all crop months",
        )

        dest_month_sel = st.multiselect(
            "Filter by Crop Month (optional \u2014 default = all)",
            options=MONTH_ORDER, default=[], key=f"{_fk}_dest_month_sel",
        )
        _dest_active_months = dest_month_sel if dest_month_sel else MONTH_ORDER

        _dest_mask_base = (
            _dest_df_rep["PARTNER"].isin(dest_partners or _dest_partners_scope)
            & _dest_df_rep["CROP_YEAR"].isin(dest_active_cy)
            & _dest_df_rep["CROP_MONTH_NUM"].isin([MONTH_ORDER.index(m) + 1 for m in _dest_active_months])
        )
        if _dest_all_types:
            _dest_mask_base &= _dest_df_rep["COMMODITY_TAG"].isin(dest_tags or _dest_all_types)

        _dest_mask = _dest_mask_base.copy()
        if dest_basis == "YTD Basis":
            _dest_mask &= _dest_df_rep["CROP_MONTH_NUM"] <= _dest_common_cm

        dest_dff      = _dest_df_rep[_dest_mask].copy()
        dest_dff_roll = _dest_df_rep[_dest_mask_base].copy()

        if dest_month_sel:
            _basis_label = _fmt_list(_dest_active_months)
        elif dest_basis == "YTD Basis":
            _basis_label = f"YTD {MONTH_ORDER[0]}\u2013{_dest_cutoff_label}"
        else:
            _basis_label = "Full Year"

        _dest_type = (" \u00b7 " + _fmt_list(dest_tags or _dest_all_types)) if _dest_all_types else ""
        _dest_reg  = (
            f" \u2192 {dest_partner_region} ({_fmt_list(dest_partners or _dest_partners_scope)})"
            if dest_partner_region != "All" else " \u2192 All Regions"
        )
        _dest_sub = _fmt_list(_dest_active_reps) + _dest_type + _dest_reg + f" \u00b7 {_basis_label}"

        if dest_dff.empty:
            st.info("No data for the current selection.")
        else:
            _dest_reg_agg = dest_dff.groupby(["CROP_YEAR","REGION"])["BAGS"].sum().reset_index()
            _dest_piv     = _dest_reg_agg.pivot(index="CROP_YEAR", columns="REGION", values="BAGS").fillna(0).sort_index()
            _dest_regions_ord = _dest_piv.sum().sort_values(ascending=False).index.tolist()
            _dest_piv     = _dest_piv[_dest_regions_ord]
            _dest_piv_pct = _dest_piv.div(_dest_piv.sum(axis=1), axis=0) * 100
            _dest_cya     = _dest_piv.index.tolist()

            _drill_key = f"{_fk}_dest_drill"
            if _drill_key not in st.session_state:
                st.session_state[_drill_key] = None

            _reg_color = {reg: _PAL[i % len(_PAL)] for i, reg in enumerate(_dest_regions_ord)}

            _dest_view = st.radio("View", ["By Year","By Month"], horizontal=True, key=f"{_fk}_dest_view")
            _show_all_partners = st.checkbox("Show all partners", key=f"{_fk}_dest_show_all_partners", value=False, help="Show partner breakdown for all regions without clicking a bar")

            if _dest_view == "By Year":
                dc_l, dc_r = st.columns(2)
                with dc_l:
                    st.markdown(lbl(f"{', '.join(_dest_active_reps)} \u2192 {_dest_noun}s \u00b7 {_basis_label} ({unit_label})", _dest_sub), unsafe_allow_html=True)
                    fig_dest_abs = go.Figure()
                    for i, reg in enumerate(_dest_regions_ord):
                        _drill_active = st.session_state[_drill_key]
                        _is_faded = _drill_active is not None and _drill_active["region"] != reg
                        fig_dest_abs.add_trace(go.Bar(
                            x=_dest_cya, y=_dest_piv[reg].tolist(),
                            name=reg, marker_color=_reg_color[reg],
                            opacity=0.35 if _is_faded else 1.0,
                            customdata=[[reg]] * len(_dest_cya),
                        ))
                    fig_dest_abs.update_traces(hovertemplate=_HT_BAG)
                    fig_dest_abs.update_layout(
                        barmode="stack", height=CHART_H * 2,
                        xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=9, color=_TC)),
                        yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                        margin=dict(t=25, b=7, l=4, r=4), clickmode="event", **_D,
                    )
                    _abs_sel = st.plotly_chart(fig_dest_abs, use_container_width=True, on_select="rerun", key=f"{_fk}_dest_abs_chart")
                    _abs_pts = (_abs_sel or {}).get("selection", {}).get("points", [])
                    if _abs_pts:
                        _clicked_reg = _dest_regions_ord[_abs_pts[0].get("curve_number", 0)]
                        _clicked_cy  = str(_abs_pts[0].get("x", ""))
                        _cur = st.session_state[_drill_key]
                        if _cur and _cur["region"] == _clicked_reg and _cur["cy"] == _clicked_cy:
                            st.session_state[_drill_key] = None
                        else:
                            st.session_state[_drill_key] = {"region": _clicked_reg, "cy": _clicked_cy}
                        st.rerun()
                with dc_r:
                    st.markdown(lbl(f"Share by {_dest_noun} \u00b7 {_basis_label} (%)", _dest_sub), unsafe_allow_html=True)
                    fig_dest_pct = go.Figure()
                    for i, reg in enumerate(_dest_regions_ord):
                        _drill_active = st.session_state[_drill_key]
                        _is_faded = _drill_active is not None and _drill_active["region"] != reg
                        fig_dest_pct.add_trace(go.Bar(
                            x=_dest_cya, y=_dest_piv_pct[reg].tolist(),
                            name=reg, marker_color=_reg_color[reg],
                            opacity=0.35 if _is_faded else 1.0, showlegend=False,
                            text=_dest_piv_pct[reg].map(lambda v: f"{v:.0f}%" if v >= 5 else ""),
                            textposition="inside", textfont=dict(size=7),
                        ))
                    fig_dest_pct.update_traces(hovertemplate=_HT_PCT)
                    fig_dest_pct.update_layout(
                        barmode="stack", height=CHART_H * 2,
                        xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=9, color=_TC)),
                        yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC), ticksuffix="%", range=[0, 100]),
                        margin=dict(t=25, b=7, l=4, r=4), **_D,
                    )
                    st.plotly_chart(fig_dest_pct, use_container_width=True)

            else:  # By Month
                _drill_mo_key = f"{_fk}_dest_drill_mo"
                if _drill_mo_key not in st.session_state:
                    st.session_state[_drill_mo_key] = None
                _dest_cy_mo = st.selectbox("Crop Year", sorted(_dest_cya, reverse=True), key=f"{_fk}_dest_cy_month_sel")
                _dest_mo_agg = (
                    dest_dff[dest_dff["CROP_YEAR"] == _dest_cy_mo]
                    .groupby(["CROP_MONTH_NUM","REGION"])["BAGS"].sum().reset_index()
                )
                _dest_mo_agg["CROP_MONTH"] = _dest_mo_agg["CROP_MONTH_NUM"].map(NUM_TO_MONTH)
                _dest_mo_total = _dest_mo_agg.groupby("CROP_MONTH_NUM")["BAGS"].sum()

                mo_l, mo_r = st.columns(2)
                with mo_l:
                    st.markdown(lbl(f"{', '.join(_dest_active_reps)} \u2192 Regions by Month \u00b7 {_dest_cy_mo} ({unit_label})", _dest_sub), unsafe_allow_html=True)
                    fig_mo_abs = go.Figure()
                    for reg in _dest_regions_ord:
                        _md = _dest_mo_agg[_dest_mo_agg["REGION"] == reg].sort_values("CROP_MONTH_NUM")
                        _drill_mo_active = st.session_state[_drill_mo_key]
                        _is_faded_mo = _drill_mo_active is not None and _drill_mo_active != reg
                        fig_mo_abs.add_trace(go.Bar(
                            x=_md["CROP_MONTH"].tolist(), y=_md["BAGS"].tolist(),
                            name=reg, marker_color=_reg_color[reg],
                            opacity=0.35 if _is_faded_mo else 1.0,
                            customdata=[[reg]] * len(_md),
                        ))
                    fig_mo_abs.update_traces(hovertemplate=_HT_BAG)
                    fig_mo_abs.update_layout(
                        barmode="stack", height=CHART_H * 2,
                        xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                        yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                        margin=dict(t=25, b=7, l=4, r=4), clickmode="event", **_D,
                    )
                    _mo_abs_sel = st.plotly_chart(fig_mo_abs, use_container_width=True, on_select="rerun", key=f"{_fk}_dest_mo_abs_chart")
                    _mo_abs_pts = (_mo_abs_sel or {}).get("selection", {}).get("points", [])
                    if _mo_abs_pts:
                        _clicked_mo_reg = _dest_regions_ord[_mo_abs_pts[0].get("curve_number", 0)]
                        _cur_mo = st.session_state[_drill_mo_key]
                        st.session_state[_drill_mo_key] = None if _cur_mo == _clicked_mo_reg else _clicked_mo_reg
                        st.rerun()
                with mo_r:
                    st.markdown(lbl(f"Share by Region by Month \u00b7 {_dest_cy_mo} (%)", _dest_sub), unsafe_allow_html=True)
                    fig_mo_pct = go.Figure()
                    for reg in _dest_regions_ord:
                        _md = _dest_mo_agg[_dest_mo_agg["REGION"] == reg].sort_values("CROP_MONTH_NUM")
                        _mo_tot = _md["CROP_MONTH_NUM"].map(_dest_mo_total)
                        _mo_pct = (_md["BAGS"].values / _mo_tot.values * 100)
                        _drill_mo_active = st.session_state[_drill_mo_key]
                        fig_mo_pct.add_trace(go.Bar(
                            x=_md["CROP_MONTH"].tolist(), y=_mo_pct.tolist(),
                            name=reg, marker_color=_reg_color[reg], showlegend=False,
                            opacity=0.35 if (_drill_mo_active is not None and _drill_mo_active != reg) else 1.0,
                            text=[f"{v:.0f}%" if v >= 5 else "" for v in _mo_pct],
                            textposition="inside", textfont=dict(size=7),
                        ))
                    fig_mo_pct.update_traces(hovertemplate=_HT_PCT)
                    fig_mo_pct.update_layout(
                        barmode="stack", height=CHART_H * 2,
                        xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                        yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC), ticksuffix="%", range=[0, 100]),
                        margin=dict(t=25, b=7, l=4, r=4), **_D,
                    )
                    st.plotly_chart(fig_mo_pct, use_container_width=True)

                # By Month drill-down
                _drill_mo = st.session_state[_drill_mo_key]
                if _drill_mo or _show_all_partners:
                    _all_mo_mode = _show_all_partners and not _drill_mo
                    if _all_mo_mode:
                        _dmo_color = "#888888"
                        _dmo_r, _dmo_g, _dmo_b = 136, 136, 136
                        _dmo_reg_label = "All Regions"
                        _dmo_banner_title = f"All Regions \u00b7 {_dest_cy_mo} \u00b7 by Month"
                        _dmo_sub_note = "Uncheck 'Show all partners' to clear"
                    else:
                        _dmo_color = _reg_color.get(_drill_mo, _PAL[0])
                        _dmo_r, _dmo_g, _dmo_b = int(_dmo_color[1:3], 16), int(_dmo_color[3:5], 16), int(_dmo_color[5:7], 16)
                        _dmo_reg_label = _drill_mo
                        _dmo_banner_title = f"Drill-down: <b>{_drill_mo}</b> \u00b7 {_dest_cy_mo} \u00b7 by Month"
                        _dmo_sub_note = "Click same segment again to clear"
                    st.markdown(
                        f"<div style='background:rgba({_dmo_r},{_dmo_g},{_dmo_b},0.08);"
                        f"border-left:3px solid {_dmo_color};border-radius:6px;"
                        f"padding:8px 14px;margin:8px 0;display:flex;align-items:center;justify-content:space-between'>"
                        f"<span style='font-size:0.75rem;font-weight:600;color:{_dmo_color}'>"
                        f"{_dmo_banner_title}"
                        f"</span><span style='font-size:0.65rem;color:#8e8e93;font-style:italic'>"
                        f"{_dmo_sub_note}</span></div>",
                        unsafe_allow_html=True,
                    )
                    if _all_mo_mode:
                        _dmo_data = (
                            dest_dff[dest_dff["CROP_YEAR"] == _dest_cy_mo]
                            .groupby(["CROP_MONTH_NUM","PARTNER"])["BAGS"].sum().reset_index()
                        )
                    else:
                        _dmo_data = (
                            dest_dff[(dest_dff["CROP_YEAR"] == _dest_cy_mo) & (dest_dff["REGION"] == _drill_mo)]
                            .groupby(["CROP_MONTH_NUM","PARTNER"])["BAGS"].sum().reset_index()
                        )
                    _dmo_data["CROP_MONTH"] = _dmo_data["CROP_MONTH_NUM"].map(NUM_TO_MONTH)
                    _dmo_top  = _dmo_data.groupby("PARTNER")["BAGS"].sum().nlargest(12).index.tolist()
                    _dmo_data = _dmo_data[_dmo_data["PARTNER"].isin(_dmo_top)]
                    if not _dmo_data.empty:
                        _dmo_piv = (
                            _dmo_data.pivot(index="CROP_MONTH_NUM", columns="PARTNER", values="BAGS")
                            .reindex(range(1, 13)).fillna(0)
                        )
                        _dmo_piv.index = _dmo_piv.index.map(NUM_TO_MONTH)
                        _dmo_piv = _dmo_piv[_dmo_piv.sum().sort_values(ascending=False).index]
                        _dmo_piv_pct = _dmo_piv.div(_dmo_piv.sum(axis=1).replace(0, 1), axis=0) * 100
                        dmo_c1, dmo_c2 = st.columns(2)
                        with dmo_c1:
                            st.markdown(lbl(f"{_dmo_reg_label} Partners by Month \u00b7 {_dest_cy_mo} ({unit_label})", f"{', '.join(_dest_active_reps)} \u00b7 {_dmo_reg_label} \u00b7 {_dest_cy_mo}"), unsafe_allow_html=True)
                            fig_dmo_abs = go.Figure()
                            for j, partner in enumerate(_dmo_piv.columns):
                                fig_dmo_abs.add_trace(go.Bar(
                                    x=_dmo_piv.index.tolist(), y=_dmo_piv[partner].tolist(),
                                    name=partner, marker_color=_PAL[j % len(_PAL)],
                                    text=[f"{v:{_num_fmt}}" if v > 0 else "" for v in _dmo_piv[partner]],
                                    textposition="inside", textfont=dict(size=7),
                                ))
                            fig_dmo_abs.update_traces(hovertemplate=_HT_BAG)
                            fig_dmo_abs.update_layout(barmode="stack", height=CHART_H * 2,
                                xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                                legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                                margin=dict(t=25, b=7, l=4, r=4), **_D)
                            st.plotly_chart(fig_dmo_abs, use_container_width=True)
                        with dmo_c2:
                            st.markdown(lbl(f"{_dmo_reg_label} Partners \u00b7 Share by Month (%)", f"{', '.join(_dest_active_reps)} \u00b7 {_dmo_reg_label} \u00b7 {_dest_cy_mo}"), unsafe_allow_html=True)
                            fig_dmo_pct = go.Figure()
                            for j, partner in enumerate(_dmo_piv_pct.columns):
                                fig_dmo_pct.add_trace(go.Bar(
                                    x=_dmo_piv_pct.index.tolist(), y=_dmo_piv_pct[partner].tolist(),
                                    name=partner, marker_color=_PAL[j % len(_PAL)], showlegend=False,
                                    text=_dmo_piv_pct[partner].map(lambda v: f"{v:.0f}%" if v >= 5 else ""),
                                    textposition="inside", textfont=dict(size=7),
                                ))
                            fig_dmo_pct.update_traces(hovertemplate=_HT_PCT)
                            fig_dmo_pct.update_layout(barmode="stack", height=CHART_H * 2,
                                xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9, color=_TC)),
                                yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC), ticksuffix="%", range=[0, 100]),
                                margin=dict(t=25, b=7, l=4, r=4), **_D)
                            st.plotly_chart(fig_dmo_pct, use_container_width=True)

            # By Year drill-down panel
            _drill = st.session_state[_drill_key]
            if (_drill or _show_all_partners) and _dest_view == "By Year":
                _all_yr_mode = _show_all_partners and not _drill
                if _all_yr_mode:
                    _dr_reg   = None
                    _dr_cy    = None
                    _dr_color = "#888888"
                    _dr_r, _dr_g, _dr_b = 136, 136, 136
                    _dr_reg_label    = "All Regions"
                    _dr_banner_title = "All Regions \u2014 All Partners"
                    _dr_sub_note     = "Uncheck 'Show all partners' to clear"
                else:
                    _dr_reg   = _drill["region"]
                    _dr_cy    = _drill["cy"]
                    _dr_color = _reg_color.get(_dr_reg, _PAL[0])
                    _dr_r, _dr_g, _dr_b = int(_dr_color[1:3], 16), int(_dr_color[3:5], 16), int(_dr_color[5:7], 16)
                    _dr_reg_label    = _dr_reg
                    _dr_banner_title = f"\u21b3 Drill-down: <b>{_dr_reg}</b> \u00b7 {_dr_cy}"
                    _dr_sub_note     = "Click same segment again to clear"
                st.markdown(
                    f"<div style='background:rgba({_dr_r},{_dr_g},{_dr_b},0.08);"
                    f"border-left:3px solid {_dr_color};border-radius:6px;"
                    f"padding:8px 14px;margin:8px 0;display:flex;align-items:center;justify-content:space-between'>"
                    f"<span style='font-size:0.75rem;font-weight:600;color:{_dr_color}'>"
                    f"{_dr_banner_title}"
                    f"</span><span style='font-size:0.65rem;color:#8e8e93;font-style:italic'>"
                    f"{_dr_sub_note}</span></div>",
                    unsafe_allow_html=True,
                )
                if _all_yr_mode:
                    _dr_all_cy = dest_dff.groupby(["PARTNER","CROP_YEAR"])["BAGS"].sum().reset_index()
                    _dr_top    = dest_dff.groupby("PARTNER")["BAGS"].sum().sort_values(ascending=False).head(12).index.tolist()
                else:
                    _dr_all_cy = dest_dff[dest_dff["REGION"] == _dr_reg].groupby(["PARTNER","CROP_YEAR"])["BAGS"].sum().reset_index()
                    _dr_reg_dff = dest_dff[dest_dff["REGION"] == _dr_reg]
                    _dr_top = _dr_reg_dff[_dr_reg_dff["CROP_YEAR"] == _dr_cy].groupby("PARTNER")["BAGS"].sum().sort_values(ascending=False).head(12).index.tolist()
                    # Fallback: if clicked crop year has no data, use latest available year
                    if not _dr_top and not _dr_reg_dff.empty:
                        _dr_fallback_cy = sorted(_dr_reg_dff["CROP_YEAR"].unique())[-1]
                        _dr_top = _dr_reg_dff[_dr_reg_dff["CROP_YEAR"] == _dr_fallback_cy].groupby("PARTNER")["BAGS"].sum().sort_values(ascending=False).head(12).index.tolist()
                _dr_all_cy = _dr_all_cy[_dr_all_cy["PARTNER"].isin(_dr_top)]
                if not _dr_all_cy.empty:
                    _dr_piv = (
                        _dr_all_cy.pivot(index="CROP_YEAR", columns="PARTNER", values="BAGS")
                        .reindex(_dest_cya).fillna(0)
                    )
                    _dr_piv = _dr_piv[_dr_piv.sum().sort_values(ascending=False).index]
                    _dr_piv_pct = _dr_piv.div(_dr_piv.sum(axis=1), axis=0) * 100
                    dd_c1, dd_c2 = st.columns(2)
                    with dd_c1:
                        st.markdown(lbl(f"{_dr_reg_label} Partners \u00b7 All Crop Years ({unit_label})", f"{', '.join(_dest_active_reps)} \u00b7 {_dr_reg_label}"), unsafe_allow_html=True)
                        fig_dd_abs = go.Figure()
                        for j, partner in enumerate(_dr_piv.columns):
                            fig_dd_abs.add_trace(go.Bar(
                                x=_dr_piv.index.tolist(), y=_dr_piv[partner].tolist(),
                                name=partner, marker_color=_PAL[j % len(_PAL)],
                                text=[f"{v:{_num_fmt}}" if v > 0 else "" for v in _dr_piv[partner]],
                                textposition="inside", textfont=dict(size=7),
                            ))
                        fig_dd_abs.update_traces(hovertemplate=_HT_BAG)
                        fig_dd_abs.update_layout(barmode="stack", height=CHART_H * 2,
                            xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=9, color=_TC)),
                            yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                            legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                            margin=dict(t=25, b=7, l=4, r=4), **_D)
                        st.plotly_chart(fig_dd_abs, use_container_width=True)
                    with dd_c2:
                        st.markdown(lbl(f"{_dr_reg_label} Partners \u00b7 Share by Crop Year (%)", f"{', '.join(_dest_active_reps)} \u00b7 {_dr_reg_label}"), unsafe_allow_html=True)
                        fig_dd_pct = go.Figure()
                        for j, partner in enumerate(_dr_piv_pct.columns):
                            fig_dd_pct.add_trace(go.Bar(
                                x=_dr_piv_pct.index.tolist(), y=_dr_piv_pct[partner].tolist(),
                                name=partner, marker_color=_PAL[j % len(_PAL)], showlegend=False,
                                text=_dr_piv_pct[partner].map(lambda v: f"{v:.0f}%" if v >= 5 else ""),
                                textposition="inside", textfont=dict(size=7),
                            ))
                        fig_dd_pct.update_traces(hovertemplate=_HT_PCT)
                        fig_dd_pct.update_layout(barmode="stack", height=CHART_H * 2,
                            xaxis=dict(showgrid=False, tickangle=45, tickfont=dict(size=9, color=_TC)),
                            yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC), ticksuffix="%", range=[0, 100]),
                            margin=dict(t=25, b=7, l=4, r=4), **_D)
                        st.plotly_chart(fig_dd_pct, use_container_width=True)

            # Rolling window expander
            with st.expander("Rolling Window", expanded=False):
                st.markdown("<hr>", unsafe_allow_html=True)
                dest_roll_choice = st.radio("Rolling Window", ["1m","3m","6m","12m"], index=3, horizontal=True, key=f"{_fk}_dest_roll")
                dest_window = {"1m":1,"3m":3,"6m":6,"12m":12}[dest_roll_choice]
                st.markdown(lbl(f"{dest_roll_choice} Rolling {flow_label} by Region ({unit_label})", _dest_sub), unsafe_allow_html=True)
                _dest_roll_agg = dest_dff_roll.groupby(["DATE","REGION"])["BAGS"].sum().reset_index().sort_values("DATE")
                fig_dest_roll = go.Figure()
                for i, reg in enumerate(_dest_regions_ord):
                    _dr = _dest_roll_agg[_dest_roll_agg["REGION"] == reg].sort_values("DATE").copy()
                    _dr["ROLLING"] = _dr["BAGS"].rolling(dest_window).sum()
                    fig_dest_roll.add_trace(go.Scatter(x=_dr["DATE"], y=_dr["ROLLING"], name=reg, mode="lines", line=dict(color=_PAL[i % len(_PAL)], width=1.6)))
                fig_dest_roll.update_traces(hovertemplate=_HT_BAG)
                fig_dest_roll.update_layout(
                    height=280,
                    xaxis=dict(showgrid=False, tickfont=dict(size=9, color=_TC)),
                    yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9, color=_TC)),
                    legend=dict(orientation="h", y=1.02, x=0, font=dict(size=9, color=_TC), bgcolor="rgba(255,255,255,0.9)"),
                    margin=dict(t=25, b=7, l=4, r=4), **_D,
                )
                st.plotly_chart(fig_dest_roll, use_container_width=True)

            # Market share expander
            with st.expander("Destination Region Market Share", expanded=False):
                st.caption(f"% share of each {_dest_noun.lower()} \u00b7 {_basis_label} \u00b7 rows = regions, columns = crop years")
                _spt = _dest_piv_pct.T.copy().loc[_dest_regions_ord]
                _spt_styled = (
                    _spt.style
                    .background_gradient(cmap="Blues", axis=None)
                    .format("{:.1f}%")
                    .set_properties(**{"text-align":"center","font-size":"8px"})
                    .set_table_styles([{"selector":"th","props":[("text-align","center"),("font-size","8px"),("font-weight","600")]}])
                )
                st.dataframe(_spt_styled, use_container_width=True, height=min(35 * (len(_spt) + 2), 400))

                st.markdown("<hr>", unsafe_allow_html=True)
                _piv_drill_reg = st.selectbox(
                    "Partner Market Share \u2014 drill into region",
                    ["\u2014 select \u2014"] + _dest_regions_ord, key="piv_drill_reg",
                )
                if _piv_drill_reg != "\u2014 select \u2014":
                    _pdr_data = (
                        dest_dff[dest_dff["REGION"] == _piv_drill_reg]
                        .groupby(["PARTNER","CROP_YEAR"])["BAGS"].sum().reset_index()
                    )
                    _pdr_top  = _pdr_data.groupby("PARTNER")["BAGS"].sum().nlargest(10).index.tolist()
                    _pdr_data = _pdr_data[_pdr_data["PARTNER"].isin(_pdr_top)]
                    _pdr_piv  = _pdr_data.pivot(index="PARTNER", columns="CROP_YEAR", values="BAGS").fillna(0)
                    _pdr_piv  = _pdr_piv.loc[_pdr_piv.sum(axis=1).sort_values(ascending=False).index]
                    _pdr_piv_pct = _pdr_piv.div(_pdr_piv.sum(axis=0), axis=1) * 100
                    pdr_c1, pdr_c2 = st.columns(2)
                    with pdr_c1:
                        st.markdown(lbl(f"{_piv_drill_reg} Partners \u00b7 Volume ({unit_label})", f"{', '.join(_dest_active_reps)} \u00b7 {_piv_drill_reg}"), unsafe_allow_html=True)
                        st.dataframe(
                            _pdr_piv.style.background_gradient(cmap="Blues", axis=None).format(f"{{:{_num_fmt}}}")
                            .set_properties(**{"text-align":"center","font-size":"8px"})
                            .set_table_styles([{"selector":"th","props":[("text-align","center"),("font-size","8px"),("font-weight","600")]}]),
                            use_container_width=True, height=min(35 * (len(_pdr_piv) + 2), 380),
                        )
                    with pdr_c2:
                        st.markdown(lbl(f"{_piv_drill_reg} Partners \u00b7 % Share within region", f"{', '.join(_dest_active_reps)} \u00b7 {_piv_drill_reg}"), unsafe_allow_html=True)
                        st.dataframe(
                            _pdr_piv_pct.style.background_gradient(cmap="Greens", axis=None).format("{:.1f}%")
                            .set_properties(**{"text-align":"center","font-size":"8px"})
                            .set_table_styles([{"selector":"th","props":[("text-align","center"),("font-size","8px"),("font-weight","600")]}]),
                            use_container_width=True, height=min(35 * (len(_pdr_piv_pct) + 2), 380),
                        )

            # Destination table
            with st.expander("Destination Table \u2014 Top 10 Partners", expanded=False):
                st.caption(f"Values = {_basis_label} {unit_label}  \u00b7  Rows ranked by total  \u00b7  Colour: white (low) \u2192 dark blue (high)")
                _dest_part_agg = dest_dff.groupby(["PARTNER","CROP_YEAR"])["BAGS"].sum().reset_index()
                _dest_top10    = _dest_part_agg.groupby("PARTNER")["BAGS"].sum().nlargest(10).index.tolist()
                _dest_tbl      = (
                    _dest_part_agg[_dest_part_agg["PARTNER"].isin(_dest_top10)]
                    .pivot(index="PARTNER", columns="CROP_YEAR", values="BAGS").fillna(0)
                )
                _dest_tbl = _dest_tbl.loc[_dest_tbl.sum(axis=1).sort_values(ascending=False).index]
                st.dataframe(
                    _dest_tbl.style.background_gradient(cmap="Blues", axis=None).format(f"{{:{_num_fmt}}}")
                    .set_properties(**{"text-align":"center","font-size":"8px"})
                    .set_table_styles([{"selector":"th","props":[("text-align","center"),("font-size","8px"),("font-weight","600")]}]),
                    use_container_width=True, height=min(35 * (len(_dest_tbl.index) + 2), 350),
                )

    st.markdown("<hr>", unsafe_allow_html=True)
    st.caption(f"Cocoa TDM Trade Flow Dashboard  \u00b7  ETG Softs  \u00b7  Unit: {unit_label}  \u00b7  BEQ: Beans\u00d71.00, Liquor\u00d71.22, Butter\u00d72.70  \u00b7  1 Lot = 10 MT")


# =============================================================================
# TAB 2 — SOURCE  (Coming Soon)
# =============================================================================
with tab2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        "<div style='text-align:center;padding:60px 0'>"
        "<span style='font-size:2rem;color:#c8c8cc'>\U0001f6a7</span><br>"
        "<span style='font-size:1.2rem;font-weight:500;color:#3a3a3c'>Source Aggregation</span><br>"
        "<span style='font-size:0.85rem;color:#8e8e93'>Coming Soon \u2014 plug in source data file to activate</span>"
        "</div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# TAB 3 — DIVERGENCE  (Coming Soon)
# =============================================================================
with tab3:
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        "<div style='text-align:center;padding:60px 0'>"
        "<span style='font-size:2rem;color:#c8c8cc'>\U0001f6a7</span><br>"
        "<span style='font-size:1.2rem;font-weight:500;color:#3a3a3c'>Divergence &amp; Nowcast</span><br>"
        "<span style='font-size:0.85rem;color:#8e8e93'>Coming Soon \u2014 requires Source tab data</span>"
        "</div>",
        unsafe_allow_html=True,
    )


# =============================================================================
# TAB 4 \u2014 MIRROR
# =============================================================================
with tab4:
    try:
        _mdf_exp = _load_parquet_raw(FLOW_PATHS["Cocoa Exports"]).copy()
        _mdf_imp = _load_parquet_raw(FLOW_PATHS["Cocoa Imports"]).copy()
        for _mdf in [_mdf_exp, _mdf_imp]:
            _mdf["DATE"] = pd.to_datetime(_mdf[["YEAR","MONTH_NUM"]].rename(columns={"MONTH_NUM":"MONTH"}).assign(DAY=1))
        _mdf_exp = apply_crop_year(_mdf_exp, crop_start_month)
        _mdf_imp = apply_crop_year(_mdf_imp, crop_start_month)
        for _mdf in [_mdf_exp, _mdf_imp]:
            _col = "BEQ" if "BEQ" in _mdf.columns else "QTY1"
            if unit_choice == "k MT (BEQ)":
                _mdf["BAGS"] = _mdf[_col] / 1000
            elif unit_choice == "k MT (Raw)":
                _mdf["BAGS"] = _mdf["QTY1"] / 1000
            elif unit_choice == "Lots (BEQ)":
                _mdf["BAGS"] = _mdf[_col] / 10
            else:
                _mdf["BAGS"] = _mdf["QTY1"] / 10
    except Exception as e:
        st.error(str(e)); st.stop()

    mc1, mc2, mc3, mc4, mc5 = st.columns([1.5, 1.5, 1.5, 1.5, 1])
    _mir_exporters = sorted(_mdf_exp["REPORTER"].dropna().unique())
    with mc1:
        _mir_def_exp = next((r for r in ["Cote d'Ivoire","Ghana"] if r in _mir_exporters), _mir_exporters[0] if _mir_exporters else None)
        _mir_sel_exp = st.selectbox("Exporter (Reporter)", _mir_exporters,
            index=_mir_exporters.index(_mir_def_exp) if _mir_def_exp else 0, key="mir_exporter")
    _mdf_exp_rep   = _mdf_exp[_mdf_exp["REPORTER"] == _mir_sel_exp]
    _mir_exp_parts = sorted(_mdf_exp_rep["PARTNER"].dropna().unique())
    with mc2:
        _mir_def_part = next((p for p in ["Netherlands","Germany","United States of America"] if p in _mir_exp_parts), _mir_exp_parts[0] if _mir_exp_parts else None)
        _mir_sel_imp_partner = st.selectbox("Destination (Partner)", _mir_exp_parts,
            index=_mir_exp_parts.index(_mir_def_part) if _mir_def_part else 0, key="mir_imp_partner")
    _ALIAS = {"United States of America": "United States", "United States": "United States of America"}
    _mir_importers = sorted(_mdf_imp["REPORTER"].dropna().unique())
    with mc3:
        _dest_candidates = [_mir_sel_imp_partner, _ALIAS.get(_mir_sel_imp_partner, "")]
        _mir_def_imp = next((r for r in _dest_candidates if r in _mir_importers), _mir_importers[0] if _mir_importers else None)
        _mir_sel_imp = st.selectbox("Importer (Reporter)", _mir_importers,
            index=_mir_importers.index(_mir_def_imp) if _mir_def_imp else 0, key="mir_importer")
    _mdf_imp_rep   = _mdf_imp[_mdf_imp["REPORTER"] == _mir_sel_imp]
    _mir_imp_parts = sorted(_mdf_imp_rep["PARTNER"].dropna().unique())
    with mc4:
        _mir_def_origin = _mir_sel_exp if _mir_sel_exp in _mir_imp_parts else (_mir_imp_parts[0] if _mir_imp_parts else None)
        _mir_sel_origin = st.selectbox("Origin (Partner)", _mir_imp_parts,
            index=_mir_imp_parts.index(_mir_def_origin) if _mir_def_origin else 0, key="mir_origin")
    with mc5:
        _mir_lag = st.slider("Lag (months)", 0, 4, 1, key="mir_lag")

    _mir_all_tags = sorted(_mdf_exp["COMMODITY_TAG"].dropna().unique()) if "COMMODITY_TAG" in _mdf_exp.columns else []
    if _mir_all_tags:
        _mir_sel_tags = st.multiselect("Commodity Type", _mir_all_tags, default=_mir_all_tags, key="mir_tags")
    else:
        _mir_sel_tags = []

    _mir_direct = _mdf_exp_rep[_mdf_exp_rep["PARTNER"] == _mir_sel_imp_partner].copy()
    _mir_mirror = _mdf_imp_rep[_mdf_imp_rep["PARTNER"] == _mir_sel_origin].copy()
    if _mir_all_tags:
        _mir_direct = _mir_direct[_mir_direct["COMMODITY_TAG"].isin(_mir_sel_tags or _mir_all_tags)]
        _mir_mirror = _mir_mirror[_mir_mirror["COMMODITY_TAG"].isin(_mir_sel_tags or _mir_all_tags)]

    def _apply_lag(df, lag):
        if lag == 0 or df.empty: return apply_crop_year(df.copy(), crop_start_month)
        d = df.copy()
        total = d["YEAR"] * 12 + (d["MONTH_NUM"] - 1) - lag
        d["YEAR"] = total // 12; d["MONTH_NUM"] = total % 12 + 1
        d["DATE"] = pd.to_datetime(d[["YEAR","MONTH_NUM"]].rename(columns={"MONTH_NUM":"MONTH"}).assign(DAY=1))
        return apply_crop_year(d, crop_start_month)

    _mir_direct = apply_crop_year(_mir_direct, crop_start_month)
    _mir_mirror = apply_crop_year(_mir_mirror, crop_start_month)
    _mir_mirror_shifted = _apply_lag(_mir_mirror.copy(), _mir_lag)

    def _mir_agg(df):
        if df.empty: return pd.DataFrame(columns=["CROP_YEAR","CROP_MONTH_NUM","BAGS"])
        return df.groupby(["CROP_YEAR","CROP_MONTH_NUM"])["BAGS"].sum().reset_index()

    _agg_dir = _mir_agg(_mir_direct); _agg_mir = _mir_agg(_mir_mirror); _agg_ms = _mir_agg(_mir_mirror_shifted)
    _mir_cys = sorted(set(_agg_dir["CROP_YEAR"].tolist()) | set(_agg_mir["CROP_YEAR"].tolist()))
    if not _mir_cys:
        st.warning("No data found for this combination."); st.stop()

    _mir_latest_cy = _mir_cys[-1]; _mir_prev_cy = _mir_cys[-2] if len(_mir_cys) >= 2 else None
    def _mir_cy_color(cy):
        if cy == _mir_latest_cy: return _TC, 2.5
        if cy == _mir_prev_cy:   return "#c0392b", 2.0
        return None, 1.4

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown(f"### {_mir_sel_exp} &rarr; {_mir_sel_imp_partner} &nbsp;<span style='font-size:0.85rem;font-weight:400;color:#6e6e73'>Direct vs Mirror{f'  \u00b7  Mirror shifted -{_mir_lag}m' if _mir_lag > 0 else ''}</span>", unsafe_allow_html=True)
    st.markdown("<hr>", unsafe_allow_html=True)

    if len(_mir_cys) >= 2:
        _mir_cy_range = st.select_slider("Crop year range", options=_mir_cys, value=(_mir_cys[-2], _mir_cys[-1]), key="mir_cy_range")
        _mir_sel_cys = [cy for cy in _mir_cys if _mir_cy_range[0] <= cy <= _mir_cy_range[1]]
    else:
        _mir_sel_cys = _mir_cys

    st.markdown(lbl(f"Direct vs Mirror ({unit_label}) \u00b7 Monthly by Crop Year", f"{_mir_sel_exp} \u2192 {_mir_sel_imp_partner}  \u00b7  Mirror lag: {_mir_lag}m"), unsafe_allow_html=True)
    fig_mir = go.Figure(); _pal_i = 0
    for cy in sorted(_mir_sel_cys):
        color, width = _mir_cy_color(cy)
        if color is None: color = _PAL[_pal_i % len(_PAL)]; _pal_i += 1
        _d_dir = _agg_dir[_agg_dir["CROP_YEAR"] == cy].sort_values("CROP_MONTH_NUM").copy()
        _d_mir = _agg_mir[_agg_mir["CROP_YEAR"] == cy].sort_values("CROP_MONTH_NUM").copy()
        _d_ms  = _agg_ms[_agg_ms["CROP_YEAR"] == cy].sort_values("CROP_MONTH_NUM").copy()
        for _d in [_d_dir, _d_mir, _d_ms]: _d["CROP_MONTH"] = _d["CROP_MONTH_NUM"].map(NUM_TO_MONTH)
        if not _d_dir.empty: fig_mir.add_trace(go.Scatter(x=_d_dir["CROP_MONTH"], y=_d_dir["BAGS"], name=f"{cy} Direct", mode="lines+markers", line=dict(color=color, width=width), marker=dict(size=4), legendgroup=cy))
        if not _d_mir.empty: fig_mir.add_trace(go.Scatter(x=_d_mir["CROP_MONTH"], y=_d_mir["BAGS"], name=f"{cy} Mirror", mode="lines+markers", line=dict(color=color, width=width, dash="dash"), marker=dict(size=4, symbol="square"), legendgroup=cy))
        if _mir_lag > 0 and not _d_ms.empty: fig_mir.add_trace(go.Scatter(x=_d_ms["CROP_MONTH"], y=_d_ms["BAGS"], name=f"{cy} Mirror \u2212{_mir_lag}m", mode="lines+markers", line=dict(color=color, width=width, dash="dot"), marker=dict(size=4, symbol="diamond"), legendgroup=cy))
    fig_mir.update_traces(hovertemplate=_HT_BAG)
    fig_mir.update_layout(height=CHART_H*2, xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9,color=_TC)), yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9,color=_TC)), legend=dict(orientation="h", y=-0.12, x=0, font=dict(size=9,color=_TC), bgcolor="rgba(255,255,255,0.9)"), margin=dict(t=10,b=100,l=4,r=4), **_D)
    st.plotly_chart(fig_mir, use_container_width=True, key="mir_fig_mir")
    st.markdown("<hr>", unsafe_allow_html=True)

    _mir_div_cy = st.selectbox("Crop Year for Divergence Analysis", sorted(_mir_sel_cys, reverse=True), key="mir_div_cy")
    _mir_idx = list(range(1,13)); _months_lbl = [NUM_TO_MONTH[i] for i in _mir_idx]
    def _to_series(agg_df, cy):
        return agg_df[agg_df["CROP_YEAR"]==cy].set_index("CROP_MONTH_NUM")["BAGS"].reindex(_mir_idx, fill_value=0)
    _s_dir = _to_series(_agg_dir, _mir_div_cy); _s_mir = _to_series(_agg_mir, _mir_div_cy); _s_ms = _to_series(_agg_ms, _mir_div_cy)
    _div_raw = (_s_dir - _s_mir).values; _div_shifted = (_s_dir - _s_ms).values
    div_c1, div_c2 = st.columns(2)
    with div_c1:
        st.markdown(lbl(f"Direct \u2212 Mirror ({unit_label}) \u00b7 No Lag", f"{_mir_div_cy}  \u00b7  +ve = exporter reported more"), unsafe_allow_html=True)
        fig_div1 = go.Figure(go.Bar(x=_months_lbl, y=_div_raw, marker_color=["#c0392b" if v>0 else "#4a7fb5" for v in _div_raw], text=[f"{v:+{_num_fmt}}" for v in _div_raw], textposition="outside", textfont=dict(size=8)))
        fig_div1.add_hline(y=0, line_color="#cccccc", line_width=1)
        fig_div1.update_layout(height=CHART_H, xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9,color=_TC)), yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9,color=_TC)), margin=dict(t=10,b=7,l=4,r=4), **_D)
        st.plotly_chart(fig_div1, use_container_width=True, key="mir_fig_div1")
    with div_c2:
        st.markdown(lbl(f"Direct \u2212 Mirror\u2212{_mir_lag}m ({unit_label}) \u00b7 Lag Applied", f"{_mir_div_cy}  \u00b7  mirror shifted back {_mir_lag} month(s)"), unsafe_allow_html=True)
        fig_div2 = go.Figure(go.Bar(x=_months_lbl, y=_div_shifted, marker_color=["#c0392b" if v>0 else "#4a7fb5" for v in _div_shifted], text=[f"{v:+{_num_fmt}}" for v in _div_shifted], textposition="outside", textfont=dict(size=8)))
        fig_div2.add_hline(y=0, line_color="#cccccc", line_width=1)
        fig_div2.update_layout(height=CHART_H, xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9,color=_TC)), yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9,color=_TC)), margin=dict(t=10,b=7,l=4,r=4), **_D)
        st.plotly_chart(fig_div2, use_container_width=True, key="mir_fig_div2")
    st.caption("\u2191 Red = exporter reported more than importer recorded  \u00b7  Blue = importer recorded more")
    st.markdown("<hr>", unsafe_allow_html=True)

    _mir_cum_cy = st.selectbox("Crop Year for Cumulative View", sorted(_mir_sel_cys, reverse=True), key="mir_cum_cy")
    st.markdown(lbl(f"Cumulative Comparison ({unit_label}) \u00b7 {_mir_cum_cy}", f"{_mir_sel_exp} \u2192 {_mir_sel_imp_partner}  \u00b7  Direct vs Mirror vs Mirror\u2212{_mir_lag}m"), unsafe_allow_html=True)
    def _cum_trace(agg_df, cy):
        d = agg_df[agg_df["CROP_YEAR"]==cy].sort_values("CROP_MONTH_NUM").copy()
        d["CROP_MONTH"] = d["CROP_MONTH_NUM"].map(NUM_TO_MONTH); d["CUM"] = d["BAGS"].cumsum(); return d
    _c_dir = _cum_trace(_agg_dir, _mir_cum_cy); _c_mir = _cum_trace(_agg_mir, _mir_cum_cy); _c_ms = _cum_trace(_agg_ms, _mir_cum_cy)
    fig_cum = go.Figure()
    if not _c_dir.empty: fig_cum.add_trace(go.Scatter(x=_c_dir["CROP_MONTH"], y=_c_dir["CUM"], name="Direct", mode="lines+markers", line=dict(color=_TC, width=2.5), marker=dict(size=5)))
    if not _c_mir.empty: fig_cum.add_trace(go.Scatter(x=_c_mir["CROP_MONTH"], y=_c_mir["CUM"], name="Mirror", mode="lines+markers", line=dict(color="#4a7fb5", width=2.0, dash="dash"), marker=dict(size=5, symbol="square")))
    if _mir_lag > 0 and not _c_ms.empty: fig_cum.add_trace(go.Scatter(x=_c_ms["CROP_MONTH"], y=_c_ms["CUM"], name=f"Mirror \u2212{_mir_lag}m", mode="lines+markers", line=dict(color="#e07b39", width=2.0, dash="dot"), marker=dict(size=5, symbol="diamond")))
    fig_cum.update_traces(hovertemplate=_HT_BAG)
    fig_cum.update_layout(height=CHART_H*2, xaxis=dict(categoryorder="array", categoryarray=MONTH_ORDER, showgrid=False, tickfont=dict(size=9,color=_TC)), yaxis=dict(showgrid=True, gridcolor=_GC, tickfont=dict(size=9,color=_TC)), legend=dict(orientation="h", y=-0.1, x=0, font=dict(size=9,color=_TC), bgcolor="rgba(255,255,255,0.9)"), margin=dict(t=10,b=80,l=4,r=4), **_D)
    st.plotly_chart(fig_cum, use_container_width=True, key="mir_fig_cum")
    st.markdown("<hr>", unsafe_allow_html=True)
    st.caption(f"Mirror Tab  \u00b7  ETG Softs  \u00b7  Direct = {_mir_sel_exp} exports \u2192 {_mir_sel_imp_partner}  \u00b7  Mirror = {_mir_sel_imp} imports \u2190 {_mir_sel_origin}  \u00b7  Lag = {_mir_lag}m  \u00b7  Unit: {unit_label}")
