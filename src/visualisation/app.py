"""Streamlit dashboard of the Smart City Traffic Data Platform (spec T09/AC09).

Shows source coverage, processing volume, quality indicators, batch results, streaming window metrics,
a performance comparison, one complete pipeline run and the abnormal / recovery cases. Everything comes
from recorded runs (committed evidence/ and outputs/); each panel names its file and run. The runs
shown are chosen in configs/dashboard.yaml.

Run:  pip install -r requirements-dashboard.txt
      streamlit run src/visualisation/app.py        # http://localhost:8501
"""
from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.visualisation import data as D  # noqa: E402

st.set_page_config(page_title="Smart City Traffic Data Platform", layout="wide")
CFG = D.config()
C1, C2 = CFG["colors"]["series_1"], CFG["colors"]["series_2"]


def source(text: str) -> None:
    st.caption(f"Source: `{text}`")


def table(rows, label="Table view") -> None:
    with st.expander(label):
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


st.title("Smart City Traffic Data Platform")
st.markdown("Real sources → HDFS Bronze/Silver/Gold → Spark → Kafka/Flink (**historical event replay**, "
            "not live data) → quality → data products. All figures below are read from recorded runs.")

tabs = st.tabs(["Pipeline", "Sources", "Quality", "Batch results", "Streaming", "Performance",
                "Abnormal cases"])

# ---------------- Pipeline: one complete run, processing volume ----------------
with tabs[0]:
    counts, src = D.layer_counts(CFG)
    rows = D.layer_table(counts)
    st.subheader("Processing volume per layer")
    m = st.columns(4)
    m[0].metric("Bronze records (all sources)", f"{sum(r['bronze'] for r in rows):,}")
    m[1].metric("NYC trips accepted in Silver", f"{counts['nyc_tlc']['silver_records_by_outcome']['accepted']:,}")
    m[2].metric("UCI hours in Gold", f"{counts['uci_metro']['gold_rows']:,}")
    m[3].metric("Data products", "4")
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    source(src)
    st.subheader("One complete pipeline run (the runs shown)")
    st.dataframe(pd.DataFrame([{"stage": k, "run_id": v} for k, v in CFG["runs"].items()]),
                 use_container_width=True, hide_index=True)
    source("configs/dashboard.yaml; evidence/AC0x/<run_id>/")

# ---------------- Sources ----------------
with tabs[1]:
    rows, src = D.sources(CFG)
    st.subheader("Source coverage (second full ingestion: every file unchanged, re-ingestion check)")
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    source(src)
    st.markdown("Origins, licences, fields and update behaviour: `docs/source_inventory.md`. "
                "NYC TLC is a different city and is never joined to UCI.")

# ---------------- Quality ----------------
with tabs[2]:
    outcomes, rules, srcs = D.quality(CFG)
    st.subheader("Silver outcomes per dataset")
    od = pd.DataFrame(outcomes).pivot(index="dataset", columns="outcome", values="records").fillna(0).astype(int)
    od["reconciled_to_bronze"] = [next(o["reconciled"] for o in outcomes if o["dataset"] == d) for d in od.index]
    st.dataframe(od, use_container_width=True)
    st.subheader("Records per quality rule (rules that fired)")
    rd = pd.DataFrame(rules)
    pick = st.selectbox("Dataset", sorted(rd["dataset"].unique()), index=1)
    sel = rd[rd["dataset"] == pick]
    st.altair_chart(alt.Chart(sel).mark_bar(color=C1, cornerRadiusEnd=4).encode(
        x=alt.X("records:Q", title="records"), y=alt.Y("rule:N", sort="-x", title=None),
        tooltip=["rule", alt.Tooltip("records:Q", format=",")]).properties(height=28 * max(len(sel), 2)),
        use_container_width=True)
    table(sel.to_dict("records"))
    for s in srcs:
        source(s)
    st.markdown("Rules, severities and lineage: `docs/quality.md`. Flagged records stay accepted; "
                "quarantined/rejected ones never reach Gold or products.")

# ---------------- Batch results ----------------
with tabs[3]:
    a1, s1 = D.analytics(CFG, "A1_hourly_pattern")
    df = pd.DataFrame(a1).astype({"local_hour": int, "avg_volume": float})
    st.subheader("A1 · Mean hourly volume at ATR 301 (I-94 westbound), weekday vs weekend")
    st.altair_chart(alt.Chart(df).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=40)).encode(
        x=alt.X("local_hour:O", title="local hour"), y=alt.Y("avg_volume:Q", title="vehicles / hour"),
        color=alt.Color("day_type:N", scale=alt.Scale(domain=["weekday", "weekend"], range=[C1, C2]),
                        legend=alt.Legend(title=None, orient="top")),
        tooltip=["day_type", "local_hour", alt.Tooltip("avg_volume:Q", format=",.0f")]), use_container_width=True)
    table(a1)
    source(s1)
    c = st.columns(2)
    with c[0]:
        a2, s2 = D.analytics(CFG, "A2_day_type")
        st.subheader("A2 · Daily volume by day type (full days)")
        d2 = pd.DataFrame(a2).astype({"avg_daily_volume": float})
        st.altair_chart(alt.Chart(d2).mark_bar(color=C1, cornerRadiusEnd=4).encode(
            x=alt.X("avg_daily_volume:Q", title="mean vehicles / day"), y=alt.Y("day_type:N", title=None, sort="-x"),
            tooltip=["day_type", "full_days", alt.Tooltip("avg_daily_volume:Q", format=",.0f")]), use_container_width=True)
        table(a2)
        source(s2)
    with c[1]:
        a3, s3 = D.analytics(CFG, "A3_weather_impact")
        st.subheader("A3 · Volume vs dry hours at the same local hour (ERA5)")
        d3 = pd.DataFrame(a3).astype({"volume_vs_dry_same_hour": float})
        st.altair_chart(alt.Chart(d3).mark_bar(color=C1, cornerRadiusEnd=4).encode(
            x=alt.X("volume_vs_dry_same_hour:Q", title="ratio to dry hours (1 = same)", scale=alt.Scale(domain=[0, 1.1])),
            y=alt.Y("weather_class:N", title=None, sort="x"),
            tooltip=["weather_class", "hours", "volume_vs_dry_same_hour"]), use_container_width=True)
        table(a3)
        source(s3)
        st.caption("Weather data by Open-Meteo.com (ERA5 reanalysis, CC BY 4.0) — not a road-side observation.")
    st.subheader("Daily traffic at ATR 301 (gaps = no UCI records)")
    dd, sd = D.daily_traffic(CFG)
    dd = pd.DataFrame(dd).astype({"total_volume": int, "hours": int})
    dd["local_date"] = pd.to_datetime(dd["local_date"])
    full = dd[dd["hours"] == 24]
    st.altair_chart(alt.Chart(full).mark_line(color=C1, strokeWidth=1).encode(
        x=alt.X("local_date:T", title=None), y=alt.Y("total_volume:Q", title="vehicles / day (full days)"),
        tooltip=[alt.Tooltip("local_date:T"), alt.Tooltip("total_volume:Q", format=",")]), use_container_width=True)
    source(sd)
    c = st.columns(2)
    with c[0]:
        a4, s4 = D.analytics(CFG, "A4_nyc_boroughs")
        st.subheader("A4 · NYC yellow-taxi pickups by borough")
        st.dataframe(pd.DataFrame(a4), use_container_width=True, hide_index=True)
        source(s4)
    with c[1]:
        a5, s5 = D.analytics(CFG, "A5_nyc_top_zones")
        st.subheader("A5 · Ten busiest pickup zones")
        st.dataframe(pd.DataFrame(a5), use_container_width=True, hide_index=True)
        source(s5)

# ---------------- Streaming ----------------
with tabs[4]:
    rv, srv = D.replay_verify(CFG)
    v = rv["verify"]
    st.subheader("Historical event replay → Flink 6 h event-time windows")
    m = st.columns(4)
    m[0].metric("Events replayed", f"{v['replay_events']:,}")
    m[1].metric("Windows", f"{v['final_windows']:,}")
    m[2].metric("Equal to oracle", "yes" if v["comparison"]["match"] else "NO")
    m[3].metric("Flink job", f"{rv['job_duration_s']:.0f} s")
    source(srv)
    wm, swm = D.window_metrics(CFG)
    w = pd.DataFrame(wm).astype({"volume_sum": int, "event_count": int, "completeness": float})
    w["window_start_utc"] = pd.to_datetime(w["window_start_utc"])
    years = sorted(w["window_start_utc"].dt.year.unique())
    yr = st.select_slider("Year", options=years, value=years[min(4, len(years) - 1)])
    wy = w[w["window_start_utc"].dt.year == yr]
    st.altair_chart(alt.Chart(wy).mark_line(color=C1, strokeWidth=1).encode(
        x=alt.X("window_start_utc:T", title="window start (UTC)"), y=alt.Y("volume_sum:Q", title="vehicles per 6 h window"),
        tooltip=[alt.Tooltip("window_start_utc:T"), alt.Tooltip("volume_sum:Q", format=","), "event_count",
                 "completeness", "final_firing"]), use_container_width=True)
    comp = w.groupby("event_count").size().reset_index(name="windows")
    st.altair_chart(alt.Chart(comp).mark_bar(color=C1, cornerRadiusEnd=4).encode(
        x=alt.X("event_count:O", title="hours present in the 6 h window (completeness)"), y=alt.Y("windows:Q"),
        tooltip=["event_count", "windows"]).properties(height=200), use_container_width=True)
    source(swm + " (product traffic_window_metrics_v1)")
    st.markdown("Watermark = max event time − 60 min; allowed lateness 6 h; results are upserted by "
                "(station, window). Details: `docs/streaming.md`.")

# ---------------- Performance ----------------
with tabs[5]:
    b, sb = D.benchmark(CFG)
    bd = pd.DataFrame(b)
    st.subheader("Strategy pairs: median of 3 timed runs (after 1 warm-up)")
    pair = st.selectbox("Pair", sorted(bd["pair"].unique()))
    sel = bd[bd["pair"] == pair].copy()
    sel["label"] = sel["query"] + " · " + sel["config"]
    st.altair_chart(alt.Chart(sel).mark_bar(color=C1, cornerRadiusEnd=4).encode(
        x=alt.X("median_s:Q", title="median seconds"), y=alt.Y("label:N", title=None),
        tooltip=["config", "query", "median_s", "warmup_s", "runs_s", "mb_read", alt.Tooltip("rows_read:Q", format=",")]),
        use_container_width=True)
    st.dataframe(sel.drop(columns=["label"]), use_container_width=True, hide_index=True)
    source(sb)
    st.markdown("Conclusions, plans and warm-up effects: `docs/performance.md`.")

# ---------------- Abnormal cases ----------------
with tabs[6]:
    ec, sec = D.edge_case(CFG)
    st.subheader("Late, duplicate and malformed events (test topics, synthetic station TEST-301)")
    st.write("Passed:" , "yes" if ec["passed"] else "NO")
    st.dataframe(pd.DataFrame([{"window_start_utc": wk, **r} for wk, rs in ec["windows"].items() for r in rs]),
                 use_container_width=True, hide_index=True)
    st.dataframe(pd.DataFrame([{"dlq_reason": k, "records": n} for k, n in ec["dlq_reasons"].items()]),
                 use_container_width=True, hide_index=True)
    source(sec)
    rc, src_rc = D.recovery(CFG)
    st.subheader("Failure and recovery: Flink TaskManager killed mid-replay")
    st.dataframe(pd.DataFrame(rc["timeline"]), use_container_width=True, hide_index=True)
    vc = rc["verify"]
    st.write(f"After restart: {vc['final_windows']:,} windows, results equal to oracle: "
             f"**{'yes' if vc['comparison']['match'] else 'NO'}**; "
             f"{vc['window_results']['re_emitted']} window results re-emitted (at-least-once), removed by upsert.")
    source(src_rc)
    st.subheader("Failed and annotated runs kept as evidence")
    st.dataframe(pd.DataFrame(D.failed_runs()), use_container_width=True, hide_index=True)
    source("evidence/AC*/<run>/summary.txt, FAILED_RUN.md, NOTE.md")

st.divider()
st.caption("Data: UCI Metro Interstate Traffic Volume (CC BY 4.0, DOI 10.24432/C5X60B) · Weather data by "
           "Open-Meteo.com (CC BY 4.0; ERA5, Copernicus C3S) · NYC TLC Trip Records (NYC Open Data) · KITTI "
           "(CC BY-NC-SA 3.0; metadata only, images not redistributed).")
