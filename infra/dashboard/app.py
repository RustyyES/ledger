"""Ledger data-quality dashboard.

This is an OPERATIONS dashboard, not a business intelligence one. It answers
"can I trust the numbers right now", not "how is revenue doing" -- the metrics
API answers the second question and this one exists to tell you whether to
believe it.

The distinction drives every panel: each shows a series over time rather than a
current value, because the question an on-call engineer actually has is "when
did this start", and a single number cannot answer it.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

WAREHOUSE = os.environ.get("DASHBOARD_WAREHOUSE_PATH", "/data/warehouse/ledger.duckdb")
REFRESH_SECONDS = int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "60"))

st.set_page_config(page_title="Ledger · Data Quality", page_icon="📊", layout="wide")


@st.cache_data(ttl=REFRESH_SECONDS)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Query the warehouse read-only.

    A NEW connection per query, cached for the refresh interval. Holding a
    connection open would take a file lock and block the dbt rebuild -- the
    dashboard would prevent the warehouse it monitors from being refreshed.
    """
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        return con.execute(sql, list(params)).df()


def safe(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Query, returning an empty frame if the relation does not exist yet.

    A dashboard that crashes because a mart has not been built is a dashboard
    nobody opens during an incident -- which is exactly when it is needed.
    """
    try:
        return q(sql, params)
    except duckdb.Error as exc:
        st.warning(f"unavailable: {str(exc)[:160]}")
        return pd.DataFrame()


st.title("Ledger · Data Quality")
st.caption(
    "Operational health of the pipeline. Business metrics live in the metrics "
    "API; this page exists to tell you whether to trust them."
)

# --------------------------------------------------------------------------- #
# Header: the four numbers worth seeing first.
# --------------------------------------------------------------------------- #

freshness = safe("""
    select max(_ingested_at) as last_ingested
    from marts.fct_payments
""")

col1, col2, col3, col4 = st.columns(4)

if not freshness.empty and pd.notna(freshness.iloc[0]["last_ingested"]):
    last = pd.to_datetime(freshness.iloc[0]["last_ingested"], utc=True)
    hours = (datetime.now(UTC) - last.to_pydatetime()).total_seconds() / 3600
    col1.metric(
        "Data freshness",
        f"{hours:.1f}h",
        delta="stale" if hours > 26 else "current",
        delta_color="inverse" if hours > 26 else "normal",
    )
else:
    col1.metric("Data freshness", "unknown")

counts = safe("""
    select
        (select count(*) from marts.fct_orders)               as orders,
        (select count(*) from marts.fct_payments)             as payments,
        (select count(*) from marts.dim_customer)             as customer_versions,
        (select count(distinct customer_id) from marts.dim_customer) as customers
""")
if not counts.empty:
    r = counts.iloc[0]
    col2.metric("Orders", f"{int(r['orders']):,}")
    col3.metric("Customers", f"{int(r['customers']):,}")
    col4.metric(
        "SCD2 versions",
        f"{int(r['customer_versions']):,}",
        delta=f"+{int(r['customer_versions']) - int(r['customers']):,} from changes",
    )

st.divider()

# --------------------------------------------------------------------------- #
# Source-to-warehouse row count delta, per table.
# --------------------------------------------------------------------------- #

st.subheader("Source → warehouse row count delta")
st.caption(
    "Staging reads the raw CDC layer; marts are built from staging. A non-zero "
    "delta means a join is losing or duplicating rows — the failure mode that "
    "leaves every schema test green."
)

pairs = [
    ("orders", "staging.stg_orders", "marts.fct_orders"),
    ("payments", "staging.stg_payments", "marts.fct_payments"),
    ("subscription_events", "staging.stg_subscription_events", "marts.fct_subscription_events"),
]
rows = []
for label, staging_rel, mart_rel in pairs:
    df = safe(f"""
        select
            (select count(*) from {staging_rel}) as staging_rows,
            (select count(*) from {mart_rel})    as mart_rows
    """)
    if df.empty:
        continue
    s, m = int(df.iloc[0]["staging_rows"]), int(df.iloc[0]["mart_rows"])
    rows.append(
        {
            "table": label,
            "staging": s,
            "mart": m,
            "delta": m - s,
            "delta_pct": round((m - s) / s * 100, 3) if s else 0.0,
            "status": "OK" if s == m else "DRIFT",
        }
    )

if rows:
    delta_df = pd.DataFrame(rows)
    st.dataframe(
        delta_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "delta_pct": st.column_config.NumberColumn("delta %", format="%.3f%%"),
        },
    )
    if (delta_df["status"] == "DRIFT").any():
        st.error("Row count drift detected — see quality_dag/rowcount_drift_check.")

st.divider()

# --------------------------------------------------------------------------- #
# Quality observations over time (written by quality_dag).
# --------------------------------------------------------------------------- #

st.subheader("Quality metrics over time")
st.caption("Written by `quality_dag` every 4 hours. The trend is the point.")

obs = safe("""
    select observed_at, metric, subject, value
    from ops.quality_observations
    where observed_at > now() - interval '14 days'
    order by observed_at
""")

if obs.empty:
    st.info("No observations yet — `quality_dag` writes these every 4 hours.")
else:
    tab1, tab2, tab3 = st.tabs(["Freshness lag", "dbt test pass rate", "Row count drift"])

    with tab1:
        f = obs[obs["metric"] == "freshness_hours"]
        if not f.empty:
            fig = px.line(
                f,
                x="observed_at",
                y="value",
                color="subject",
                labels={"value": "hours behind", "observed_at": ""},
            )
            fig.add_hline(
                y=6, line_dash="dash", line_color="red", annotation_text="error threshold"
            )
            fig.add_hline(
                y=2, line_dash="dot", line_color="orange", annotation_text="warn threshold"
            )
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        t = obs[obs["metric"] == "dbt_test_pass_rate"]
        if not t.empty:
            fig = px.line(
                t, x="observed_at", y="value", labels={"value": "pass rate %", "observed_at": ""}
            )
            fig.update_yaxes(range=[0, 105])
            fig.add_hline(y=100, line_dash="dash", line_color="green")
            st.plotly_chart(fig, use_container_width=True)

    with tab3:
        d = obs[obs["metric"] == "rowcount_drift_pct"]
        if not d.empty:
            fig = px.line(
                d,
                x="observed_at",
                y="value",
                color="subject",
                labels={"value": "drift %", "observed_at": ""},
            )
            fig.add_hline(y=2, line_dash="dash", line_color="red")
            st.plotly_chart(fig, use_container_width=True)

st.divider()

# --------------------------------------------------------------------------- #
# Late-arriving facts vs the incremental lookback.
# --------------------------------------------------------------------------- #

st.subheader("Refund arrival lag vs the incremental lookback")
st.caption(
    "The distribution the whole incremental design rests on. Anything at or "
    "beyond the lookback line is a fact the pipeline can no longer see."
)

lag = safe("""
    select refund_lag_days, count(*) as refunds
    from marts.fct_payments
    where refund_lag_days is not null
    group by 1 order by 1
""")
LOOKBACK_DAYS = 21
if not lag.empty:
    fig = go.Figure(go.Bar(x=lag["refund_lag_days"], y=lag["refunds"], name="refunds"))
    fig.add_vline(
        x=LOOKBACK_DAYS,
        line_dash="dash",
        line_color="red",
        annotation_text=f"lookback = {LOOKBACK_DAYS}d",
    )
    fig.update_layout(
        xaxis_title="days between payment and refund", yaxis_title="refunds", height=320
    )
    st.plotly_chart(fig, use_container_width=True)

    beyond = int(lag[lag["refund_lag_days"] > LOOKBACK_DAYS]["refunds"].sum())
    worst = int(lag["refund_lag_days"].max())
    c1, c2 = st.columns(2)
    c1.metric(
        "Worst observed lag",
        f"{worst}d",
        delta=f"{LOOKBACK_DAYS - worst}d of headroom",
        delta_color="normal" if worst < LOOKBACK_DAYS else "inverse",
    )
    c2.metric(
        "Refunds beyond the lookback",
        f"{beyond:,}",
        delta="permanently wrong" if beyond else "none",
        delta_color="inverse" if beyond else "normal",
    )
    if beyond:
        st.error(
            f"{beyond} refund(s) arrived beyond the {LOOKBACK_DAYS}-day lookback. "
            "Those payment rows are permanently wrong — widen "
            "`incremental_lookback_days` and full-refresh fct_payments."
        )

st.divider()

# --------------------------------------------------------------------------- #
# Schema change audit.
# --------------------------------------------------------------------------- #

st.subheader("Last 20 schema change events")
st.caption(
    "Written by the CDC sink's schema guard. Additive and widening are accepted; incompatible halts that table."
)

schema_changes = safe("""
    select detected_at, "table", column_name, change, from_type, to_type, outcome
    from ops.schema_changes
    order by detected_at desc
    limit 20
""")
if schema_changes.empty:
    st.info(
        "No schema changes recorded yet. Run `make schema-change` to apply "
        "migration 0003 (`orders.channel`) while the pipeline is running and "
        "watch the guard classify it as ADDITIVE."
    )
else:
    st.dataframe(schema_changes, use_container_width=True, hide_index=True)

st.caption(
    f"Warehouse: `{WAREHOUSE}` · cache TTL {REFRESH_SECONDS}s · "
    f"rendered {datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC"
)
