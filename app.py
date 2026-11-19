from pathlib import Path
import duckdb
import polars as pl
import streamlit as st

st.set_page_config(
    page_title="FinTech Loan Risk & Lineage Portal",
    page_icon="📊",
    layout="wide",
)

st.title("FinTech Loan Risk & Portfolio Intelligence Portal")
st.caption(
    "Medallion Lakehouse (Bronze → Quarantine / Silver → Gold) • "
    "DuckDB + Polars + Prefect • 60% LGD Credit Risk Economics"
)

GOLD_ROOT = Path("data/gold")


@st.cache_data
def get_available_partitions() -> list[str]:
    if not GOLD_ROOT.exists():
        return []
    partitions = sorted(
        [
            p.name.replace("dt=", "")
            for p in GOLD_ROOT.glob("dt=*")
            if (p / "portfolio_risk_summary.parquet").exists()
        ],
        reverse=True,
    )
    return partitions


@st.cache_data
def load_gold_risk(partition_dt: str) -> pl.DataFrame:
    path = GOLD_ROOT / f"dt={partition_dt}" / "portfolio_risk_summary.parquet"
    if not path.exists():
        return pl.DataFrame()
    con = duckdb.connect()
    try:
        return con.sql(f"SELECT * FROM read_parquet('{path.as_posix()}')").pl()
    finally:
        con.close()


@st.cache_data
def load_audit_report(partition_dt: str) -> pl.DataFrame:
    audit_path = GOLD_ROOT / "audit" / f"dt={partition_dt}" / "lineage_reconciliation_audit.parquet"
    if not audit_path.exists():
        return pl.DataFrame()
    con = duckdb.connect()
    try:
        return con.sql(f"SELECT * FROM read_parquet('{audit_path.as_posix()}')").pl()
    finally:
        con.close()


partitions = get_available_partitions()

if not partitions:
    st.warning(
        "No Gold layer partitions found under `data/gold/dt=*`. "
        "Run `python -m src.pipeline` first to generate Gold metrics."
    )
    st.stop()

# Sidebar Controls
st.sidebar.header("Partition & Risk Filters")
selected_dt = st.sidebar.selectbox("Execution Partition (`dt`)", partitions, index=0)

df_gold = load_gold_risk(selected_dt)
if df_gold.is_empty():
    st.error(f"No risk summary rows found for partition `{selected_dt}`.")
    st.stop()

asset_options = ["All"] + sorted(df_gold["asset_class"].drop_nulls().unique().to_list())
city_options = ["All"] + sorted(df_gold["city"].drop_nulls().unique().to_list())

selected_asset = st.sidebar.selectbox("Asset Class", asset_options)
selected_city = st.sidebar.selectbox("City", city_options)
min_default_rate = st.sidebar.slider(
    "Minimum Default Rate (%)",
    min_value=0.0,
    max_value=float(df_gold["default_rate_pct"].max() or 100.0),
    value=0.0,
    step=0.5,
)

filtered = df_gold
if selected_asset != "All":
    filtered = filtered.filter(pl.col("asset_class") == selected_asset)
if selected_city != "All":
    filtered = filtered.filter(pl.col("city") == selected_city)
if min_default_rate > 0.0:
    filtered = filtered.filter(pl.col("default_rate_pct") >= min_default_rate)

# Executive KPI Strip
total_loans = int(filtered["total_loans"].sum()) if not filtered.is_empty() else 0
total_ead = float(filtered["total_portfolio_exposure"].sum() or 0.0)
total_el = float(filtered["estimated_expected_loss"].sum() or 0.0)
total_net_rev = float(filtered["net_risk_adjusted_revenue"].sum() or 0.0)
weighted_pd = (
    (float(filtered["defaulted_exposure"].sum() or 0.0) / total_ead * 100.0)
    if total_ead > 0
    else 0.0
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Loans in Slice", f"{total_loans:,}")
k2.metric("Total Exposure (EAD)", f"{total_ead:,.2f}")
k3.metric("Portfolio PD", f"{weighted_pd:.2f}%")
k4.metric("Expected Loss (60% LGD)", f"{total_el:,.2f}")
k5.metric("Net Risk-Adjusted Rev", f"{total_net_rev:,.2f}")

st.markdown("---")

# Asset Class Rollup & Risk Table
left_col, right_col = st.columns([1.1, 1.9])

with left_col:
    st.subheader("Asset Class Risk Rollup")
    if not filtered.is_empty():
        asset_rollup = (
            filtered.group_by("asset_class")
            .agg(
                [
                    pl.col("total_portfolio_exposure").sum().round(2).alias("ead"),
                    pl.col("estimated_expected_loss").sum().round(2).alias("expected_loss"),
                    pl.col("net_risk_adjusted_revenue").sum().round(2).alias("net_risk_rev"),
                ]
            )
            .sort("net_risk_rev", descending=False)
        )
        st.dataframe(asset_rollup.to_pandas(), use_container_width=True, hide_index=True)
        st.bar_chart(
            asset_rollup.to_pandas().set_index("asset_class")[["ead", "expected_loss"]],
            height=260,
        )
    else:
        st.info("No rows match the current filter selection.")

with right_col:
    st.subheader("Granular Portfolio Risk Matrix (Asset Class × City)")
    if not filtered.is_empty():
        st.dataframe(
            filtered.sort("net_risk_adjusted_revenue", descending=False).to_pandas(),
            use_container_width=True,
            hide_index=True,
            height=420,
        )
    else:
        st.info("Adjust sidebar filters to view loan segments.")

st.markdown("---")

# Decision Support Callout for Risk Committee
st.subheader("Credit Risk Decision Signals")
if not filtered.is_empty():
    worst_pocket = filtered.sort("net_risk_adjusted_revenue", descending=False).row(0, named=True)
    if worst_pocket["net_risk_adjusted_revenue"] < 0 or worst_pocket["default_rate_pct"] > 0:
        st.warning(
            f"**Watchlist Segment:** **{worst_pocket['asset_class']}** in **{worst_pocket['city']}** "
            f"shows default rate **{worst_pocket['default_rate_pct']}%**, "
            f"Expected Loss **{worst_pocket['estimated_expected_loss']:,.2f}**, and "
            f"Net Risk-Adjusted Revenue **{worst_pocket['net_risk_adjusted_revenue']:,.2f}**. "
            "Action: Review origination limits, pricing spread, and collections cadence for this pocket."
        )
    else:
        st.success(
            f"**Lowest-Margin Pocket in Slice:** **{worst_pocket['asset_class']}** in **{worst_pocket['city']}** "
            f"currently records **0.00%** default rate with **{worst_pocket['net_risk_adjusted_revenue']:,.2f}** "
            "net risk-adjusted revenue. No elevated default watch triggered in this filtered view."
        )

# Regulatory Lineage & Reconciliation Panel
st.markdown("---")
st.subheader("Data Lineage & Reconciliation Audit (Compliance View)")
df_audit = load_audit_report(selected_dt)
if not df_audit.is_empty():
    st.dataframe(df_audit.to_pandas(), use_container_width=True, hide_index=True)
else:
    st.info(
        f"No reconciliation audit file found at `data/gold/audit/dt={selected_dt}/lineage_reconciliation_audit.parquet`."
    )