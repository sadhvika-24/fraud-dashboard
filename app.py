import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio
import altair as alt
import snowflake.connector

# ---------------- FIX WEBGL ----------------
pio.renderers.default = "svg"

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Financial Fraud Dashboard",
    layout="wide"
)

st.title("🚨 Financial Fraud Detection Dashboard")

# ---------------- SNOWFLAKE CONNECTION ----------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse="Bank_WH",
        database="FIN_FRAUD_DB1",
        schema="GOLD"
    )

conn = get_connection()

def run_query(query):
    return pd.read_sql(query, conn)

# =====================================================
# KPI SECTION
# =====================================================
kpi_df = run_query("SELECT * FROM GOLD.VW_FRAUD_KPI")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Transactions", int(kpi_df["TOTAL_TXNS"][0]))
c2.metric("Total Amount", f"{kpi_df['TOTAL_AMOUNT'][0]:,.0f}")
c3.metric("High Risk", int(kpi_df["HIGH_RISK_TXNS"][0]))
c4.metric("Medium Risk", int(kpi_df["MEDIUM_RISK_TXNS"][0]))
c5.metric("Low Risk", int(kpi_df["LOW_RISK_TXNS"][0]))

st.divider()

# =====================================================
# LOAD TREND DATA
# =====================================================
trend_df = run_query("SELECT * FROM GOLD.VW_FRAUD_TRENDS")
trend_df["TXN_DATE"] = pd.to_datetime(trend_df["TXN_DATE"])
trend_df["YEAR"] = trend_df["TXN_DATE"].dt.year
trend_df["MONTH"] = trend_df["TXN_DATE"].dt.month

# =====================================================
# YEARLY HIGH RISK % (PIE)
# =====================================================
st.subheader("📅 Year-wise High Risk Comparison (%)")

yearly_df = trend_df.groupby("YEAR")["HIGH_RISK_TXNS"].sum().reset_index()

fig_year = px.pie(
    yearly_df,
    names="YEAR",
    values="HIGH_RISK_TXNS",
    title="Year-wise High Risk Transactions (%)"
)
st.plotly_chart(fig_year, use_container_width=True)

# =====================================================
# MONTHLY HIGH RISK % (PIE)
# =====================================================
st.subheader("🗓️ Monthly High Risk Comparison (%)")

year_m = st.selectbox(
    "Select Year",
    sorted(trend_df["YEAR"].unique())
)

monthly_df = trend_df[trend_df["YEAR"] == year_m] \
    .groupby("MONTH")["HIGH_RISK_TXNS"].sum().reset_index()

fig_month = px.pie(
    monthly_df,
    names="MONTH",
    values="HIGH_RISK_TXNS",
    title=f"Monthly High Risk % - {year_m}"
)
st.plotly_chart(fig_month, use_container_width=True)

# =====================================================
# DAILY HIGH RISK TREND
# =====================================================
st.subheader("📆 Daily High Risk Trend")

c1, c2 = st.columns(2)

with c1:
    year_d = st.selectbox(
        "Year",
        sorted(trend_df["YEAR"].unique()),
        key="daily_year"
    )

with c2:
    month_d = st.selectbox(
        "Month",
        sorted(trend_df[trend_df["YEAR"] == year_d]["MONTH"].unique()),
        key="daily_month"
    )

daily_df = trend_df[
    (trend_df["YEAR"] == year_d) &
    (trend_df["MONTH"] == month_d)
]

daily_chart = alt.Chart(daily_df).mark_line(point=True).encode(
    x="TXN_DATE:T",
    y="HIGH_RISK_TXNS:Q",
    tooltip=["TXN_DATE", "HIGH_RISK_TXNS"]
).properties(height=350)

st.altair_chart(daily_chart, use_container_width=True)

# =====================================================
# TOTAL vs HIGH RISK TREND
# =====================================================
st.subheader("📊 Total vs High Risk Transactions")

year_c = st.selectbox(
    "Select Year",
    sorted(trend_df["YEAR"].unique()),
    key="compare_year"
)

compare_df = trend_df[trend_df["YEAR"] == year_c].melt(
    id_vars=["TXN_DATE"],
    value_vars=["TOTAL_TXNS", "HIGH_RISK_TXNS"],
    var_name="TYPE",
    value_name="COUNT"
)

compare_chart = alt.Chart(compare_df).mark_line(point=True).encode(
    x="TXN_DATE:T",
    y="COUNT:Q",
    color="TYPE:N",
    tooltip=["TYPE", "COUNT"]
).properties(height=350)

st.altair_chart(compare_chart, use_container_width=True)

st.divider()

# =====================================================
# HIGH RISK BY LOCATION
# =====================================================
st.subheader("🌍 High Risk Transactions by Location")

loc_df = run_query("SELECT * FROM GOLD.VW_LOCATION_RISK")

fig_loc = px.bar(
    loc_df,
    x="LOCATION",
    y="HIGH_RISK_TXNS",
    title="High Risk Transactions by Location"
)
st.plotly_chart(fig_loc, use_container_width=True)

st.divider()

# =====================================================
# TRANSACTION DRILL-DOWN
# =====================================================
st.subheader("🎯 Transaction Drill-Down")

dash_df = run_query("SELECT * FROM GOLD.VW_FRAUD_DASHBOARD")
dash_df["TXN_TIME"] = pd.to_datetime(dash_df["TXN_TIME"])
dash_df["YEAR"] = dash_df["TXN_TIME"].dt.year
dash_df["MONTH"] = dash_df["TXN_TIME"].dt.month

f1, f2, f3, f4 = st.columns(4)

with f1:
    year = st.selectbox("Year", sorted(dash_df["YEAR"].unique()))

with f2:
    month = st.selectbox("Month", sorted(dash_df["MONTH"].unique()))

with f3:
    city = st.selectbox(
        "City",
        ["ALL"] + sorted(dash_df["LOCATION"].dropna().unique())
    )

with f4:
    risk = st.selectbox(
        "Risk Level",
        ["ALL", "HIGH_RISK", "MEDIUM_RISK", "LOW_RISK"]
    )

filtered_df = dash_df[
    (dash_df["YEAR"] == year) &
    (dash_df["MONTH"] == month)
]

if city != "ALL":
    filtered_df = filtered_df[filtered_df["LOCATION"] == city]

if risk != "ALL":
    filtered_df = filtered_df[filtered_df["FRAUD_RISK_LEVEL"] == risk]

st.dataframe(filtered_df, use_container_width=True)

st.info(
    f"Filters → Year: {year}, Month: {month}, City: {city}, Risk: {risk}"
)
