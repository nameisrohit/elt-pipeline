import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

BQ_PROJECT = "elt-pipeline-490819"

@st.cache_data(ttl=3600)
def load_data():
    import os
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        client = bigquery.Client(project=BQ_PROJECT)
    else:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    query = """
        SELECT area, quarter_label, total_completions, year
        FROM `elt-pipeline-490819.housing_transformed.housing_summary`
        ORDER BY quarter_label DESC
    """
    return client.query(query).to_dataframe()

st.set_page_config(
    page_title="Irish Housing Dashboard",
    page_icon="🏠",
    layout="wide"
)

st.markdown("""
    <h1 style='text-align: center;'>🏠 Irish Housing Completions</h1>
    <p style='text-align: center; color: gray;'>
        Real-time data from CSO (Central Statistics Office) via data.gov.ie<br>
        Pipeline: Python → GCS → BigQuery → dbt → Streamlit
    </p>
    <hr>
""", unsafe_allow_html=True)

df = load_data()

st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/4/45/Flag_of_Ireland.svg", width=60)
st.sidebar.title("Filters")

areas = st.sidebar.multiselect(
    "Select Areas",
    options=sorted(df["area"].unique()),
    default=sorted(df["area"].unique())[:5]
)
years = st.sidebar.slider(
    "Year Range",
    min_value=int(df["year"].min()),
    max_value=int(df["year"].max()),
    value=(2020, int(df["year"].max()))
)

filtered = df[
    (df["area"].isin(areas)) &
    (df["year"].between(years[0], years[1]))
]

col1, col2, col3, col4 = st.columns(4)
col1.metric("🏗️ Total Completions", f"{filtered['total_completions'].sum():,.0f}")
col2.metric("📍 Areas Selected", len(areas))
col3.metric("📅 Time Range", f"{years[0]}-{years[1]}")
total_all = df[df["year"].between(years[0], years[1])]["total_completions"].sum()
if total_all > 0:
    pct = (filtered["total_completions"].sum() / total_all) * 100
    col4.metric("📊 Share of National", f"{pct:.1f}%")

st.markdown("### Completions over time")
chart_data = filtered.copy()
chart_data["area"] = chart_data["area"].str.replace(":", "-")
time_series = (
    chart_data.groupby(["quarter_label", "area"])["total_completions"]
    .sum()
    .reset_index()
    .pivot(index="quarter_label", columns="area", values="total_completions")
    .fillna(0)
)
st.line_chart(time_series)

left, right = st.columns(2)

with left:
    st.markdown("### Top 10 areas")
    by_area = (
        filtered.groupby("area")["total_completions"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    st.bar_chart(by_area)

with right:
    st.markdown("### Yearly trend")
    by_year = (
        filtered.groupby("year")["total_completions"]
        .sum()
        .reset_index()
        .set_index("year")
    )
    st.bar_chart(by_year)

with st.expander("📋 View raw data"):
    st.dataframe(filtered, width="stretch")

st.markdown("""
    <hr>
    <p style='text-align: center; color: gray; font-size: 12px;'>
        Built by Rohit Yadav | 
        <a href="https://github.com/YOUR_USERNAME/elt-pipeline" target="_blank">GitHub</a> | 
        Data: CSO Ireland
    </p>
""", unsafe_allow_html=True)
