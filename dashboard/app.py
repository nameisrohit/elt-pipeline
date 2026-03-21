
import streamlit as st
from google.cloud import bigquery
import pandas as pd

BQ_PROJECT = "elt-pipeline-490819"

@st.cache_data(ttl=3600)
def load_data():
    client = bigquery.Client(project=BQ_PROJECT)
    query = """
        SELECT area, quarter_label, total_completions, year
        FROM `elt-pipeline-490819.housing_transformed.housing_summary`
        ORDER BY quarter_label DESC
    """
    return client.query(query).to_dataframe()

st.set_page_config(page_title="Irish Housing Dashboard", layout="wide")
st.title("Irish Housing Completions")
st.markdown("Data source: CSO via data.gov.ie | Transformed with dbt")

df = load_data()

st.sidebar.header("Filters")
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

col1, col2, col3 = st.columns(3)
col1.metric("Total Completions", f"{filtered['total_completions'].sum():,.0f}")
col2.metric("Areas Selected", len(areas))
col3.metric("Time Range", f"{years[0]}-{years[1]}")

st.subheader("Completions Over Time")
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

st.subheader("Top Areas by Completions")
by_area = (
    filtered.groupby("area")["total_completions"]
    .sum()
    .sort_values(ascending=False)
    .head(10)
)
st.bar_chart(by_area)

with st.expander("View Raw Data"):
    st.dataframe(filtered, use_container_width=True)