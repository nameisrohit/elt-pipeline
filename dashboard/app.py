import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os

BQ_PROJECT = "elt-pipeline-490819"

@st.cache_data(ttl=3600)
def load_data():
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

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .main-header {
        background: linear-gradient(135deg, #1B4332 0%, #2D6A4F 50%, #40916C 100%);
        padding: 2.5rem 2rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        color: white;
    }
    .main-header h1 {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .main-header p {
        font-size: 0.95rem;
        opacity: 0.85;
        margin: 0.5rem 0 0 0;
    }
    .main-header .pipeline-tag {
        display: inline-block;
        background: rgba(255,255,255,0.15);
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        margin-top: 0.8rem;
        letter-spacing: 0.5px;
    }

    .metric-card {
        background: white;
        border: 1px solid #e8e8e8;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        text-align: center;
    }
    .metric-card .value {
        font-size: 2rem;
        font-weight: 700;
        color: #1B4332;
    }
    .metric-card .label {
        font-size: 0.8rem;
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 4px;
    }

    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1B4332;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #40916C;
        display: inline-block;
    }

    .footer {
        text-align: center;
        color: #adb5bd;
        font-size: 0.75rem;
        padding: 2rem 0 1rem 0;
        border-top: 1px solid #e8e8e8;
        margin-top: 3rem;
    }
    .footer a {
        color: #40916C;
        text-decoration: none;
    }

    [data-testid="stSidebar"] {
        background: #f8faf9;
    }
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1 {
        color: #1B4332;
        font-size: 1.3rem;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🏠 Irish Housing Completions</h1>
    <p>Tracking new dwelling completions across Ireland, powered by CSO open data</p>
    <span class="pipeline-tag">PYTHON → GCS → BIGQUERY → DBT → STREAMLIT</span>
</div>
""", unsafe_allow_html=True)

df = load_data()

# Sidebar
st.sidebar.markdown("# 🇮🇪 Filters")
st.sidebar.markdown("---")

areas = st.sidebar.multiselect(
    "Select areas",
    options=sorted(df["area"].unique()),
    default=sorted(df["area"].unique())[:5]
)
years = st.sidebar.slider(
    "Year range",
    min_value=int(df["year"].min()),
    max_value=int(df["year"].max()),
    value=(2020, int(df["year"].max()))
)

st.sidebar.markdown("---")
st.sidebar.markdown(
    f"**{len(df['area'].unique())}** areas available  \n"
    f"**{int(df['year'].min())}–{int(df['year'].max())}** data range"
)

filtered = df[
    (df["area"].isin(areas)) &
    (df["year"].between(years[0], years[1]))
]

# Metrics
total = filtered["total_completions"].sum()
total_national = df[df["year"].between(years[0], years[1])]["total_completions"].sum()
pct = (total / total_national * 100) if total_national > 0 else 0

latest_year = filtered[filtered["year"] == filtered["year"].max()]["total_completions"].sum()
prev_year = filtered[filtered["year"] == (filtered["year"].max() - 1)]["total_completions"].sum()
growth = ((latest_year - prev_year) / prev_year * 100) if prev_year > 0 else 0

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{total:,.0f}</div>
        <div class="label">Total completions</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{len(areas)}</div>
        <div class="label">Areas selected</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="value">{pct:.1f}%</div>
        <div class="label">Share of national</div>
    </div>""", unsafe_allow_html=True)
with c4:
    arrow = "↑" if growth >= 0 else "↓"
    color = "#2D6A4F" if growth >= 0 else "#c0392b"
    st.markdown(f"""
    <div class="metric-card">
        <div class="value" style="color:{color}">{arrow} {abs(growth):.1f}%</div>
        <div class="label">YoY growth</div>
    </div>""", unsafe_allow_html=True)

# Charts
st.markdown('<div class="section-title">Completions over time</div>', unsafe_allow_html=True)
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
    st.markdown('<div class="section-title">Top 10 areas</div>', unsafe_allow_html=True)
    by_area = (
        filtered.groupby("area")["total_completions"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    st.bar_chart(by_area)

with right:
    st.markdown('<div class="section-title">Yearly trend</div>', unsafe_allow_html=True)
    by_year = (
        filtered.groupby("year")["total_completions"]
        .sum()
        .reset_index()
        .set_index("year")
    )
    st.bar_chart(by_year)

with st.expander("📋 View raw data"):
    st.dataframe(filtered, width="stretch")

# Footer
st.markdown("""
<div class="footer">
    Built by <strong>Rohit Yadav</strong> &nbsp;|&nbsp;
    <a href="https://github.com/nameisrohit/elt-pipeline" target="_blank">GitHub Repo</a> &nbsp;|&nbsp;
    Data: Central Statistics Office, Ireland &nbsp;|&nbsp;
    Pipeline: Python → GCS → BigQuery → dbt → Streamlit → Airflow
</div>
""", unsafe_allow_html=True)
