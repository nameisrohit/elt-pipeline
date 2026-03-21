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

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .hero {
        background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
        padding: 2.5rem 2.5rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        border: 1px solid rgba(255,255,255,0.08);
    }
    .hero h1 {
        font-size: 2.4rem;
        font-weight: 700;
        margin: 0;
        color: #ffffff;
        letter-spacing: -1px;
    }
    .hero .subtitle {
        font-size: 1rem;
        color: rgba(255,255,255,0.6);
        margin: 0.5rem 0 1rem 0;
    }
    .hero .tags {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
    }
    .hero .tag {
        background: rgba(255,255,255,0.08);
        border: 1px solid rgba(255,255,255,0.12);
        padding: 5px 14px;
        border-radius: 100px;
        font-size: 0.7rem;
        color: rgba(255,255,255,0.7);
        letter-spacing: 1.5px;
        font-weight: 500;
    }

    .metric-row {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
        margin-bottom: 2rem;
    }
    .metric-box {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
    }
    .metric-box .number {
        font-size: 2.2rem;
        font-weight: 700;
        color: #4fc3f7;
        line-height: 1;
    }
    .metric-box .number.green { color: #66bb6a; }
    .metric-box .number.orange { color: #ffa726; }
    .metric-box .number.pink { color: #ef5350; }
    .metric-box .desc {
        font-size: 0.7rem;
        color: rgba(255,255,255,0.4);
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-top: 8px;
    }

    .section-head {
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: rgba(255,255,255,0.35);
        margin: 2.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid rgba(255,255,255,0.06);
    }

    .footer-bar {
        text-align: center;
        padding: 2rem 0 1rem 0;
        margin-top: 3rem;
        border-top: 1px solid rgba(255,255,255,0.06);
        font-size: 0.75rem;
        color: rgba(255,255,255,0.3);
    }
    .footer-bar a {
        color: #4fc3f7;
        text-decoration: none;
    }
    .footer-bar .pipe-viz {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 6px;
        margin-bottom: 12px;
        flex-wrap: wrap;
    }
    .footer-bar .pipe-step {
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.08);
        padding: 3px 10px;
        border-radius: 6px;
        font-size: 0.65rem;
        letter-spacing: 0.5px;
    }
    .footer-bar .pipe-arrow {
        color: rgba(255,255,255,0.15);
        font-size: 0.7rem;
    }

    [data-testid="stSidebar"] {
        background: #0a1a20;
        border-right: 1px solid rgba(255,255,255,0.06);
    }
</style>
""", unsafe_allow_html=True)

# Hero
st.markdown("""
<div class="hero">
    <h1>🏠 Irish Housing Completions</h1>
    <p class="subtitle">Tracking new dwelling completions across Ireland using CSO open data</p>
    <div class="tags">
        <span class="tag">REAL-TIME DATA</span>
        <span class="tag">170+ AREAS</span>
        <span class="tag">2012 — 2025</span>
    </div>
</div>
""", unsafe_allow_html=True)

df = load_data()

# Sidebar
st.sidebar.markdown("## 🇮🇪 Filters")

areas = st.sidebar.multiselect(
    "Areas",
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
st.sidebar.caption(f"{len(df['area'].unique())} areas · {int(df['year'].min())}–{int(df['year'].max())} data range")

filtered = df[
    (df["area"].isin(areas)) &
    (df["year"].between(years[0], years[1]))
]

# Metrics
total = filtered["total_completions"].sum()
total_national = df[df["year"].between(years[0], years[1])]["total_completions"].sum()
pct = (total / total_national * 100) if total_national > 0 else 0

latest_year_data = filtered[filtered["year"] == filtered["year"].max()]["total_completions"].sum()
prev_year_data = filtered[filtered["year"] == (filtered["year"].max() - 1)]["total_completions"].sum()
growth = ((latest_year_data - prev_year_data) / prev_year_data * 100) if prev_year_data > 0 else 0
arrow = "↑" if growth >= 0 else "↓"
growth_class = "green" if growth >= 0 else "pink"

st.markdown(f"""
<div class="metric-row">
    <div class="metric-box">
        <div class="number">{total:,.0f}</div>
        <div class="desc">Total completions</div>
    </div>
    <div class="metric-box">
        <div class="number green">{len(areas)}</div>
        <div class="desc">Areas selected</div>
    </div>
    <div class="metric-box">
        <div class="number orange">{pct:.1f}%</div>
        <div class="desc">Share of national</div>
    </div>
    <div class="metric-box">
        <div class="number {growth_class}">{arrow} {abs(growth):.1f}%</div>
        <div class="desc">Year-over-year growth</div>
    </div>
</div>
""", unsafe_allow_html=True)

# Line chart
st.markdown('<div class="section-head">Completions over time</div>', unsafe_allow_html=True)
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

# Two column charts
left, right = st.columns(2)

with left:
    st.markdown('<div class="section-head">Top 10 areas</div>', unsafe_allow_html=True)
    by_area = (
        filtered.groupby("area")["total_completions"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    st.bar_chart(by_area)

with right:
    st.markdown('<div class="section-head">Yearly trend</div>', unsafe_allow_html=True)
    by_year = (
        filtered.groupby("year")["total_completions"]
        .sum()
        .reset_index()
        .set_index("year")
    )
    st.bar_chart(by_year)

# Raw data
with st.expander("View raw data"):
    st.dataframe(filtered, width="stretch")

# Footer
st.markdown("""
<div class="footer-bar">
    <div class="pipe-viz">
        <span class="pipe-step">Python</span><span class="pipe-arrow">→</span>
        <span class="pipe-step">GCS</span><span class="pipe-arrow">→</span>
        <span class="pipe-step">BigQuery</span><span class="pipe-arrow">→</span>
        <span class="pipe-step">dbt</span><span class="pipe-arrow">→</span>
        <span class="pipe-step">Streamlit</span><span class="pipe-arrow">→</span>
        <span class="pipe-step">Airflow</span>
    </div>
    Built by <strong>Rohit Yadav</strong> &nbsp;·&nbsp;
    <a href="https://github.com/nameisrohit/elt-pipeline" target="_blank">Source Code</a> &nbsp;·&nbsp;
    Data: Central Statistics Office, Ireland
</div>
""", unsafe_allow_html=True)
