# End-to-End ELT Pipeline: Irish Housing Data

A production-style ELT pipeline that ingests Irish housing completions data from the CSO (Central Statistics Office), lands it in cloud storage, loads it into a data warehouse, transforms it with dbt, and serves it through an interactive dashboard — all orchestrated with Apache Airflow.

## Architecture

```
data.gov.ie (CSO API)
        │
        ▼
  Python Ingestion ──────► Google Cloud Storage (raw JSON)
                                    │
                                    ▼
                              BigQuery (raw tables)
                                    │
                                    ▼
                            dbt (staging → marts)
                                    │
                                    ▼
                          Streamlit Dashboard
                                    │
                    ┌───────────────┘
                    ▼
            Apache Airflow (daily schedule)
```

## Tech Stack

| Layer          | Technology              | Purpose                              |
|----------------|-------------------------|--------------------------------------|
| Ingestion      | Python, requests        | Pull data from CSO REST API          |
| Storage        | Google Cloud Storage    | Immutable raw data landing zone      |
| Warehouse      | BigQuery                | Columnar analytics warehouse         |
| Transformation | dbt                     | SQL-based staging and mart models    |
| Dashboard      | Streamlit               | Interactive charts and filters       |
| Orchestration  | Airflow (Docker)        | Scheduled daily pipeline runs        |
| Auth           | GCP Service Account     | Least-privilege IAM authentication   |
| Version Control| Git + GitHub            | Incremental commits per phase        |

## Data Source

**Dataset:** New Dwelling Completions (NDQ07) from the Central Statistics Office of Ireland via [data.gov.ie](https://data.gov.ie)

The dataset tracks new housing completions across Irish areas (by Eircode) on a quarterly basis from 2012 onwards. It answers questions like:
- How many new homes were built in Dublin 15 last quarter?
- Which areas are seeing the most construction activity?
- How have completion trends changed since 2020?

## Project Structure

```
elt-pipeline/
├── extract/
│   └── ingest.py                  # Pull CSO data → GCS
├── load/
│   └── gcs_to_bq.py              # GCS → BigQuery (flatten JSON-stat)
├── dbt_project/
│   └── housing_transforms/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_housing.sql      # Clean raw data
│       │   │   └── sources.yml          # Source definitions
│       │   └── marts/
│       │       ├── housing_summary.sql  # Aggregated mart
│       │       └── schema.yml           # Column tests
│       └── dbt_project.yml
├── dashboard/
│   └── app.py                     # Streamlit dashboard
├── airflow/
│   └── dags/
│       └── elt_dag.py             # DAG: extract → load → transform → test
├── docker-compose.yaml            # Airflow infrastructure
├── .gitignore                     # Excludes credentials, venv
└── README.md
```

## How to Run

### Prerequisites
- Python 3.10+
- Google Cloud account with BigQuery and Cloud Storage APIs enabled
- Docker Desktop (for Airflow)
- GCP service account key (JSON)

### 1. Setup
```bash
git clone https://github.com/YOUR_USERNAME/elt-pipeline.git
cd elt-pipeline
python3 -m venv venv
source venv/bin/activate
pip install requests google-cloud-storage google-cloud-bigquery pandas dbt-bigquery streamlit
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
```

### 2. Run the pipeline manually
```bash
# Extract: CSO API → GCS
python extract/ingest.py

# Load: GCS → BigQuery
python load/gcs_to_bq.py

# Transform: dbt staging + marts
cd dbt_project/housing_transforms
dbt run
dbt test

# Dashboard
cd ../..
streamlit run dashboard/app.py
```

### 3. Run with Airflow (automated)
```bash
docker compose up -d
# Open http://localhost:8080 (admin/admin)
# Toggle on the housing_elt_pipeline DAG
```

## Design Decisions

**Why ELT over ETL?**
Transforming inside the warehouse (BigQuery) is cheaper and more flexible. Raw data stays intact in GCS — I can re-transform anytime without re-ingesting.

**Why BigQuery?**
Generous free tier (1TB queries/month), serverless (no cluster to manage), and widely used in industry. Located in europe-west1 to keep data close to the source.

**Why dbt?**
Brings software engineering practices to SQL: version control, automated testing, documentation, and lineage tracking. The staging/mart pattern separates data cleaning from business logic.

**Why GCS as a landing zone?**
Each ingestion run writes a timestamped file — immutable storage means I never overwrite history. This is a core data engineering principle for auditability and reprocessing.

**Why data.gov.ie?**
Real government data is more compelling than toy datasets. Irish housing data tells a meaningful story and stands out on a CV for Dublin-based roles.

## What I'd Add Next

- **Incremental models** in dbt — only process new data instead of full refresh
- **Great Expectations** for data quality validation at the source
- **Terraform** to provision GCP infrastructure as code
- **GitHub Actions CI** to run dbt tests on every pull request
- **Slack alerts** on pipeline failure via Airflow callbacks
- **dbt snapshots** to track slowly changing dimensions over time

## Dashboard Preview

The Streamlit dashboard provides:
- Filterable area selection (by Eircode region)
- Year range slider (2012–2025)
- Line chart showing completions over time per area
- Bar chart of top areas by total completions
- Expandable raw data table

Run `streamlit run dashboard/app.py` and open http://localhost:8501

---

*Built as a portfolio project demonstrating modern data stack skills.*
