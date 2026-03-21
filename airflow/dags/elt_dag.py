"""
Airflow DAG: Orchestrate the full ELT pipeline.
Runs daily at 6am UTC.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="housing_elt_pipeline",
    default_args=default_args,
    description="ELT pipeline: CSO → GCS → BigQuery → dbt",
    schedule_interval="0 6 * * *",  # Daily at 6am UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "housing"],
) as dag:

    extract = BashOperator(
        task_id="extract_to_gcs",
        bash_command="python /opt/airflow/extract/ingest.py",
    )

    load = BashOperator(
        task_id="load_to_bigquery",
        bash_command="python /opt/airflow/load/gcs_to_bq.py",
    )

    transform = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project/housing_transforms && dbt run",
    )

    test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project/housing_transforms && dbt test",
    )

    # Define execution order
    extract >> load >> transform >> test