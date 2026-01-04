from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from datetime import datetime
import os
import pandas as pd
import yaml
import psycopg2

# =====================================================
# CONFIG
# =====================================================
DATASET_NAME = "email_thread_summaries"

BASE_PATH = "/opt/airflow/extraction/email_thread_summaries"
CSV_PATH = "/opt/airflow/data/email_thread_summaries/email_thread_summaries.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"

PG_CONN = {
    "host": "postgres-dev",
    "port": 5432,
    "dbname": "dev_db",
    "user": "dev_user",
    "password": "dev_password",
}

# =====================================================
# TASK 1 — Check CSV exists
# =====================================================
def check_csv_exists():
    if not os.path.exists(CSV_PATH):
        raise AirflowFailException(f"CSV file not found at {CSV_PATH}")

# =====================================================
# TASK 2 — Validate schema
# =====================================================
def validate_schema():
    df = pd.read_csv(CSV_PATH)

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = [col["name"] for col in schema["columns"]]
    actual_columns = list(df.columns)

    if expected_columns != actual_columns:
        raise AirflowFailException(
            f"Schema mismatch. Expected {expected_columns}, got {actual_columns}"
        )

# =====================================================
# TASK 3 — Basic transform
# =====================================================
def transform_data():
    df = pd.read_csv(CSV_PATH)

    df.columns = [c.strip() for c in df.columns]

    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

    df.to_csv(CSV_PATH, index=False)

# =====================================================
# TASK 4 — Load into PostgreSQL
# =====================================================
def load_to_postgres():
    with open(DDL_PATH, "r") as f:
        ddl = f.read()

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    # Create table
    cur.execute(ddl)

    df = pd.read_csv(CSV_PATH)

    for _, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO public.email_thread_summaries (thread_id, summary)
            VALUES (%s, %s)
            ON CONFLICT (thread_id) DO NOTHING
            """,
            (int(row["thread_id"]), row["summary"])
        )

    conn.commit()
    cur.close()
    conn.close()

# =====================================================
# DAG DEFINITION
# =====================================================
with DAG(
    dag_id="email_thread_summaries_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["csv", "postgres", "ingestion"],
) as dag:

    check_file = PythonOperator(
        task_id="check_csv_exists",
        python_callable=check_csv_exists
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    check_file >> validate >> transform >> load
