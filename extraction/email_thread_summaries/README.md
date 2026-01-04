# Email Thread Summaries — Airflow Ingestion Pipeline

## 1. Dataset Overview

**Dataset Name:** email_thread_summaries  
**Source:** Kaggle – Email Thread Summary Dataset  

### Purpose
This pipeline ingests summarized email thread data from CSV files, validates the schema against a defined contract, and loads clean records into a PostgreSQL table using Apache Airflow.

The pipeline is designed to be:
- Reproducible
- Idempotent
- Easy to maintain and extend

---

## 2. Dataset Schema

| Column    | Type    | Nullable | Description                               |
|-----------|---------|----------|-------------------------------------------|
| thread_id | INTEGER | No       | Unique identifier for an email thread     |
| summary   | TEXT    | No       | Generated summary of the email thread     |

**Primary Key:** `thread_id`

---

## 3. Repository Structure

```
extraction/email_thread_summaries/
├── config/
│   ├── schema_expected.yaml        # Schema contract
│   └── create_table.sql            # PostgreSQL DDL
├── sample_data/
│   └── email_thread_summaries.csv  # Small representative sample (committed)
├── dags/
│   └── email_thread_summaries_ingest.py  # Airflow DAG
├── logs/                           # Local runtime logs
├── MANIFEST.md                     # Dataset metadata
└── README.md                       # Documentation and runbook
```

### Runtime Data (Not Committed)

```
data/email_thread_summaries/email_thread_summaries.csv
```

- Contains the full downloaded dataset
- Used only during execution
- Must NOT be committed to Git

---

## 4. Target PostgreSQL Table

**Database:** dev_db  
**Schema:** public  
**Table:** email_thread_summaries  

The table is created automatically by the pipeline if it does not exist.

---

## 5. Environment Configuration

Environment variables are managed using a local `.env` file.

### `.env.sample`
```env
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.1

POSTGRES_AIRFLOW_DB=airflow
POSTGRES_AIRFLOW_USER=airflow
POSTGRES_AIRFLOW_PASSWORD=airflow

POSTGRES_DEV_DB=dev_db
POSTGRES_DEV_USER=dev_user
POSTGRES_DEV_PASSWORD=dev_password
POSTGRES_DEV_PORT=5433
```

Copy `.env.sample` to `.env` and update values before running the pipeline.

---

## 6. How to Run the Pipeline

### Prerequisites
- Docker
- Docker Compose

### Step 1: Start Airflow
```bash
docker compose up -d
```

Airflow UI:
```
http://localhost:8080
```

Login credentials:
```
Username: admin
Password: admin
```

---

### Step 2: Place Runtime CSV

Copy the full dataset CSV to:
```
data/email_thread_summaries/email_thread_summaries.csv
```

---

### Step 3: Trigger the DAG

1. Open the Airflow UI
2. Enable the DAG: `email_thread_summaries_ingest`
3. Click **Trigger DAG ▶**

---

## 7. DAG Overview

### Task Flow

```
check_csv_exists
        ↓
validate_schema
        ↓
transform_data
        ↓
load_to_postgres
```

### Task Descriptions

- **check_csv_exists**  
  Verifies that the runtime CSV file exists.

- **validate_schema**  
  Ensures CSV columns match `schema_expected.yaml`.

- **transform_data**  
  Applies basic normalization such as trimming whitespace.

- **load_to_postgres**  
  Creates the target table if required and inserts data using:
  ```
  ON CONFLICT (thread_id) DO NOTHING
  ```
  This makes the pipeline safe to re-run.

---

## 8. Troubleshooting

### DAG Not Visible
```bash
docker exec airflow-webserver airflow config get-value core dags_folder
```

Expected output:
```
/opt/airflow/extraction
```

Restart services if required:
```bash
docker compose restart airflow-scheduler airflow-webserver
```

---

### Schema Mismatch Error
- Update:
  - `schema_expected.yaml`
  - `create_table.sql`
  - Sample CSV headers
- Restart Airflow
- Rerun the DAG

---

### CSV Not Found Error
- Ensure the file exists at:
  ```
  data/email_thread_summaries/email_thread_summaries.csv
  ```
- Verify filename and extension

---

### Database Connection Error
- Verify values in `.env`
- Restart Docker services:
```bash
docker compose down
docker compose up -d
```

---

## 9. Runbook

### Updating Schema When Dataset Evolves
1. Update `schema_expected.yaml`
2. Update `create_table.sql`
3. Update the sample CSV to match the new schema
4. Restart Airflow
5. Rerun the DAG

---

### Rerunning with New CSV Drops
- Replace the runtime CSV file in:
  ```
  data/email_thread_summaries/
  ```
- Trigger the DAG again
- Existing rows are skipped automatically

---

### Resetting Data (Development Only)
```sql
TRUNCATE TABLE public.email_thread_summaries;
```

---

## 10. Pre-Commit Checklist

Before committing changes:
- [ ] MANIFEST.md updated
- [ ] Schema YAML matches real data
- [ ] DDL updated
- [ ] Sample CSV is small and representative
- [ ] DAG tested successfully
- [ ] Full datasets NOT committed

---

## 11. Reproducibility

Following this README, a new engineer can:
- Start Airflow using Docker
- Place a CSV file
- Run the DAG
- Verify data in PostgreSQL

No additional context or tribal knowledge is required.
