# Dataset Manifest

## Dataset Name
email_thread_summaries

## Description
This dataset contains summarized email thread information.
Source CSV is downloaded locally (SharePoint / Kaggle export) and ingested
into PostgreSQL using a Dockerized Airflow pipeline.

## Local CSV Folder Path
/opt/airflow/data/email_thread_summaries/

## Target Database
PostgreSQL (External)

## Target Table
public.email_thread_summaries

## Load Strategy
Append (idempotent via primary key)

## Dataset Status
done
