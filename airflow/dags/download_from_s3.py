from datetime import datetime

import boto3
import pandas
from airflow.models.dag import DAG
from tasks import ingestion, processing

ENV = "staging"
_bucket = f"s3://data-pipeline-s3-bucket-{ENV}"

boto3_session = boto3.Session(profile_name="GenericUser")
run_date = pandas.Timestamp.now().floor("D")

with DAG(
    dag_id=f"vaccinations-{ENV}",
    schedule_interval="0 0 1 * *",
    catchup=False,
    start_date=datetime(2022, 8, 31, 19, 40),
) as dag:
    process_vacciations_task = processing.process_vaccinations(
        bucket=_bucket,
        run_date=run_date,
        boto3_session=boto3_session,
    )
    ingest_vaccinations_task = ingestion.ingest_into_redshift(
        bucket=_bucket,
        run_date=run_date,
        boto3_session=boto3_session,
    )

process_vacciations_task >> ingest_vaccinations_task
