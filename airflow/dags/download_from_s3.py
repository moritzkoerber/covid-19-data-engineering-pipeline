from datetime import datetime

import awswrangler as awr
import boto3
import pandas
from airflow.operators.python_operator import PythonOperator

from airflow import DAG

_bucket_path = "s3://data-pipeline-s3-bucket-staging"

boto3_session = boto3.Session(profile_name="GenericUser")


def process_vaccinations(bucket_name):
    df = awr.s3.read_parquet(
        f"{_bucket_path}/data/rki/raw/germany/vaccinations/2022-09-24.parquet",  # noqa
        boto3_session=boto3_session,
    ).assign(date=lambda x: f"{pandas.to_datetime(x['meta.lastUpdate'][0]):%Y-%m-%d}")

    sel_cols = ["date", "data.administeredVaccinations"] + [
        col
        for col in df.columns
        if col.startswith("data.vaccinat") and not col.count("delta")
    ]

    rename_dict = {
        k: k.removeprefix("data.").replace(".", "_").lower() for k in sel_cols
    }

    df = df[sel_cols].rename(columns=rename_dict)

    awr.s3.to_parquet(
        df=df,
        dataset=False,
        path=f"{_bucket_path}/data/rki/processed/germany/vaccinations/{pandas.Timestamp.now().floor('D'):%Y-%m-%d}_vaccinations_filtered.parquet",  # noqa
        boto3_session=boto3_session,
    )


def ingest_into_redshift():
    redshift_client = boto3_session.client("redshift-data")

    boto3_session.client("sts").get_caller_identity()["Account"]

    redshift_client.execute_statement(
        ClusterIdentifier="vaccinations-redshift-cluster",
        Database="vaccinations",
        # DbUser="redshift_admin",
        SecretArn=f"arn:aws:secretsmanager:{boto3_session.region_name}:820381935377:secret:redshift_admin-KPRsmn",  # noqa
        Sql="""
        copy vaccinations
        from 's3://data-pipeline-s3-bucket-staging/data/rki/processed/germany/vaccinations/2022-09-25_vaccinations_filtered.parquet' # noqa
        iam_role 'arn:aws:iam::820381935377:role/RedshiftServiceRole'
        FORMAT AS PARQUET;
        """,
    )


with DAG(
    dag_id="s3",
    schedule_interval="0 0 1 * *",
    catchup=False,
    start_date=datetime(2022, 8, 31, 19, 40),
) as dag:
    process_vacciations = PythonOperator(
        op_args=["data-pipeline-s3-bucket-staging"],
        task_id="s3_lookup",
        python_callable=process_vaccinations,
    )
    ingest_vaccinations = PythonOperator(
        op_args=["data-pipeline-s3-bucket-staging"],
        task_id="s3_lookup",
        python_callable=ingest_into_redshift,
    )

process_vaccinations >> ingest_vaccinations
