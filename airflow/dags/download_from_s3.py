from datetime import datetime

import boto3
from airflow.operators.python_operator import PythonOperator

from airflow import DAG

default_args = {"start_date": datetime(2022, 8, 31, 19, 40), "owner": "Airflow"}


def read_s3(bucket_name):
    boto3_session = boto3.Session(profile_name="GenericUser")
    s3_client = boto3_session.client("s3")
    s3_client.download_file(
        "mytestbucketlol2",
        "date=2021-12-10/2021-12-10.parquet",
        "/opt/airflow/scripts/2021-12-10.parquet",
    )


with DAG(
    dag_id="s3",
    schedule_interval="0 0 * * *",
    catchup=False,
    default_args=default_args,
) as dag:
    check_s3 = PythonOperator(
        op_args=["data-pipeline-s3-bucket-staging"],
        task_id="s3_lookup",
        python_callable=read_s3,
    )
