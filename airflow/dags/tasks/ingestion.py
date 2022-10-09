import pandas
from airflow.decorators import task


@task(
    task_id="ingest_vaccinations",
)
def ingest_into_redshift(bucket: str, run_date: pandas.Timestamp, boto3_session):
    redshift_client = boto3_session.client("redshift-data")

    aws_account_id = boto3_session.client("sts").get_caller_identity()["Account"]

    redshift_client.execute_statement(
        ClusterIdentifier="vaccinations-redshift-cluster",
        Database="vaccinations",
        SecretArn=f"arn:aws:secretsmanager:{boto3_session.region_name}:{aws_account_id}:secret:redshift_admin-KPRsmn",  # noqa
        Sql=f"""
        copy vaccinations
        from 's3://{bucket}/data/rki/processed/germany/vaccinations/{run_date:%Y-%m-%d}.parquet' # noqa
        iam_role 'arn:aws:iam::820381935377:role/RedshiftServiceRole'
        FORMAT AS PARQUET;
        """,
    )
