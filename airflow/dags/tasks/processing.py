import logging

import awswrangler as awr
from airflow.decorators import task


@task(
    task_id="process_vacciations",
)
def process_vaccinations(
    bucket: str,
    boto3_session,
):
    for o in awr.s3.list_objects(
        f"{bucket}/data/rki/raw/germany/vaccinations/",
        suffix=".parquet",
        boto3_session=boto3_session,
    ):
        logging.info(f"Processing {o}")
        df = awr.s3.read_parquet(o, boto3_session=boto3_session)

        sel_cols = ["data.administeredVaccinations"] + [
            col
            for col in df.columns
            if col.startswith("data.vaccinat") and not col.count("delta")
        ]

        df = df[sel_cols].rename(
            columns={
                k: k.removeprefix("data.").replace(".", "_").lower() for k in sel_cols
            }
        )

        awr.s3.to_parquet(
            df=df,
            dataset=False,
            path=f"{bucket}/data/rki/processed/germany/vaccinations/{o.split('/')[-1]}",  # noqa
            boto3_session=boto3_session,
        )

        awr.s3.delete_objects(
            o,
            boto3_session=boto3_session,
        )
