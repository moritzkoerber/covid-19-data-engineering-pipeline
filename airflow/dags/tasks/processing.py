import awswrangler as awr
import pandas
from airflow.decorators import task


@task(
    task_id="process_vacciations",
)
def process_vaccinations(
    bucket: str,
    run_date: pandas.Timestamp,
    boto3_session,
):
    df = awr.s3.read_parquet(
        f"{bucket}/data/rki/raw/germany/vaccinations/{run_date:%Y-%m-%d}.parquet",  # noqa
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
        path=f"{bucket}/data/rki/processed/germany/vaccinations/{run_date:%Y-%m-%d}.parquet",  # noqa
        boto3_session=boto3_session,
    )
