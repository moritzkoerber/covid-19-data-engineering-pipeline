import base64
import io
import os
import re

import awswrangler as awr
import pandas as pd
import requests

_S3_BUCKET = os.environ["S3_BUCKET"]
_REPOSITORY_PATH = os.environ["REPOSITORY_PATH"]
_ENVIRONMENT = os.environ["ENVIRONMENT"]

_now = pd.Timestamp.utcnow()
# countries = [e.name for e in list(pycountry.countries)]
file_names = [
    "time_series_covid19_confirmed_global.csv",
    "time_series_covid19_deaths_global.csv",
    "time_series_covid19_recovered_global.csv",
]

regex_pattern = re.compile(r"(?:covid19_)(\w+)(?:_global)")
data_types = [regex_pattern.search(file_name)[1] for file_name in file_names]


def handler(event, context):
    for file_name, data_type in zip(file_names, data_types):
        file = requests.get(f"{_REPOSITORY_PATH}/{file_name}")
        file_obj = io.StringIO(base64.b64decode(file.json()["content"]).decode("ascii"))

        pandas_df = (
            pd.read_csv(file_obj)
            .drop(columns=["Province/State", "Lat", "Long"])
            # .query("`Country/Region` in @countries")
            .melt(id_vars=["Country/Region"], var_name="date", value_name="cases")
            .assign(
                date=lambda x: pd.to_datetime(x["date"], format="%m/%d/%y"),
                api_call_ts_utc=_now,
            )
            .pipe(lambda x: x.join(x.date.dt.isocalendar()[["year", "week"]]))
            .rename(columns={"week": "iso_week"})
        )

        awr.s3.to_parquet(
            pandas_df,
            f"{_S3_BUCKET}/data/csse/{data_type}/global/",
            schema_evolution=False,
            database=f"csse_{_ENVIRONMENT}",
            table=f"{data_type}_global",
            mode="overwrite_partitions",
            sanitize_columns=True,
            partition_cols=["year", "iso_week"],
            parameters=dict(last_update=f"{pd.Timestamp.utcnow()}"),
            dataset=True,
        )
