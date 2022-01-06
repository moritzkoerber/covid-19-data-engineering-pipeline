import os

import pandas
import requests

_API_URL = os.environ["API_URL"]
_S3_BUCKET = os.environ["S3_BUCKET"]


def handler(event, context):
    response = requests.get(_API_URL)
    df = pandas.json_normalize(response.json())

    date = f"{pandas.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.to_parquet(
        f"{_S3_BUCKET}/{date}.parquet",
        index=False,
    )
