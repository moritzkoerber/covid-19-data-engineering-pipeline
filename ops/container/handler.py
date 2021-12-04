import os

import pandas
import requests

_API_URL = os.environ["API_URL"]
_BUCKET = os.environ["BUCKET"]


def handler(event, context):

    response = requests.get(_API_URL)
    df = pandas.json_normalize(response.json())

    date = f"{pandas.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.to_parquet(
        f"{_BUCKET}/vaccinations/date={date}/{date}.parquet",
        index=False,
    )
