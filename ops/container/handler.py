import os

import pandas
import requests

_API_URL = os.environ["API_URL"]
_BUCKET = os.environ["BUCKET"]


def handler(event, context):

    response = requests.get(_API_URL)
    df = pandas.json_normalize(response.json())

    df.to_parquet(
        f"{_BUCKET}/vaccinations/{pandas.Timestamp.today():%Y-%m-%d}.parquet",
        index=False,
    )
