import logging
import os

import pandas as pd
import requests

logger = logging.getLogger()

_API_URL = os.environ["API_URL"]
_S3_BUCKET = os.environ["S3_BUCKET"]


_now = pd.Timestamp.utcnow()


def handler(event, context):
    response = requests.get(_API_URL)
    df = pd.json_normalize(response.json())

    date = f"{pd.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.assign(api_call_ts_utc=_now).to_parquet(
        f"{_S3_BUCKET}/data/vaccinations/raw/germany/{date}.parquet",
        index=False,
    )
