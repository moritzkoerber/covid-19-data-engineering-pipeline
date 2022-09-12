import logging
import os

import pandas as pd
import requests

logger = logging.getLogger()

_API_URL = os.environ["API_URL"]
_S3_BUCKET = os.environ["S3_BUCKET"]


_now = pd.Timestamp.utcnow()


def handler(event, context):
    # german data
    response = requests.get(f"{_API_URL}/germany")
    df = pd.json_normalize(response.json())

    date = f"{pd.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.assign(api_call_ts_utc=_now).to_parquet(
        f"{_S3_BUCKET}/data/rki/raw/germany/{date}.parquet",
        index=False,
    )
