import logging
import os

import pandas as pd
import requests

logger = logging.getLogger()

_API_URL = os.environ["API_URL"]
_S3_BUCKET = os.environ["S3_BUCKET"]


def handler(event, context):
    # german data
    response = requests.get(f"{_API_URL}/germany")
    df = pd.json_normalize(response.json())

    date = f"{pd.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.assign(api_call_ts_utc=pd.Timestamp.utcnow()).to_parquet(
        f"{_S3_BUCKET}/data/rki/raw/germany/cases/{date}.parquet",
        index=False,
    )
