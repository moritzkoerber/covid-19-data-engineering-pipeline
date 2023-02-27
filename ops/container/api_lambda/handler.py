import logging
import os

import pandas as pd
import requests
from awslambdaric.lambda_context import LambdaContext

logger = logging.getLogger()

_API_URL = os.environ["API_URL"]
_S3_BUCKET = os.environ["S3_BUCKET"]
_NOW = pd.Timestamp.utcnow()


def handler(event: dict, context: LambdaContext):
    # cases
    logger.info("Querying cases...")
    response = requests.get(f"{_API_URL}/germany")
    df = pd.json_normalize(response.json())

    date = f"{pd.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.assign(api_call_ts_utc=_NOW).to_parquet(
        f"{_S3_BUCKET}/data/rki/raw/germany/cases/{date}.parquet",
        index=False,
    )
    logger.info("Cases successfully queried.")

    # vaccinations
    logger.info("Querying vaccinations...")
    response = requests.get(f"{_API_URL}/vaccinations")
    df = pd.json_normalize(response.json())

    date = f"{pd.to_datetime(df['meta.lastUpdate'][0]):%Y-%m-%d}"

    df.assign(api_call_ts_utc=_NOW).to_parquet(
        f"{_S3_BUCKET}/data/rki/raw/germany/vaccinations/{date}.parquet",
        index=False,
    )
    logger.info("Vaccinations successfully queried.")
