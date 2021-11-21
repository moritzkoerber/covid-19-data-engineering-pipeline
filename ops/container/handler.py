import os

import pandas
import requests

_API_URL = os.environ["API_URL"]
_BUCKET = os.environ["BUCKET_URL"]


def handler(event, context):

    response = requests.get(_API_URL)
    df = pandas.json_normalize(response.json())

    df.to_csv(
        _BUCKET,
        index=False,
    )
