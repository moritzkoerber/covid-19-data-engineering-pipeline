import os
from datetime import date

import boto3

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

_S3_BUCKET = os.environ["S3_BUCKET"]
_S3_FOLDER = os.environ["S3_FOLDER"]
_DATASOURCE = os.environ["DATASOURCE"]
_CHECKPOINT = os.environ["CHECKPOINT"]

_date_today = date.today().strftime("%Y-%m-%d")


def handler(event, context):
    context_cfg = {
        "stores": {
            "expectations_s3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": f"{_S3_BUCKET}",
                    "prefix": "expectations",
                },
            },
            "validations_s3_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": f"{_S3_BUCKET}",
                    "prefix": "validations",
                },
            },
            "checkpoints_s3_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": f"{_S3_BUCKET}",
                    "prefix": "checkpoints",
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        "expectations_store_name": "expectations_s3_store",
        "validations_store_name": "validations_s3_store",
        "checkpoint_store_name": "checkpoints_s3_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "anonymous_usage_statistics": {
            "enabled": False,
            "data_context_id": "ee03c210-16c2-4692-ba66-cb114f2bf6f9",
        },
    }
    data_context_config = DataContextConfig(**context_cfg)
    context = BaseDataContext(project_config=data_context_config)

    datasource_cfg = {
        "name": _DATASOURCE,
        "class_name": "Datasource",
        "execution_engine": {"class_name": "PandasExecutionEngine"},
        "data_connectors": {
            "s3_data_connector": {
                "class_name": "InferredAssetS3DataConnector",
                "bucket": f"{_S3_BUCKET}",
                "prefix": f"{_S3_FOLDER}/raw",
                "default_regex": {
                    "group_names": ["data_asset_name"],
                    "pattern": fr"{_S3_FOLDER}/raw/(\d{{4}}-\d{{2}}-\d{{2}})\.parquet",
                },
            },
        },
    }

    context.add_datasource(**datasource_cfg)

    checkpoint_dict = {
        "name": _CHECKPOINT,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": f"{_DATASOURCE}",
                    "data_connector_name": "s3_data_connector",
                    "data_asset_name": _date_today,
                    "data_connector_query": {"index": -1},
                },
                "expectation_suite_name": "exp_suite",
            }
        ],
    }

    context.add_checkpoint(**checkpoint_dict)

    results = context.run_checkpoint(checkpoint_name=_CHECKPOINT)

    print(results)

    if results["success"]:
        s3_client = boto3.client("s3")
        s3_client.copy_object(
            Bucket=f"{_S3_BUCKET}",
            CopySource=f"{_S3_BUCKET}/{_S3_FOLDER}/raw/{_date_today}.parquet",
            Key=f"{_S3_FOLDER}/processed/{_date_today}.parquet",
        )
        s3_client.delete_object(
            Bucket=f"{_S3_BUCKET}", Key=f"{_S3_FOLDER}/raw/{_date_today}.parquet"
        )

    else:
        raise Exception("Error: Data validation not successful")
