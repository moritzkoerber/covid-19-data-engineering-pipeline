import logging
import os

import boto3
import botocore
from awslambdaric.lambda_context import LambdaContext
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

logging.getLogger().setLevel(logging.INFO)

_S3_BUCKET = os.environ["S3_BUCKET"]
_S3_FOLDER = os.environ["S3_FOLDER"]
_S3_FOLDER_SUCCESS = os.environ["S3_FOLDER_SUCCESS"]
_S3_FOLDER_FAILURE = os.environ["S3_FOLDER_FAILURE"]
_DATASOURCE = os.environ["DATASOURCE"]
_CHECKPOINT = os.environ["CHECKPOINT"]


def move_file(
    s3_client: botocore.client.S3,
    source_bucket: str,
    source_key: str,
    destination_key: str,
    destination_bucket: str = None,
):
    destination_bucket = destination_bucket or source_bucket

    s3_client.copy_object(
        Bucket=destination_bucket,
        CopySource=f"{source_bucket}/{source_key}",
        Key=destination_key,
    )
    s3_client.delete_object(Bucket=source_bucket, Key=source_key)


def handler(event: dict, context: LambdaContext):
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
                "prefix": f"{_S3_FOLDER}/raw/germany/cases",
                "default_regex": {
                    "group_names": ["data_asset_name"],
                    "pattern": rf"{_S3_FOLDER}/raw/germany/cases/(\d{{4}}-\d{{2}}-\d{{2}})\.parquet",  # noqa
                },
            },
        },
    }

    context.add_datasource(**datasource_cfg)

    derived_data_asset_names = context.get_available_data_asset_names()[_DATASOURCE][
        "s3_data_connector"
    ]

    logging.info(f"Derived data assets: {derived_data_asset_names}")
    errors = []
    for i in derived_data_asset_names:
        print(f"Validating data asset: {i}...")
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
                        "data_asset_name": i,
                        "data_connector_query": {"index": -1},
                    },
                    "expectation_suite_name": "exp_suite",
                }
            ],
        }

        context.add_checkpoint(**checkpoint_dict)

        results = context.run_checkpoint(checkpoint_name=_CHECKPOINT)

        logging.info(results)

        source_key = f"{_S3_FOLDER}/raw/germany/cases/{i}.parquet"
        s3_client = boto3.client("s3")

        if results["success"]:
            move_file(
                s3_client=s3_client,
                source_bucket=_S3_BUCKET,
                source_key=source_key,
                destination_key=f"{_S3_FOLDER}/{_S3_FOLDER_SUCCESS}/germany/cases/{i}.parquet",
            )
            logging.info("Success.")

        else:
            move_file(
                s3_client=s3_client,
                source_bucket=_S3_BUCKET,
                source_key=source_key,
                destination_key=f"{_S3_FOLDER}/{_S3_FOLDER_FAILURE}/germany/cases/{i}.parquet",
            )
            errors.append(i)

    if errors:
        logging.error(f"Errors occurred in {', '.join(errors)}")
        raise Exception("Error: Data validation not successful.")
