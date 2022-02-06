from functools import reduce

import boto3
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkSession.builder.getOrCreate()

glueContext = GlueContext(sc.sparkContext)
logger = glueContext.get_logger()

read_schema = ",".join(
    [
        "cases long",
        "deaths long",
        "recovered long",
        "weekIncidence double",
        "casesPer100k double",
        "casesPerWeek long",
        "`delta.cases` long",
        "`delta.deaths` long",
        "`delta.recovered` long",
        "`r.value` double",
        "`r.rValue4Days.value` double",
        "`r.rValue4Days.date` string",
        "`r.rValue7Days.value` double",
        "`r.rValue7Days.date` string",
        "`r.lastUpdate` string",
        "`hospitalization.cases7Days` long",
        "`hospitalization.incidence7Days` double",
        "`hospitalization.date` string",
        "`hospitalization.lastUpdate` string",
        "`meta.source` string",
        "`meta.contact` string",
        "`meta.info` string",
        "`meta.lastUpdate` string",
        "`meta.lastCheckedForUpdate` string",
    ]
)

bucket = "data-pipeline-s3-bucket-production"
prefix = "data/vaccinations/valid"
s3_client = boto3.client("s3")

initial_sdf = sc.createDataFrame([], schema=read_schema)

sdf = reduce(
    lambda x, y: x.union(
        sc.read.parquet(
            f"s3://{bucket}/{y}",
            schema=read_schema,
        )
    ),
    [
        e["Key"]
        for e in s3_client.list_objects(Bucket=bucket, Prefix=prefix)["Contents"]
    ],
    initial_sdf,
)

sdf.select(
    [col(x).alias(y) for x, y in [(f"`{i}`", i.replace(".", "_")) for i in sdf.columns]]
).drop(
    "meta_source",
    "meta_contact",
    "meta_info",
    "meta_lastUpdate",
    "meta_lastCheckedForUpdate",
).write.mode(
    "overwrite"
).parquet(
    "s3://data-pipeline-s3-bucket-production/data/vaccinations/processed/spark-data/"
)
