import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

args = getResolvedOptions(sys.argv, ["bucket", "prefix", "write_location"])
_BUCKET = args["bucket"]
_PREFIX = args["prefix"]
_WRITE_LOCATION = args["write_location"]

sc = SparkSession.builder.getOrCreate()

# enfore schema
data_schema = StructType(
    [
        StructField("cases", LongType(), True),
        StructField("deaths", LongType(), True),
        StructField("recovered", LongType(), True),
        StructField("weekIncidence", DoubleType(), True),
        StructField("casesPer100k", DoubleType(), True),
        StructField("casesPerWeek", LongType(), True),
        StructField("deathsPerWeek", LongType(), True),
        StructField("delta.cases", LongType(), True),
        StructField("delta.deaths", LongType(), True),
        StructField("delta.recovered", LongType(), True),
        StructField("delta.weekIncidence", DoubleType(), True),
        StructField("r.value", DoubleType(), True),
        StructField("r.rValue4Days.value", DoubleType(), True),
        StructField("r.rValue4Days.date", StringType(), True),
        StructField("r.rValue7Days.value", DoubleType(), True),
        StructField("r.rValue7Days.date", StringType(), True),
        StructField("r.lastUpdate", StringType(), True),
        StructField("hospitalization.cases7Days", LongType(), True),
        StructField("hospitalization.incidence7Days", DoubleType(), True),
        StructField("hospitalization.date", StringType(), True),
        StructField("hospitalization.lastUpdate", StringType(), True),
        StructField("meta.source", StringType(), True),
        StructField("meta.contact", StringType(), True),
        StructField("meta.info", StringType(), True),
        StructField("meta.lastUpdate", StringType(), True),
        StructField("meta.lastCheckedForUpdate", StringType(), True),
        StructField("api_call_ts_utc", TimestampType(), True),
    ]
)

sdf = sc.read.option("mergeSchema", "true").parquet(f"s3://{_BUCKET}/{_PREFIX}")

if sdf.schema != data_schema:
    sdf.printSchema()
    raise AssertionError("Error: Schema not valid.")

destination_sdf = sc.read.parquet(f"s3://{_BUCKET}/{_WRITE_LOCATION}")

result_df = destination_sdf.unionByName(
    sdf.select(
        [
            col(x).alias(y)
            for x, y in [(f"`{i}`", i.replace(".", "_")) for i in sdf.columns]
        ]
    ).drop(
        "meta_source",
        "meta_contact",
        "meta_info",
    ),
    allowMissingColumns=True,
).drop_duplicates()

result_df.cache()
result_df.limit(1).count()

result_df.write.mode("overwrite").parquet(f"s3://{_BUCKET}/{_WRITE_LOCATION}")
