from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkContext()
glueContext = GlueContext(SparkContext.getOrCreate("default"))
sc = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)

sc = SparkSession.builder.getOrCreate()

sdf = sc.read.parquet(
    "s3://data-pipeline-s3-bucket-production/data/vaccinations/valid/"
)

sdf.select(
    [col(x).alias(y) for x, y in [(f"`{i}`", i.replace(".", "_")) for i in sdf.columns]]
).drop(
    "meta_source",
    "meta_contact",
    "meta_info",
    "meta_lastUpdate",
    "meta_lastCheckedForUpdate",
).write.parquet(
    "s3://data-pipeline-s3-bucket-production/data/vaccinations/processed/"
)


job.commit()
