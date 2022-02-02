from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkSession.builder.getOrCreate()

sdf = sc.read.parquet(
    "s3://data-engineering-project-production/data/vaccinations/valid/"
)

sdf.select(
    [
        col(x).alias(y)
        for x, y in zip(sdf.columns, [x.replace(".", "_") for x in sdf.columns])
    ]
).write.parquet("s3://data-engineering-project-production/data/vaccinations/processed/")
