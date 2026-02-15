# Glue ETL: process_user_profiles (raw -> silver)
# Дані ідеальної якості, тому пропускаємо bronze

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split, to_date, trim, element_at, expr, when, length, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'data_lake_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATA_LAKE_BUCKET = args['data_lake_bucket']

# RAW -> SILVER (JSONLine формат)
print("--- RAW -> SILVER ---")
raw_df = spark.read.json(f"s3://{DATA_LAKE_BUCKET}/raw/user_profiles/")
print(f"Raw: {raw_df.count()} записів")

# Розбиваємо full_name на first_name (всі слова крім останнього) та last_name (останнє слово)
silver_df = raw_df.select(
    trim(col("email")).alias("email"),
    expr("array_join(slice(split(full_name, ' '), 1, size(split(full_name, ' ')) - 1), ' ')").alias("first_name"),
    element_at(split(col("full_name"), " "), -1).alias("last_name"),
    trim(col("state")).alias("state"),
    to_date(col("birth_date"), "yyyy-MM-dd").alias("birth_date"),
    trim(col("phone_number")).alias("phone_number")
)

# Якщо ім'я з одного слова - воно йде в first_name, last_name = NULL
silver_df = silver_df.withColumn(
    "_single", when(length(col("first_name")) == 0, lit(True)).otherwise(lit(False))
).withColumn(
    "first_name", when(col("_single"), col("last_name")).otherwise(col("first_name"))
).withColumn(
    "last_name", when(col("_single"), lit(None)).otherwise(col("last_name"))
).drop("_single")

silver_df.write.mode("overwrite").parquet(f"s3://{DATA_LAKE_BUCKET}/silver/user_profiles/")
print(f"Silver: {silver_df.count()} записів")

job.commit()
