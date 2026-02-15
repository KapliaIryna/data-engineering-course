# Glue ETL: process_sales (raw → bronze → silver)

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace, to_date, trim
from pyspark.sql.types import IntegerType, DecimalType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'data_lake_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATA_LAKE_BUCKET = args['data_lake_bucket']

# RAW -> BRONZE (всі поля як STRING, оригінальні назви колонок)
print("--- RAW -> BRONZE ---")
raw_df = spark.read.option("header", "true").option("recursiveFileLookup", "true") \
    .csv(f"s3://{DATA_LAKE_BUCKET}/raw/sales/")

bronze_df = raw_df.select(
    col("customerid").cast("string").alias("CustomerId"),
    col("purchasedate").cast("string").alias("PurchaseDate"),
    col("product").cast("string").alias("Product"),
    col("price").cast("string").alias("Price")
)

bronze_path = f"s3://{DATA_LAKE_BUCKET}/bronze/sales/"
bronze_df.write.mode("overwrite").parquet(bronze_path)
print(f"Bronze: {bronze_df.count()} записів")

# BRONZE -> SILVER (очистка: прибираємо $, конвертуємо типи, snake_case)
print("--- BRONZE -> SILVER ---")
bronze_df = spark.read.parquet(bronze_path)

silver_df = bronze_df.select(
    trim(col("CustomerId")).cast(IntegerType()).alias("client_id"),
    to_date(trim(col("PurchaseDate")), "yyyy-M-d").alias("purchase_date"),
    trim(col("Product")).alias("product_name"),
    regexp_replace(trim(col("Price")), "\\$", "").cast(DecimalType(10, 2)).alias("price")
).filter(
    col("client_id").isNotNull() &
    col("purchase_date").isNotNull() &
    col("product_name").isNotNull()
)

# Партиціонування по даті для аналітики
silver_df.write.mode("overwrite").partitionBy("purchase_date") \
    .parquet(f"s3://{DATA_LAKE_BUCKET}/silver/sales/")
print(f"Silver: {silver_df.count()} записів")

job.commit()
