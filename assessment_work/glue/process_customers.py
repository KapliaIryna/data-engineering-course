# Glue ETL: process_customers (raw ->  bronze ->  silver)

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, trim, when, length, row_number
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'data_lake_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATA_LAKE_BUCKET = args['data_lake_bucket']

# RAW -> BRONZE (всі поля як STRING)
print("--- RAW ->  BRONZE ---")
raw_df = spark.read.option("header", "true").option("recursiveFileLookup", "true") \
    .csv(f"s3://{DATA_LAKE_BUCKET}/raw/customers/")

bronze_df = raw_df.select(
    col("id").cast("string").alias("Id"),
    col("firstname").cast("string").alias("FirstName"),
    col("lastname").cast("string").alias("LastName"),
    col("email").cast("string").alias("Email"),
    col("registrationdate").cast("string").alias("RegistrationDate"),
    col("state").cast("string").alias("State")
)

bronze_path = f"s3://{DATA_LAKE_BUCKET}/bronze/customers/"
bronze_df.write.mode("overwrite").parquet(bronze_path)
print(f"Bronze: {bronze_df.count()} записів")

# BRONZE -> SILVER (очистка, дедуплікація)
print("--- BRONZE ->  SILVER ---")
bronze_df = spark.read.parquet(bronze_path)

silver_df = bronze_df.select(
    trim(col("Id")).cast(IntegerType()).alias("client_id"),
    trim(col("FirstName")).alias("first_name"),
    trim(col("LastName")).alias("last_name"),
    trim(col("Email")).alias("email"),
    to_date(trim(col("RegistrationDate")), "yyyy-M-d").alias("registration_date"),
    trim(col("State")).alias("state")
)

# Порожні рядки заповнюємо NULL
silver_df = silver_df.select(
    col("client_id"),
    when(length(col("first_name")) == 0, None).otherwise(col("first_name")).alias("first_name"),
    when(length(col("last_name")) == 0, None).otherwise(col("last_name")).alias("last_name"),
    col("email"),
    col("registration_date"),
    when(length(col("state")) == 0, None).otherwise(col("state")).alias("state")
).filter(
    col("client_id").isNotNull() & col("email").isNotNull()
)

# Дедуплікація: залишаємо останній запис по client_id
window = Window.partitionBy("client_id").orderBy(col("registration_date").desc())
silver_df = silver_df.withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1).drop("rn")

silver_df.write.mode("overwrite").parquet(f"s3://{DATA_LAKE_BUCKET}/silver/customers/")
print(f"Silver: {silver_df.count()} записів")

job.commit()
