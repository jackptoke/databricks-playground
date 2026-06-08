# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True)
])

# Read all parquet files from the landing directory into a dataframe
df = spark.read.format("parquet").load("/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/*", schema=schema)
df = df.withColumn("processed_timestamp", current_timestamp())
df.write.mode("overwrite").saveAsTable("nyctaxi.bronze.yellow_trips_raw")

# COMMAND ----------

df = spark.read.table("nyctaxi.bronze.yellow_trips_raw")
display(df.head(5))

# COMMAND ----------

