# Databricks notebook source
# MAGIC %md
# MAGIC ## Download new files to the storage

# COMMAND ----------

from urllib import request
import os
import shutil

dates_to_process = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06', ]

for date in dates_to_process:
    # prepare the download url
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"
    
    #open a connection and stream the remote file
    response = request.urlopen(url)
    
    #create a local file
    dir_path = f"/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/{date}"
    os.makedirs(dir_path, exist_ok=True)

    file_path = f"{dir_path}/yellow_tripdata_{date}.parquet"
    #open the local file and write the data from the remote file
    with open(file_path, "wb") as f:
        # copy the data from response to file
        shutil.copyfileobj(response, f)
print('Download completed!')

# COMMAND ----------

# table location: toke_dataengineering_udemy_workspace.nyctaxi.tables
# files location: 
source_files = dbutils.fs.ls("/Volumes/toke_dataengineering_udemy_workspace/nyctaxi/dataset/")
display(source_files)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# column names: VendorID
# tpep_pickup_datetime
# tpep_dropoff_datetime
# passenger_count
# trip_distance 
# RatecodeID
# store_and_fwd_flag
# PULocationID
# DOLocationID
# payment_type
# fare_amount
# extra
# mta_tax 
# tip_amount
# tolls_amount
# improvement_surcharge
# total_amount
# congestion_surcharge
# airport_fee
# cbd_congestion_fee

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
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


# COMMAND ----------

# DBTITLE 1,Cell 3
for month in range(1, 7):
    file = f"/Volumes/toke_dataengineering_udemy_workspace/nyctaxi/dataset/yellow_tripdata_2025-{month:02d}.parquet"
    df = spark.read.parquet(file, schema=schema, header=True)
    df.write.mode("append").saveAsTable("nyctaxi.landing.yellow_trips")

# COMMAND ----------

df = spark.read.table("nyctaxi.landing.yellow_trips")
display(df.head(5))

# COMMAND ----------

