# Databricks notebook source
# MAGIC %md
# MAGIC # ZONES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi.landing.yellow_zones_raw (
# MAGIC     LocationID STRING, 
# MAGIC     Borough STRING, 
# MAGIC     Zone STRING, 
# MAGIC     service_zone STRING
# MAGIC );   

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi.bronze.zones (
# MAGIC     location_id INT, 
# MAGIC     borough STRING,
# MAGIC     zone STRING, 
# MAGIC     service_zone STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi.silver.zones (
# MAGIC     location_id INT, 
# MAGIC     borough STRING,
# MAGIC     zone STRING, 
# MAGIC     service_zone STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM nyctaxi.landing.yellow_zones_raw;
# MAGIC -- SELECT * FROM nyctaxi.bronze.zones;
# MAGIC SELECT * FROM nyctaxi.silver.zones;

# COMMAND ----------

# MAGIC %md
# MAGIC # TRIPS

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.parquet.inferTimestampNTZ.enabled = false;
# MAGIC
# MAGIC DROP TABLE IF EXISTS nyctaxi.landing.trips_raw;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE nyctaxi.landing.trips_raw (
# MAGIC     VendorID INT,
# MAGIC     tpep_pickup_datetime TIMESTAMP_NTZ,
# MAGIC     tpep_dropoff_datetime TIMESTAMP_NTZ,
# MAGIC     passenger_count LONG,
# MAGIC     trip_distance DOUBLE,
# MAGIC     RatecodeID LONG,
# MAGIC     store_and_fwd_flag STRING,
# MAGIC     PULocationID INT,
# MAGIC     DOLocationID INT,
# MAGIC     payment_type LONG,
# MAGIC     fare_amount DOUBLE,
# MAGIC     extra DOUBLE,
# MAGIC     mta_tax DOUBLE,
# MAGIC     tip_amount DOUBLE,
# MAGIC     tolls_amount DOUBLE,
# MAGIC     improvement_surcharge DOUBLE,
# MAGIC     total_amount DOUBLE,
# MAGIC     congestion_surcharge DOUBLE,
# MAGIC     airport_fee DOUBLE,
# MAGIC     cbd_congestion_fee DOUBLE
# MAGIC );
# MAGIC
# MAGIC --  |-- VendorID: integer (nullable = true)
# MAGIC --  |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
# MAGIC --  |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
# MAGIC --  |-- passenger_count: long (nullable = true)
# MAGIC --  |-- trip_distance: double (nullable = true)
# MAGIC --  |-- RatecodeID: long (nullable = true)
# MAGIC --  |-- store_and_fwd_flag: string (nullable = true)
# MAGIC --  |-- PULocationID: integer (nullable = true)
# MAGIC --  |-- DOLocationID: integer (nullable = true)
# MAGIC --  |-- payment_type: long (nullable = true)
# MAGIC --  |-- fare_amount: double (nullable = true)
# MAGIC --  |-- extra: double (nullable = true)
# MAGIC --  |-- mta_tax: double (nullable = true)
# MAGIC --  |-- tip_amount: double (nullable = true)
# MAGIC --  |-- tolls_amount: double (nullable = true)
# MAGIC --  |-- improvement_surcharge: double (nullable = true)
# MAGIC --  |-- total_amount: double (nullable = true)
# MAGIC --  |-- congestion_surcharge: double (nullable = true)
# MAGIC --  |-- Airport_fee: double (nullable = true)
# MAGIC --  |-- cbd_congestion_fee: double (nullable = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nyctaxi.bronze.trips (
# MAGIC     vendor_id INT,
# MAGIC     pickup_datetime TIMESTAMP,
# MAGIC     dropoff_datetime TIMESTAMP,
# MAGIC     passenger_count LONG,
# MAGIC     trip_distance DOUBLE,
# MAGIC     rate_code_id LONG,
# MAGIC     store_and_fwd_flag STRING,
# MAGIC     pickup_location_id INT,
# MAGIC     dropoff_location_id INT,
# MAGIC     payment_type LONG,
# MAGIC     fare_amount DOUBLE,
# MAGIC     extra DOUBLE,
# MAGIC     mta_tax DOUBLE,
# MAGIC     tip_amount DOUBLE,
# MAGIC     tolls_amount DOUBLE,
# MAGIC     improvement_surcharge DOUBLE,
# MAGIC     total_amount DOUBLE,
# MAGIC     congestion_surcharge DOUBLE,
# MAGIC     airport_fee DOUBLE,
# MAGIC     cbd_congestion_fee DOUBLE,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nyctaxi.silver.trips (
# MAGIC     vendor STRING,
# MAGIC     pickup_datetime TIMESTAMP,
# MAGIC     dropoff_datetime TIMESTAMP,
# MAGIC     pickup_zone STRING,
# MAGIC     pickup_borough STRING,
# MAGIC     pickup_service_zone STRING,
# MAGIC     dropoff_zone STRING,
# MAGIC     dropoff_borough STRING,
# MAGIC     dropoff_service_zone STRING,
# MAGIC     passenger_count INT,
# MAGIC     rate_type STRING,
# MAGIC     store_and_fwd_flag STRING,
# MAGIC     payment_type STRING,
# MAGIC     trip_distance DOUBLE,
# MAGIC     fare_amount DOUBLE,
# MAGIC     extra DOUBLE,
# MAGIC     mta_tax DOUBLE,
# MAGIC     tip_amount DOUBLE,
# MAGIC     tolls_amount DOUBLE,
# MAGIC     improvement_surcharge DOUBLE,
# MAGIC     total_amount DOUBLE,
# MAGIC     congestion_surcharge DOUBLE,
# MAGIC     airport_fee DOUBLE,
# MAGIC     cbd_congestion_fee DOUBLE,
# MAGIC     processed_timestamp TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE nyctaxi.gold.daily_trips_summary;

# COMMAND ----------

df = spark.read.parquet("/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/trips/yellow_tripdata_2025-01.parquet")
df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nyctaxi.gold.daily_trips_summary (
# MAGIC     pickup_date DATE,
# MAGIC     total_trips LONG,
# MAGIC     average_passengers INT,
# MAGIC     average_distance DOUBLE,
# MAGIC     total_fare DOUBLE,
# MAGIC     max_fare DOUBLE,
# MAGIC     min_fare DOUBLE,
# MAGIC     total_revenue DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nyctaxi.bronze.nyctaxi_zones_raw;
# MAGIC DROP TABLE IF EXISTS nyctaxi.bronze.yellow_trips_raw;
# MAGIC DROP TABLE IF EXISTS nyctaxi.gold.yellow_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS count FROM nyctaxi.bronze.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyctaxi.gold.daily_trips_summary WHERE pickup_date = '2026-04-01'

# COMMAND ----------

