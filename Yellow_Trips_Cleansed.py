# Databricks notebook source
df = spark.read.table("nyctaxi.bronze.yellow_trips_raw")
df = df.filter("tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime < '2025-07-01'")
display(df.head(5))

#")

# COMMAND ----------

from pyspark.sql.functions import when, col

# A code indicating the TPEP provider that provided the record.
# 1 = Creative Mobile Technologies, LLC
# 2 = Curb Mobility, LLC
# 6 = Myle Technologies Inc
# 7 = Helix
start_date = "2025-01-01"
end_date = "2025-07-01"


query = """
    SELECT 
        CASE 
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID = 2 THEN 'Curb Mobility, LLC'
            WHEN VendorID = 6 THEN 'Myle Technologies Inc'
            WHEN VendorID = 7 THEN 'Helix'
            ELSE 'Unknown'
        END AS vendor,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        CASE 
            WHEN RatecodeID = 1 THEN 'Standard rate'
            WHEN RatecodeID = 2 THEN 'JFK'
            WHEN RatecodeID = 3 THEN 'Newark'
            WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
            WHEN RatecodeID = 5 THEN 'Negotiated fare'
            WHEN RatecodeID = 6 THEN 'Group ride'
            ELSE 'Unknown'
        END AS rate_type,
        store_and_fwd_flag,
        z.Borough AS pickup_borough,
        z.Zone AS pickup_zone,
        z.service_zone AS pickup_service_zone,
        z2.Borough AS dropoff_borough,
        z2.Zone AS dropoff_zone,
        z2.service_zone AS dropoff_service_zone,
        DOLocationID,
        CASE 
            WHEN payment_type = 0  THEN 'Flex Fare Trip'
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END AS payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        cbd_congestion_fee
    FROM nyctaxi.bronze.yellow_trips_raw t
    LEFT JOIN nyctaxi.bronze.nyctaxi_zones_raw z
        ON t.PULocationID = z.LocationID
    LEFT JOIN nyctaxi.bronze.nyctaxi_zones_raw z2
        ON t.DOLocationID = z2.LocationID
    WHERE tpep_pickup_datetime >= CAST(:start_date AS DATE) AND tpep_pickup_datetime < CAST(:end_date AS DATE);"""
df = spark.sql(query, args={"start_date": start_date, "end_date":end_date})

display(df.head(5))

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.gold.yellow_trips")

# COMMAND ----------

from pyspark.sql.functions import max, min

spark.read.table("nyctaxi.gold.yellow_trips").\
    agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).\
        display()


# COMMAND ----------

