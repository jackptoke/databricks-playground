# Databricks notebook source
from utils.tools import get_first_day_of_month, get_first_day_of_next_month

month = dbutils.widgets.get("month")
first_day = get_first_day_of_month(month)
first_day_next_month = get_first_day_of_next_month(month)

dir_path = f"/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/{month}"
file_path = f"{dir_path}/yellow_tripdata_{month}.parquet"

try:
    # Check if data already exists for the month
    count_df = spark.sql("""
    SELECT COUNT(*) AS count FROM nyctaxi.bronze.trips
    WHERE pickup_datetime >= :first_day AND pickup_datetime < :first_day_next_month
    """, args={
        "first_day": first_day.strftime("%Y-%m-%d"),
        "first_day_next_month": first_day_next_month.strftime("%Y-%m-%d")
        })

    # If data exists, skip
    if count_df.first()["count"] > 0:
        print(f"Data for {month} already exists")
    else:
        # Insert new data into the bronze table
        spark.sql("""
        INSERT INTO nyctaxi.bronze.trips
        SELECT VendorID AS vendor_id,
            tpep_pickup_datetime AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            passenger_count,
            trip_distance,
            RatecodeID rate_code_id,
            store_and_fwd_flag,
            PULocationID pickup_location_id,
            DOLocationID dropoff_location_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            cbd_congestion_fee,
            current_timestamp() AS processed_timestamp
        FROM nyctaxi.landing.trips_raw
        WHERE tpep_pickup_datetime >= :first_day AND tpep_pickup_datetime < :first_day_next_month
        """, args={
            "first_day": first_day.strftime("%Y-%m-%d"),
            "first_day_next_month": first_day_next_month.strftime("%Y-%m-%d")
            })

        print(f"Successfully appended the new {month} data")
except Exception as e:
    print(f"Error: {e}")
    raise
