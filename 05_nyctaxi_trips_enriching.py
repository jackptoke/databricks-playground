# Databricks notebook source
from utils.tools import get_first_day_of_month, get_first_day_of_next_month

month = dbutils.widgets.get("month")
first_day = get_first_day_of_month(month)
first_day_next_month = get_first_day_of_next_month(month)

try:
    # Check if data for the month already exists
    count_df = spark.sql("""
    SELECT COUNT(*) AS count FROM nyctaxi.silver.trips
    WHERE pickup_datetime >= :first_day AND pickup_datetime < :first_day_next_month
    """, args={
        "first_day": first_day.strftime("%Y-%m-%d"),
        "first_day_next_month": first_day_next_month.strftime("%Y-%m-%d")
        })
    # If data exists, skip processing
    if count_df.first()["count"] > 0:
        print(f"Data for {month} already exists")
    else:
        spark.sql("""
        INSERT INTO nyctaxi.silver.trips
        SELECT
            CASE
                WHEN t.vendor_id = 1 THEN 'Creative Mobile Technologies, LLC'
                WHEN t.vendor_id = 2 THEN 'Curb Mobility, LLC'
                WHEN t.vendor_id = 6 THEN 'Myle Technologies Inc'
                WHEN t.vendor_id = 7 THEN 'Helix'
                ELSE 'Unknown'
            END AS vendor,
            t.pickup_datetime,
            t.dropoff_datetime,
            z.zone AS pickup_zone,
            z.borough AS pickup_borough,
            z.service_zone AS pickup_service_zone,
            z2.zone AS dropoff_zone,
            z2.borough AS dropoff_borough,
            z2.service_zone AS dropoff_service_zone,
            t.passenger_count,
            CASE
                WHEN t.rate_code_id = 1 THEN 'Standard rate'
                WHEN t.rate_code_id = 2 THEN 'JFK'
                WHEN t.rate_code_id = 3 THEN 'Newark'
                WHEN t.rate_code_id = 4 THEN 'Nassau or Westchester'
                WHEN t.rate_code_id = 5 THEN 'Negotiated fare'
                WHEN t.rate_code_id = 6 THEN 'Group ride'
                ELSE 'Unknown'
            END AS rate_type,
            store_and_fwd_flag,
            CASE
                WHEN t.payment_type = 0  THEN 'Flex Fare Trip'
                WHEN t.payment_type = 1 THEN 'Credit card'
                WHEN t.payment_type = 2 THEN 'Cash'
                WHEN t.payment_type = 3 THEN 'No charge'
                WHEN t.payment_type = 4 THEN 'Dispute'
                WHEN t.payment_type = 6 THEN 'Voided trip'
                ELSE 'Unknown'
            END AS payment_type,
            t.trip_distance,
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
        FROM nyctaxi.bronze.trips t
        LEFT JOIN nyctaxi.silver.zones z
            ON t.pickup_location_id = z.location_id AND z.valid_to IS NULL
        LEFT JOIN nyctaxi.silver.zones z2
            ON t.dropoff_location_id = z2.location_id AND z2.valid_to IS NULL
        WHERE t.pickup_datetime >= CAST(:start_date AS DATE)
            AND t.pickup_datetime < CAST(:end_date AS DATE);
        """,
        args={
            "start_date": first_day,
            "end_date": first_day_next_month
            })

        # dbutils.jobs.taskValues.set(key="continue_downstream", value="true")
        print("Successfully processed trip data into the silver layer")
except Exception as e:
    print(f"Error: {e}")
    raise


# vendor
# tpep_pickup_datetime
# tpep_dropoff_datetime
# passenger_count
# trip_distance
# rate_type
# store_and_fwd_flag
# pickup_borough
# pickup_zone
# pickup_service_zone
# dropoff_borough
# dropoff_zone
# dropoff_service_zone
# DOLocationID
# payment_type
# fare_amount
# extra
# mta_tax
# tip_amount
# tolls_amount
