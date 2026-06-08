# Databricks notebook source
from utils.tools import get_first_day_of_month, get_first_day_of_next_month

month = dbutils.widgets.get("month")
first_day = get_first_day_of_month(month)
first_day_next_month = get_first_day_of_next_month(month)

try:
    # Check if the data summary for the month already exists
    count_df = spark.sql("""
    SELECT COUNT(*) AS count FROM nyctaxi.gold.daily_trips_summary
    WHERE pickup_date >= :first_day AND pickup_date < :first_day_next_month
    """, args={
        "first_day": first_day.strftime("%Y-%m-%d"),
        "first_day_next_month": first_day_next_month.strftime("%Y-%m-%d")
        })

    # If the data exists, skip
    if count_df.first()["count"] > 0:
        print(f"Data for {month} already exists")
    else:
        df = spark.sql("""
        INSERT INTO nyctaxi.gold.daily_trips_summary
        SELECT
            CAST(pickup_datetime AS DATE) AS pickup_date,
            COUNT(*) AS total_trips,
            ROUND(AVG(passenger_count)) AS average_passengers,
            ROUND(AVG(trip_distance)) AS average_distance,
            ROUND(SUM(fare_amount)) AS total_fare,
            MAX(fare_amount) AS max_fare,
            MIN(fare_amount) aS min_fare,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM nyctaxi.silver.trips
        GROUP BY pickup_date
        """,
        args={
            "start_date": first_day,
            "end_date": first_day_next_month
            })

        print("Successfully processed trip data into the silver layer")
except Exception as e:
    print(f"Error: {e}")
    raise

