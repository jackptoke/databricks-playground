# Databricks notebook source
# MAGIC %md
# MAGIC # READ NEW FILES INTO THE LANDING TABLE

# COMMAND ----------
from utils.tools import get_first_day_of_month, get_first_day_of_next_month

month = dbutils.widgets.get("month")
first_day = get_first_day_of_month(month)
first_day_next_month = get_first_day_of_next_month(month)

dir_path = f"/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/trips/{month}/"

try:
    count_df = spark.sql("""
    SELECT COUNT(*) AS count FROM nyctaxi.landing.trips_raw
    WHERE tpep_pickup_datetime >= :first_day AND tpep_pickup_datetime < :first_day_next_month
    """, args={
        "first_day": first_day.strftime("%Y-%m-%d"),
        "first_day_next_month": first_day_next_month.strftime("%Y-%m-%d")
        })

    # If data exists, skip
    if count_df.first()["count"] > 0:
        print(f"Data for {month} already exists")
    else:
        spark.sql(f"""
        COPY INTO nyctaxi.landing.trips_raw
        FROM '{dir_path}'
        FILEFORMAT = PARQUET
        FORMAT_OPTIONS ('recursiveFileLookup' = 'true');
        """)
        print("Raw data extracted successfully.")
except Exception as e:
    print(f"Error: {e}")
    raise


