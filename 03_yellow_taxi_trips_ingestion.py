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


