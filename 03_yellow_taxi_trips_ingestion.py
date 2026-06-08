# Databricks notebook source
# MAGIC %md
# MAGIC # READ NEW FILES INTO THE LANDING TABLE

# COMMAND ----------
try:
    spark.sql("""
    COPY INTO nyctaxi.landing.trips_raw
    FROM '/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/trips/'
    FILEFORMAT = PARQUET
    FORMAT_OPTIONS ('recursiveFileLookup' = 'true');
    """)

    print("Raw data extracted successfully.")
except Exception as e:
    print(f"Error: {e}")
    raise
