# Databricks notebook source
# MAGIC %md
# MAGIC # READ NEW FILES INTO THE LANDING TABLE

# COMMAND ----------

try:

    spark.sql("""
    CREATE OR REPLACE TABLE nyctaxi.bronze.zones AS
    SELECT
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone AS service_zone
    FROM nyctaxi.landing.yellow_zones_raw;
    """)

    print("Data copied successfully.")
except Exception as e:
    print(f"Error: {e}")
    raise

# LocationID	Borough	Zone	service_zone
