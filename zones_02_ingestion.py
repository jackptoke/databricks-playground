# Databricks notebook source
# MAGIC %md
# MAGIC # READ NEW FILES INTO THE LANDING TABLE

# COMMAND ----------

try:
    spark.sql("""
    COPY INTO nyctaxi.landing.yellow_zones_raw
    FROM '/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/zones/'
    FILEFORMAT = CSV
    FORMAT_OPTIONS ('header' = 'true');
    """)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="true")
    print("Data copied successfully.")
except Exception as e:
    print(f"Error: {e}")
    raise

# LocationID	Borough	Zone	service_zone
