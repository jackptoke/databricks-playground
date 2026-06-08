# Databricks notebook source
# MAGIC %md
# MAGIC # Implement SCD2 - Silver Zones

# COMMAND ----------

try:
    # Implemetn SCD2
    spark.sql("""
    MERGE INTO nyctaxi.silver.zones AS z
    USING (
        SELECT u.location_id AS merge_key, u.*
        FROM nyctaxi.bronze.zones u

        UNION ALL

        SELECT NULL AS merge_key, u.*
        FROM nyctaxi.bronze.zones u
            JOIN nyctaxi.silver.zones z
            ON u.location_id = z.location_id
        WHERE z.valid_to IS NULL
            AND ( NOT (z.borough <=> u.borough)
            OR NOT (z.zone <=> u.zone)
            OR NOT (z.service_zone <=> u.service_zone)
        )
    ) AS staged
    ON z.location_id = staged.merge_key

    WHEN MATCHED AND z.valid_to IS NULL
        AND ( NOT (z.borough <=> staged.borough)
        OR NOT (z.zone <=> staged.zone)
        OR NOT (z.service_zone <=> staged.service_zone)
        )
    THEN UPDATE SET z.valid_to = current_timestamp()

    WHEN NOT MATCHED THEN
        INSERT (location_id, borough, zone, service_zone, valid_from, valid_to)
        VALUES (
            staged.location_id,
            staged.borough,
            staged.zone,
            staged.service_zone,
            current_timestamp(),
            NULL);
    """)

    print("Data copied successfully.")
except Exception as e:
    print(f"Error: {e}")
    raise

# LocationID	Borough	Zone	service_zone
