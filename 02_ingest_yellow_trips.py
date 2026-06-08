# Databricks notebook source
# MAGIC %md
# MAGIC # Simply download the files if it is not there already

# COMMAND ----------

# from datetime import datetime, date, timezone
# from dateutils.relativedelta import relativedelta
import os
import shutil
from urllib import request

month = dbutils.widgets.get("month")

print(f"Ingesting NYC Yello Taxi file for: {month}")

# eleven_months_ago = date.today() - relativedelta(monts=11)
# formatted_date = two_months_ago.strftime("%Y-%m")

dir_path = f"/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/trips/{month}"

file_path = f"{dir_path}/yellow_tripdata_{month}.parquet"

try:
    # check if the file is already exists
    dbutils.fs.ls(file_path)
    # dbutils.jobs.taskValues.set(key="continue_downstream", value="false")
    print(f"File {file_path} already exists")
except:
    try:
        # download the file
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
        response = request.urlopen(url)

        os.makedirs(dir_path, exist_ok=True)
        #open the local file and write the data from the remote file
        # --------------
        # Disable this section for flow testing.
        # Uncomment the following with clause when going live
        # --------------
        with open(file_path, "wb") as f:
            # copy the data from response to file
            shutil.copyfileobj(response, f)

        print(f"File persisted to {file_path}")
        print(f"Yellow taxi file for {month} downloaded and saved")
    except:
        print(f"Failed to download and save yellow taxi file for the month of {month}")
        raise


