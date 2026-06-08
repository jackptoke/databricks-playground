# Databricks notebook source
import os
import shutil
from urllib import request

try:
    lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

        #open a connection and stream the remote file
    response = request.urlopen(lookup_url)

        #create a local file
    dir_path = f"/Volumes/nyctaxi/landing/data_sources/nyctaxi_yellow/zones/"
    os.makedirs(dir_path, exist_ok=True)

    file_path = f"{dir_path}/taxi_zone_lookup.csv"
    #open the local file and write the data from the remote file
    with open(file_path, "wb") as f:
        # copy the data from response to file
        shutil.copyfileobj(response, f)
    dbutils.jobs.taskValues.set(key="continue_downstream", value="true")
except Exception as e:
    print(f"Failed to download file: {str(e)}")
    raise
