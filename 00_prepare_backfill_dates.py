# Databricks notebook source
from datetime import datetime

from utils.tools import get_backfill_months

try:
    num_months = dbutils.widgets.get("num_months")

    print(f"Number of months to backfill: {num_months}")

    months = get_backfill_months(num_months=int(num_months))
    print(f"Months to backfill: {months}")

    recent_run = datetime.today()

    dbutils.jobs.taskValues.set(key="months", value=months)
    dbutils.jobs.taskValues.set(key="most_recent_run", value=recent_run.isoformat())
except Exception as e:
    print(f"Error: {e}")
    raise
