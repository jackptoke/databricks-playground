# Databricks notebook source
from utils.tools import get_backfill_months

try:
    num_months = dbutils.widgets.get("num_months")

    print(f"Number of months to backfill: {num_months}")

    months = get_backfill_months(num_months=int(num_months))
    print(f"Months to backfill: {months}")

    dbutils.jobs.taskValues.set(key="months", value=months)
except Exception as e:
    print(f"Error: {e}")
    raise
