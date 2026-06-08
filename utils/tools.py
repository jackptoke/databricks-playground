from datetime import date, datetime

from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, TimestampType


@udf(returnType=ArrayType(StringType()))
def get_backfill_months(num_months: int) -> list[str]:
    return [(date.today().replace(day=1) - relativedelta(months= x + 2)).strftime('%Y-%m') for x in range(int(num_months))]

@udf(returnType=TimestampType())
def get_first_day_of_month(month: str) -> datetime:
    return datetime.strptime(month, "%Y-%m").replace(day=1)

@udf(returnType=TimestampType())
def get_first_day_of_next_month(month: str) -> datetime:
    first_day = get_first_day_of_month(month=month)
    return first_day + relativedelta(months=1)
