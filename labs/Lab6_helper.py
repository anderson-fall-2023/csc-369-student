import numpy as np

from pyspark.sql.functions import avg, udf, col
from pyspark.sql.types import StringType

def getYearMonthStr(year, month):
    return '%d-%02d'%(year,month)

udfGetYearMonthStr = udf(getYearMonthStr, StringType())

def exercise_1(on_time_df,spark):
    result = None
    # Your solution here
    return result

def exercise_2(airline_delay,airlines,spark):
    result = None
    # Your solution here
    return result

def exercise_3(airline_delay2,spark):
    result = None
    # partial solution
    # airline_delay2.groupBy(?).pivot(?).agg(avg(?))
    # Your solution here
    return result
