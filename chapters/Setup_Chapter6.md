---
jupyter:
  jupytext:
    encoding: '# -*- coding: utf-8 -*-'
    formats: ipynb,md,py
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region slideshow={"slide_type": "slide"} -->
# Setup of Chapter 6

Please review Chapter 6 before proceeding. 
<!-- #endregion -->

```python
import pyspark

from pyspark.sql import SparkSession

#conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')])

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .config("spark.executor.memory", "8g")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()

sc = pyspark.SparkContext

#sc = spark.sparkContext
```

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

air_schema = T.StructType([
    T.StructField("Year", T.IntegerType()),
    T.StructField("Quarter", T.IntegerType()),
    T.StructField("Month", T.IntegerType()),
    T.StructField("DayofMonth", T.IntegerType()),
    T.StructField("DayOfWeek", T.IntegerType()),
    T.StructField("FlightDate", T.StringType()),
    T.StructField("UniqueCarrier", T.StringType()),
    T.StructField("AirlineID", T.LongType()),
    T.StructField("Carrier", T.StringType()),
    T.StructField("TailNum", T.StringType()),
    T.StructField("FlightNum", T.IntegerType()),
    T.StructField("OriginAirportID", T.IntegerType()),
    T.StructField("OriginAirportSeqID", T.IntegerType()),
    T.StructField("OriginCityMarketID", T.IntegerType()),
    T.StructField("Origin", T.StringType()),
    T.StructField("OriginCityName", T.StringType()),
    T.StructField("OriginState", T.StringType()),
    T.StructField("OriginStateFips", T.IntegerType()),
    T.StructField("OriginStateName", T.StringType()),
    T.StructField("OriginWac", T.IntegerType()),
    T.StructField("DestAirportID", T.IntegerType()),
    T.StructField("DestAirportSeqID", T.IntegerType()),
    T.StructField("DestCityMarketID", T.IntegerType()),
    T.StructField("Dest", T.StringType()),
    T.StructField("DestCityName", T.StringType()),
    T.StructField("DestState", T.StringType()),
    T.StructField("DestStateFips", T.IntegerType()),
    T.StructField("DestStateName", T.StringType()),
    T.StructField("DestWac", T.IntegerType()),
    T.StructField("CRSDepTime", T.StringType()),
    T.StructField("DepTime", T.StringType()),
    T.StructField("DepDelay", T.DoubleType()),
    T.StructField("DepDelayMinutes", T.DoubleType()),
    T.StructField("DepDel15", T.DoubleType()),
    T.StructField("DepartureDelayGroups", T.IntegerType()),
    T.StructField("DepTimeBlk", T.StringType()),
    T.StructField("TaxiOut", T.DoubleType()),
    T.StructField("WheelsOff", T.StringType()),
    T.StructField("WheelsOn", T.StringType()),
    T.StructField("TaxiIn", T.DoubleType()),
    T.StructField("CRSArrTime", T.StringType()),
    T.StructField("ArrTime", T.StringType()),
    T.StructField("ArrDelay", T.DoubleType()),
    T.StructField("ArrDelayMinutes", T.DoubleType()),
    T.StructField("ArrDel15", T.DoubleType()),
    T.StructField("ArrivalDelayGroups", T.IntegerType()),
    T.StructField("ArrTimeBlk", T.StringType()),
    T.StructField("Cancelled", T.DoubleType()),
    T.StructField("CancellationCode", T.StringType()),
    T.StructField("Diverted", T.DoubleType()),
    T.StructField("CRSElapsedTime", T.DoubleType()),
    T.StructField("ActualElapsedTime", T.DoubleType()),
    T.StructField("AirTime", T.DoubleType()),
    T.StructField("Flights", T.DoubleType()),
    T.StructField("Distance", T.DoubleType()),
    T.StructField("DistanceGroup", T.IntegerType()),
    T.StructField("CarrierDelay", T.DoubleType()),
    T.StructField("WeatherDelay", T.DoubleType()),
    T.StructField("NASDelay", T.DoubleType()),
    T.StructField("SecurityDelay", T.DoubleType()),
    T.StructField("LateAircraftDelay", T.DoubleType()),
    T.StructField("FirstDepTime", T.StringType()),
    T.StructField("TotalAddGTime", T.StringType()),
    T.StructField("LongestAddGTime", T.StringType()),
    T.StructField("DivAirportLandings", T.StringType()),
    T.StructField("DivReachedDest", T.StringType()),
    T.StructField("DivActualElapsedTime", T.StringType()),
    T.StructField("DivArrDelay", T.StringType()),
    T.StructField("DivDistance", T.StringType()),
    T.StructField("Div1Airport", T.StringType()),
    T.StructField("Div1AirportID", T.StringType()),
    T.StructField("Div1AirportSeqID", T.StringType()),
    T.StructField("Div1WheelsOn", T.StringType()),
    T.StructField("Div1TotalGTime", T.StringType()),
    T.StructField("Div1LongestGTime", T.StringType()),
    T.StructField("Div1WheelsOff", T.StringType()),
    T.StructField("Div1TailNum", T.StringType()),
    T.StructField("Div2Airport", T.StringType()),
    T.StructField("Div2AirportID", T.StringType()),
    T.StructField("Div2AirportSeqID", T.StringType()),
    T.StructField("Div2WheelsOn", T.StringType()),
    T.StructField("Div2TotalGTime", T.StringType()),
    T.StructField("Div2LongestGTime", T.StringType()),
    T.StructField("Div2WheelsOff", T.StringType()),
    T.StructField("Div2TailNum", T.StringType()),
    T.StructField("Div3Airport", T.StringType()),
    T.StructField("Div3AirportID", T.StringType()),
    T.StructField("Div3AirportSeqID", T.StringType()),
    T.StructField("Div3WheelsOn", T.StringType()),
    T.StructField("Div3TotalGTime", T.StringType()),
    T.StructField("Div3LongestGTime", T.StringType()),
    T.StructField("Div3WheelsOff", T.StringType()),
    T.StructField("Div3TailNum", T.StringType()),
    T.StructField("Div4Airport", T.StringType()),
    T.StructField("Div4AirportID", T.StringType()),
    T.StructField("Div4AirportSeqID", T.StringType()),
    T.StructField("Div4WheelsOn", T.StringType()),
    T.StructField("Div4TotalGTime", T.StringType()),
    T.StructField("Div4LongestGTime", T.StringType()),
    T.StructField("Div4WheelsOff", T.StringType()),
    T.StructField("Div4TailNum", T.StringType()),
    T.StructField("Div5Airport", T.StringType()),
    T.StructField("Div5AirportID", T.StringType()),
    T.StructField("Div5AirportSeqID", T.StringType()),
    T.StructField("Div5WheelsOn", T.StringType()),
    T.StructField("Div5TotalGTime", T.StringType()),
    T.StructField("Div5LongestGTime", T.StringType()),
    T.StructField("Div5WheelsOff", T.StringType()),
    T.StructField("Div5TailNum", T.StringType())
])
```

```python
type(spark)
```

```python
raw_df = spark.read.csv( 
        'file:///disk/airline-data/On_Time_On_Time_Performance_*.csv', 
        header=True, 
        schema=air_schema,
        escape='"')

airline_data = raw_df.select(
        "Year","Quarter","Month","DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID",
        "Carrier","TailNum","FlightNum","OriginAirportID","OriginAirportSeqID","OriginCityMarketID",
        "Origin","OriginCityName","OriginState","OriginStateFips","OriginStateName","OriginWac",
        "DestAirportID","DestAirportSeqID","DestCityMarketID","Dest","DestCityName","DestState",
        "DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime","DepDelay","DepDelayMinutes",
        "DepDel15","DepartureDelayGroups","DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime",
        "ArrTime","ArrDelay","ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled",
        "CancellationCode","Diverted","CRSElapsedTime","ActualElapsedTime","AirTime","Flights","Distance",
        "DistanceGroup","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"
    ).withColumn(
        'FlightDate', F.to_date(F.col('FlightDate'),'yyyy-MM-dd')
    )
```

```python
airline_data.repartition('Year').write.partitionBy(
        "Year","Month"
    ).parquet(
        'file:///disk/airline-data-processed/airline-data.parquet',
        mode='overwrite'
    )
```

```python
from pyspark.sql import Row

def mapAirlineIdRow(r):
    airline_id = int(r.Code)
    airline_name_parts = r.Description.split(':')
    airline_name = airline_name_parts[0].strip()
    iata_carrier = airline_name_parts[1].strip()
    out = Row(
        AirlineID=airline_id,
        AirlineName=airline_name,
        Carrier=iata_carrier
    )
    return out;

airline_id_csv = spark.read.csv(
    'file:///disk/airline-data/LUT-DOT_airline_IDs.csv',
    header=True,
    escape='"'
)

airline_id_df = airline_id_csv.rdd.map(mapAirlineIdRow).toDF().coalesce(1)
airline_id_df.write.parquet(
        'file:///disk/airline-data/DOT_airline_codes_table',
        mode='overwrite'
    )
    
airline_id_df.take(1)

airport_schema = T.StructType([
    T.StructField("Code", T.StringType()),
    T.StructField("Description", T.StringType()),
])

def mapAirportIdRow(r):
    airport_id = r.Code
    airport_city = ''
    airport_name = ''
    airport_name_parts = r.Description.split(':')
    if len(airport_name_parts) is 2:
        airport_city = airport_name_parts[0].strip()
        airport_name = airport_name_parts[1].strip()
    elif len(airport_name_parts) is 1:
        airport_city = airport_name_parts[0]
        airport_name = r.Code
    
    out = Row(
        
        AirportID=airport_id,
        City=airport_city,
        Name=airport_name
    )
    return out;

airport_codes_csv = spark.read.csv(
    'file:///disk/airline-data/LUT-airport_codes.csv',
    header=True,
    escape='"',
    schema=airport_schema
)

airport_codes_df = airport_codes_csv.rdd.map(mapAirportIdRow).toDF().coalesce(1)
airport_codes_df.write.parquet(
        'file:///disk/airline-data-processed/airport_codes_table',
        mode='overwrite'
    )

airport_id_csv = spark.read.csv(
    'file:///disk/airline-data/LUT-DOT_airport_IDs.csv',
    header=True,
    escape='"',
    schema=airport_schema
)

airport_id_df = (
    airport_id_csv
    .rdd.map(mapAirportIdRow)
    .toDF()
    .withColumn(
        'AirportID',
        F.col('AirportID').cast(T.IntegerType())
    )
    .coalesce(1)
)
airport_id_df.write.parquet(
        'file:///disk/airline-data-processed/airport_id_table',
        mode='overwrite'
    )
```
