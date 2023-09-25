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

<!-- #region slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false -->
# Chapter 10.1 - Putting it all together

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

What is the first thing you are going to do once all your assignments are finished and exams are over?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Learning objectives:** Understand how most of the tools discussed in this textbook work together.
<!-- #endregion -->

```python
%load_ext autoreload
%autoreload 2

from pathlib import Path
home = str(Path.home()) # all other paths are relative to this path. 
# This is not relevant to most people because I recommended you use my server, but
# change home to where you are storing everything. Again. Not recommended.
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Dataset Overview

AirBnB provides a lot of data about their listings, reviews, and neighborhoods in different formats. I have downloaded only a small subset.
<!-- #endregion -->

```python
!ls -l {home}/csc-369-student/data/airbnb/LA/02_November_2021
```

## Exploring the files

```python
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
os.environ['PYSPARK_SUBMIT_ARGS'] += " --conf spark.cassandra.connection.host=127.0.0.1"

os.environ['PYSPARK_SUBMIT_ARGS'] += ' --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/csc-369.neighbourhoods_geo?readPreference=primaryPreferred"'
os.environ['PYSPARK_SUBMIT_ARGS'] += ' --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/csc-369.neighbourhoods_geo"'
os.environ['PYSPARK_SUBMIT_ARGS'] += " pyspark-shell"

```

```python
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

RDDs are great for taking a look at a file you are unfamiliar with:


### Calendar.csv.gz

```python
rdd = sc.textFile(f"{home}/csc-369-student/data/airbnb/LA/02_November_2021/calendar.csv.gz")

print("\n".join(rdd.take(10)))
```

We can read that directly with Spark SQL

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Chapter10") \
    .getOrCreate()
```

```python
df = spark.read.csv(f"{home}/csc-369-student/data/airbnb/LA/02_November_2021/calendar.csv.gz",header=True) 
df
```

It read the file, but it had trouble with the fields. We'll need to process them correctly.

```python
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date
from pyspark.sql.functions import regexp_replace


df = df.withColumn("listing_id", df["listing_id"].cast("long"))
df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))
df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))
df = df.withColumn('price', regexp_replace('price', '\$', '')).withColumn('price',col('price').cast('double'))
df = df.withColumn('adjusted_price', regexp_replace('adjusted_price', '\$', '')).withColumn('adjusted_price',col('adjusted_price').cast('double'))
df = df.withColumn("minimum_nights", df["minimum_nights"].cast("int"))
df = df.withColumn("maximum_nights", df["maximum_nights"].cast("int"))

df
```

```python
df = df.na.drop(subset='price') # Remove any listings without price because we want to use it in a primary key later
```

```python
df.take(5)
```

```python
df.printSchema()
```

Since we are going to have a lot of these files in theory, it is good to process them and then insert them into a Cassandra database.

```python
from cassandra.cluster import Cluster
cluster = Cluster(['0.0.0.0'],port=9042)
session = cluster.connect()
```

```python
for row in session.execute('select release_version from system.local;'):
    print(row)
```

```python
session.execute("DROP KEYSPACE IF EXISTS airbnb")
```

```python
for row in session.execute("CREATE KEYSPACE airbnb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};"):
    print(row)

# We are using the first replica placement strategy, i.e.., Simple Strategy.

# And we are choosing the replication factor to 3 replica.
```

```python
for row in session.execute('DROP TABLE IF EXISTS airbnb.calendar;'):
    print(row)
for row in session.execute("""
CREATE TABLE airbnb.calendar 
( listing_id int, 
  date timestamp, 
  available text, 
  price double,
  adjusted_price double,
  minimum_nights int,
  maximum_nights int,
  
  PRIMARY KEY (date, price, listing_id)
);
"""):
    print(row)

#Item one is the partition key
#Item two is the first clustering column. Added_date is a timestamp so the sort order is chronological, ascending.
#Item three is the second clustering column. Since videoid is a UUID, we are including it so simply show that it is a part of a unique record.
```

```python
df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='calendar',keyspace='airbnb').save()
```

```python
for row in session.execute("SELECT price FROM airbnb.calendar LIMIT 50;"):
    print(row)
```

### listings.csv.gz

```python
rdd = sc.textFile(f"{home}/csc-369-student/data/airbnb/LA/02_November_2021/listings.csv.gz")

print("\n".join(rdd.take(10)))
```

```python
listings_df = spark.read.csv(f"{home}/csc-369-student/data/airbnb/LA/02_November_2021/listings.csv.gz",header=True,quote="\"",escape="\"",multiLine=True)
listings_df
```

```python
listings_df = listings_df.select(['id','host_id','name','description','bedrooms','host_neighbourhood','neighbourhood_group_cleansed'])
listings_df
```

```python
listings_df.take(5)
```

```python
listings_df = listings_df.withColumn("id", listings_df["id"].cast("long"))
listings_df = listings_df.withColumn("host_id", listings_df["host_id"].cast("long"))
listings_df = listings_df.withColumn("bedrooms", listings_df["bedrooms"].cast("int"))

listings_df
```

```python
for row in session.execute('DROP TABLE IF EXISTS airbnb.listings;'):
    print(row)
for row in session.execute("""
CREATE TABLE airbnb.listings 
( id bigint, 
  host_id bigint,
  name text,
  description text,
  bedrooms int,
  host_neighbourhood text,
  neighbourhood_group_cleansed text,
  PRIMARY KEY (host_id, id)
);
"""):
    print(row)
```

```python
listings_df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='listings',keyspace='airbnb').save()
```

```python
for row in session.execute("SELECT host_neighbourhood FROM airbnb.listings LIMIT 50;"):
    print(row)
```

### Spark Pushdown Filters

Spark will work to push down filters to the database to only return the data we need.

```python
df2 = spark.read.format("org.apache.spark.sql.cassandra").options(table='calendar',keyspace='airbnb').load()
df_with_pushdown = df2.filter(df2["price"] > 50)
df_with_pushdown.take(5)
```

## neighbourhoods.geojson

```python
!jq --compact-output ".features" {home}/csc-369-student/data/airbnb/LA/02_November_2021/neighbourhoods.geojson > {home}/output.geojson
```

```python
!mongoimport --db "csc-369" -c neighbourhoods_geo --file {home}/output.geojson --jsonArray
```

```python
from pymongo import MongoClient
client = MongoClient()

db = client["csc-369"]

col = db["neighbourhoods_geo"]
```

```python
for record in col.find().limit(5):
    print(record)
```

```python
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
```

```python
df.take(5)
```

### Viewing prices over time for each neighborhood

```python
df2 = spark.read.format("org.apache.spark.sql.cassandra").options(table='calendar',keyspace='airbnb').load()
df2_with_pushdown = df2.filter(df2["price"] > 50)
df2_with_pushdown.head()
```

```python
df3 = spark.read.format("org.apache.spark.sql.cassandra").options(table='listings',keyspace='airbnb').load()
df3_with_pushdown = df3.filter("host_neighbourhood is not null")
df3_with_pushdown.head()
```

```python
df4 = df2_with_pushdown.join(df3_with_pushdown,df2_with_pushdown["listing_id"] ==  df3_with_pushdown["id"],"inner")
df4.take(5)
```

```python
df4.groupBy("host_neighbourhood").count().show()
```

```python
df4.groupBy("host_neighbourhood").avg("adjusted_price").show()
```

```python
df5 = df4.groupBy("host_neighbourhood").count()
df6 = df4.groupBy("host_neighbourhood").avg("adjusted_price")
df5.join(df6,df5['host_neighbourhood'] == df6['host_neighbourhood']).show()
```

```python

```

```python

```
