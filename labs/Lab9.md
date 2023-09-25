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
# Lab 9 - Bringing it all together
<!-- #endregion -->

This is the last lab! We will bring together several of the big technologies and approaches we have discussed in class:
* MongoDB
* Spark
* Spark SQL
* Cassandra
* Spark Streaming

If you have not already done so, make sure you review and are familiar with Chapter 10. We are going to continue to use the AirBnB data sets available at: http://insideairbnb.com/get-the-data.html

In the chapter we processed a few of the files and stored them in both MongoDB and Cassandra depending on the structure of the file. We didn't process all of the available files. Nor did we construct any solutions to bringing in new files. We will do both of those things in this lab. 


**Customizing for youself:**

We don't want to interfere with each other when we writing to our databases. 

```python
import getpass

username = getpass.getuser().split("@")[0].replace("-","_")

print(username)
```

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

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Lab9") \
    .getOrCreate()
```

```python
from cassandra.cluster import Cluster
cluster = Cluster(['0.0.0.0'],port=9042)
session = cluster.connect()
```

```python
session.execute(f"DROP KEYSPACE IF EXISTS {username}_airbnb")
```

```python
for row in session.execute("CREATE KEYSPACE %s_airbnb WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};"%username):
    print(row)
```

### Important detail below:

```python
print(f"Your keyspace is: {username}_airbnb")
```

Make sure you put your tables under this keyspace.


Now create your MongoDB database:

```python
from pymongo import MongoClient
client = MongoClient()

db = client[f"{username}_airbnb"]

db
```

```python
print(f"Your MongoDB database is: {username}_airbnb. Make sure you use this one instead of csc-369.")
```

**Data location**

```python
!ls /disk/airbnb
```

**Exercise 1:** Using the approach demonstrated in Chapter 10, load the summary (.csv) and detailed (.csv.gz) files into Spark and then into Cassandra tables. Provide verification and documentation of your approach.


**Exercise 2:** Using the approached demonstrated in Chapter 10, load neighbourhoods.geojson into MongoDB. Provide verification and documentation of your approach.


**Exercise 3:** Now that you have a programatic way of loading files into the databases, create a Spark streaming application that monitors a directory where we can load our airbnb files to be automatically detected and processed. i.e., do what you did in exercise 1 and exercise 2 automatically when the user adds files to a directory you are monitoring with Spark streaming.


**Exercise 4:** First, describe in detail a question you would like to answer from the airbnb dataset. The answer to the question must involve at least two of the tables OR a single table and the MongoDB collection. Second, provide the Spark/Cassandra/Mongo solution to your question.


**Submission:** When you are done with this lab, upload a PDF providing the necessary documentation of your progress to Canvas.

```python

```
