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
# Chapter 7.1 - Spark Streaming

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

What was the best halloween costume you saw over the weekend?

Also, what's the best candy?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Streaming and Data Analysis
Analyzing data as it comes in at a high velocity in real time.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://opensistemas.com/wp-content/uploads/2020/06/4-Vs-of-big-data-1.jpg">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Velocity

* Data drivers
    * Social media (e.g., Twitter)
    * IoT (e.g., Smart Watches)
    * Mobile applications
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Business Use Cases
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Streaming ETL
* Traditional ETL (Extract, Transform, Load) tools used for batch processing in data warehouse environments must read data, convert it to a database compatible format, and then write it to the target database
* With Streaming ETL, data is continually cleaned and aggregated before it is pushed into data stores.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Data Enrichment
* Enriches live data by combining it with static data, thus allowing organizations to conduct more complete real-time data analysis.
* e.g., Online advertisers use data enrichment to combine historical customer data with live customer behavior data and deliver more personalized and targeted ads in real-time and in context with what customers are doing.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Trigger Event Detection
* Detect and respond quickly to rare or unusual behaviors (“trigger events”) that could indicate a potentially serious problem within the system. 
* Financial institutions use triggers to detect fraudulent transactions and stop fraud in their tracks. * Hospitals also use triggers to detect potentially dangerous health changes while monitoring patient vital signs—sending automatic alerts to the right caregivers who can then take immediate and appropriate action.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Complex Session Analysis
* Events relating to live sessions—such as user activity after logging into a website or application—can be grouped together and quickly analyzed
* Session information can also be used to continuously update machine learning models
* Companies such as Netflix use this functionality to gain immediate insights as to how users are engaging on their site and provide more real-time movie recommendations
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Other high level use cases
* Twitter wants to process billions of tweets/s to publish trending topics
* Credit card companies need to process millions of transactions for identifying fraud
* Mobile applications like whatsapp need to constantly crunch logs for service availability
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Real Time Analytics
* We need to process TB's of streaming data in real time to get up to date analysis
* Data will be coming from more than one stream
* Need to combine historical data with real time data
* Ability to process stream data for downstream application
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## There are alternatives to Spark
* Apache Storm
    * Stream processing built on HDFS
    * Built by twitter
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Spark Streaming
<img src="https://miro.medium.com/max/720/1*FLYjc6U-qAQ64yDLLrzdWw.jpeg">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Micro batch
* Spark streaming is a fast batch processing system
* Collects stream data into small batches and processes them
* Batch interval can be small (1s) or multiple hours
* Batches are called DStreams
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Example: WordCount
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### The usual SparkContext
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Grab a streaming context
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 1)
```

```python slideshow={"slide_type": "subslide"}
PORT=9999 # Change this to a unique port before running individually
HOST="localhost"
```

```python slideshow={"slide_type": "subslide"}
print("Run this command at the terminal and type in words and hit enter periodically:")
print(f"nc -lk {PORT}")
```

```python slideshow={"slide_type": "subslide"}
lines = ssc.socketTextStream(HOST, PORT)
counts = lines.flatMap(lambda line: line.split(" "))\
              .map(lambda word: (word, 1))\
              .reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(60) # wait 60 seconds
```
