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
# Chapter 7.3 - Spark Streaming

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

Best breakfast burrito in town?
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
%load_ext autoreload
%autoreload 2
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Problem Statement:
* You are approached by a company who has a machine learning pipeline that is trained and tested on historical data. 
* This pipeline is used by the company to sort tweets into one of three categories which also have a corresponding numerical label in parentheses.
    * Negative (0)
    * Positive (1)
    * Neutral (2)
    
The company has heard about your amazing skills as a Spark streaming expert. They would like you to take their pre-trained classifier and update it with new incoming data processed via Spark streaming.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Detours

In order to implement our streaming approach, we need to take a couple of brief detours into machine learning. We need to answer the following questions:
* How do we represent text as a vector of numbers such that a machine can mathematically learn from data?
* How to use and evaluate an algorithm to predict numeric data into three categories (negative, positive, and neutral)? 
<!-- #endregion -->

<!-- #region colab_type="text" id="YbTiSkqNb75B" slideshow={"slide_type": "subslide"} -->
### Representing text as a vector using `scikit-learn`

scikit-learn is a popular package for machine learning.

We will use a class called `CountVectorizer` in `scikit-learn` to obtain what is called the term-frequency matrix. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
A couple famous book openings:

> The story so far: in the beginning, the universe was created. This has made a lot of people very angry and been widely regarded as a bad move - The Restaurant at the End of the Universe by Douglas Adams (1980)

> Whether I shall turn out to be the hero of my own life, or whether that station will be held by anybody else, these pages must show. — Charles Dickens, David Copperfield (1850)

How will a computer understand these sentences when computers can only add/mult/compare numbers?
<!-- #endregion -->

```python colab={} colab_type="code" id="Fhl2Kwb5b75C" slideshow={"slide_type": "subslide"}
from sklearn.feature_extraction.text import CountVectorizer

famous_book_openings = [
    "The story so far: in the beginning, the universe was created. This has made a lot of people very angry and been widely regarded as a bad move",
    "Whether I shall turn out to be the hero of my own life, or whether that station will be held by anybody else, these pages must show."
]

vec = CountVectorizer()
vec.fit(famous_book_openings) # This determines the vocabulary.
tf_sparse = vec.transform(famous_book_openings)
tf_sparse
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Printing in a readable format
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
import pandas as pd

pd.DataFrame(
    tf_sparse.todense(),
    columns=vec.get_feature_names()
)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Applying this process to our twitter data
We will do the following:
1. Load the tweets into a dataframe
2. Convert those tweets into a term-frequency matrix using the code from above
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Loading tweets into a dataframe
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
```

```python slideshow={"slide_type": "subslide"}
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([ \
    StructField("ItemID", IntegerType(), True), \
    StructField("Sentiment", IntegerType(), True), \
    StructField("SentimentText",StringType(),True)
  ])

spark_df = spark.read.schema(schema).csv(f'{home}/csc-369-student/data/twitter_sentiment_analysis/historical/xa*')
spark_df.first()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Convert to Pandas DataFrame for sklearn
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
historical_training_data = spark_df.toPandas()
historical_training_data.head()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Convert to a term frequency matrix
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
vec = CountVectorizer()
vec.fit(historical_training_data['SentimentText']) # This determines the vocabulary.
tf_sparse = vec.transform(historical_training_data['SentimentText'])
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Mathematical model for prediction
* We will use a multinomial Bayes classifier. 
* It is a statistical classifier that has good baseline performance for text analysis. 
* It's a classifier that you can update as new data arrives (i.e., online learning)
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from sklearn.naive_bayes import MultinomialNB
model = MultinomialNB()
model.fit(tf_sparse,historical_training_data['Sentiment'])
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**How well does this model predict on historical data?**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
test_spark_df = spark.read.schema(schema).csv(f'{home}/csc-369-student/data/twitter_sentiment_analysis/historical/xb*')
historical_test_data = test_spark_df.toPandas()
test_tf_sparse = vec.transform(historical_test_data['SentimentText'])
print("Accuracy on new historical test data:",sum(model.predict(test_tf_sparse) == historical_test_data['Sentiment'])/len(historical_test_data))
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**We've got a predictive model that does better than guessing!**

That's enough for this illustrative example. Now how would we update this using Spark Streaming?
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

<!-- #region slideshow={"slide_type": "subslide"} -->
### Create a directory where we can add tweets
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
data_dir="/tmp/tweets"
!rm -rf {data_dir}
!mkdir {data_dir}
!chmod 777 {data_dir}
!ls {data_dir}
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Code from 7.2

```python
data_dir = "/tmp/add_books_here"


from pyspark.sql import SparkSession
from pyspark.sql import Row
import traceback

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

ssc = StreamingContext(sc, 1)
ssc.checkpoint("checkpoint")

lines = ssc.textFileStream(data_dir)

def process(time, rdd):
    print("========= %s =========" % str(time))
    if rdd.isEmpty():
        return
    # Get the singleton instance of SparkSession
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        # Convert RDD[String] to RDD[Row] to DataFrame
        words = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: word)
        rowRdd = words.map(lambda w: Row(word=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
        print(wordCountsDataFrame.show())
    except Exception:
        print(traceback.format_exc())

lines.foreachRDD(process)

ssc.start()
import time; time.sleep(30)
#ssc.awaitTerminationOrTimeout(60) # wait 60 seconds
ssc.stop(stopSparkContext=False)
```
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### How would you modify this so it updates the model via Spark Streaming?
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
# Your solution here

ssc.start()
import time; time.sleep(10)
ssc.stop(stopSparkContext=False)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Help our algorithm by copying some of the data files in the directory!**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
!ls {home}/csc-369-student/data/twitter_sentiment_analysis/streaming/
```

```python slideshow={"slide_type": "fragment"}
!echo cp \~/csc-369-student/data/twitter_sentiment_analysis/streaming/xca {data_dir}
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**When we are ready we can check the accuracy again. In theory, we should get better with more data.**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
print("Accuracy on new historical test data:",sum(model.predict(test_tf_sparse) == historical_test_data['Sentiment'])/len(historical_test_data))
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Thank you! Don't forget to push.
<!-- #endregion -->

```python

```
