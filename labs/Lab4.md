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
# Lab 4 - Spark Lab 1

## Map/Reduce

In this lab, you will work through some of your first programs using the map/reduce paradigm. They are designed to get you to think in a map reduce frame of mind.
<!-- #endregion -->

### The usual imports

```python slideshow={"slide_type": "skip"}
%load_ext autoreload
%autoreload 2


# Put all your solutions into Lab1_helper.py as this script which is autograded
import Lab4_helper
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
```

### Set up your Spark context

```python
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

```python
rdd=sc.parallelize(Lab4_helper.data) # distributes data
for element in rdd.collect():
    print(element)
```

**Exercise 1:** Create a python function called ``word_counts``. You should use ``flatMap``, ``map``, and a ``reduceByKey``. Your function should take in an RDD.

```python
counts = Lab4_helper.word_counts(rdd)
counts
```

**Exercise 2:** Create a function that returns the word frequency of each word in an RDD.

```python
word_frequencies = Lab4_helper.word_freq(rdd)
word_frequencies
```

**Exercise 3:** 

Write a function that reads all of the books into a single RDD. Call this function ``load_rdd_all_books``.

```python
all_books_rdd = Lab4_helper.load_rdd_all_books(sc,f"file:{home}/csc-369-student/data/gutenberg")
all_books_rdd
```

```python
for element in all_books_rdd.collect()[:10]:
    print(element)
```

**Problem 1:** Apply your word frequency function to the all_books_rdd and check out the output.

```python
output = Lab4_helper.word_freq(all_books_rdd)
output[:10]
```

```python
# We can use pandas to print it in a readable way
pd.DataFrame(output,columns=['Word','Frequency']).sort_values(by="Frequency")
```

**Exercise 4:** Use ``wholeTextFiles`` and the ``map`` function to return the word counts for each book individually in an **RDD**. Call this function book_word_counts. Do not use your previous function word_freq as that will not work in this case without modifications.

```python tags=[]
res = Lab4_helper.book_word_counts(sc,f"file:{home}/csc-369-student/data/gutenberg")
res
```

```python tags=[]
# print this in a pretty way
pd.DataFrame(res,columns=['File','Word Frequency'])
```

**Exercise 5:** Create a new function called ``lower_case_word_freq``. This function takes as input the output of word_freq parallized into an RDD (see below). This new function converts all the keys to lowercase and then reduces the counts correctly. 

```python
output = Lab4_helper.word_freq(all_books_rdd)
output_lower = Lab4_helper.lower_case_word_freq(sc.parallelize(output))
output_lower[:10]
```

```python
pd.DataFrame(output_lower,columns=['Word','Frequency']).sort_values(by="Frequency")
```

```python slideshow={"slide_type": "skip"} tags=[]
# Don't forget to push!
```
```python

```
