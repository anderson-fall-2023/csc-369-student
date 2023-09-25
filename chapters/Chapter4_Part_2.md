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
# Chapter 4 - Part 2

Paul E. Anderson
<!-- #endregion -->

```python
%load_ext autoreload
%autoreload 2

from pathlib import Path
home = str(Path.home()) # all other paths are relative to this path. 

import pandas as pd
```

```python
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

Creating an RDD from a Python object is useful, but most of our data will be in files or databases. Spark provides a few functions to read files:

* ``sc.textFile(path)``
* ``sc.wholeTextFiles(path)``


``textFile`` takes a string argument that is the path to the file or files to be read. Both of these can read multiple files into an RDD/PairRDD.

For example:

* ``sc.textFile(path)`` - returns an RDD with each line as an element
* ``sc.wholeTextFiles(path)`` - returns a PairRDD object where the key is file path and the value is the contents of the file.


``textFile`` will split the data into chuncks of 32MB. 

This is advantagous from a memory perspective, but the ordering of the lines is lost, if the order should be preserved then wholeTextFiles should be used.

``wholeTextFiles`` will read the complete content of a file at once. Each file will be handled by one core and the data for each file will be one a single machine making it harder to distribute the load.

```python
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=sc.parallelize(data) # distributes data
for element in rdd.collect():
    print(element)
```

**Exercise 2:** Create a function that returns the word frequency of each word in an RDD.

```python
def word_freq(rdd):
    output = []
    # Your solution here
    return output
```

```python
word_freq(rdd)
```

**Exercise 3:** 

Write a function that reads all of the books into a single RDD. Call this function ``load_rdd_all_books``.

```python
def load_rdd_all_books(sc,dir):
    lines = None
    return lines
```

```python
all_books_rdd = load_rdd_all_books(sc,f"file:{home}/csc-369-student/data/gutenberg")
all_books_rdd
```

**Problem 1:** Apply your word frequency function to the all_books_rdd.

```python
output = word_freq(all_books_rdd)
# Your solution here
output[:10]
```

```python
# We can use pandas to print it in a readable way
pd.DataFrame(output,columns=['Word','Frequency']).sort_values(by="Frequency")
```

**Exercise 4:** Use ``wholeTextFiles`` and the ``map`` function to return the word counts for each book individually. Call this function book_word_counts. Do not use your previous function word_freq as that will not work in this case without modifications.

```python
# This is a helper function you can use

def count_words(content):
    counts = {}
    for line in content.split("\n"):
        words = line.split(" ")
        for word in words:
            if word not in counts:
                counts[word] = 0
            counts[word] += 1
    return counts
```

```python
def book_word_counts(sc,dir):
    res = None
    return res
```

```python tags=[] jupyter={"outputs_hidden": true}
res = book_word_counts(sc,f"file:{home}/csc-369-student/data/gutenberg")
res[0] # One book
```

```python
# print this in a pretty way
pd.DataFrame(res,columns=['File','Word Frequency'])
```
