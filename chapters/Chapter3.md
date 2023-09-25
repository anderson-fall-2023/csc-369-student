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
# Chapter 3 - Why isn’t distributed computing built into my favorite language? 

## Language extensions for distributed computing

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

If you were stranded on a desert island, what two items do you bring with you?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>. Because of the limited time, you can use my server.
<!-- #endregion -->

```python
%load_ext autoreload
%autoreload 2
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Motivation: What about programming Languages?
For the sake of the discussion, let's focus on imperative programming languages such as Python and Java. These programming languages provide a way to algorithmicly specify your program. We're comfortable and familiar with this paradigm. There are other approaches of course. Functional and logic programming for example. Most of our languages are designed for software engineers in mind. I'm oversimplying the world here, but our main programming languages are used to build software. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Consider the Python programming language: Python have multiprocessor libraries (https://docs.python.org/2/library/multiprocessing.html). So why isn't that good enough? Among other things, this library does not efficiently solve these common problems of distributed computing:
* Running the same code on more than one machine.
* Building microservices and actors that have state and can communicate.
* Gracefully handling machine failures.
* Efficiently handling large objects and numerical data.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
There are languages such as Julia that are gaining popularity. These languages provide better support for **distributed computing at a lower level of abstraction** than we need in this course. Still there are arguments for their adoption. We will not unpack those arguments at the moment. We are building towards domain specific tools such as:
* TensorFlow for neural network training
* Spark for data processing and SQL
* Flink for stream processing
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
These provide **higher level abstractions** for neural networks, datasets, and streams. One of the biggest hurtles for their adoption is that these often require rewriting a lot of our code and our thinking about how to compose solutions. This leads to an obvious question:
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "fragment"} -->
What about something in between? **Can't we just improve our favorite languages?**

We will consider two popular distributed computing platforms for Python: Ray and Dask.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Python Library 1: Ray
Ray is a Python library that attempts to translate the traditional ideas of functions and classes to distributed settings such as **tasks** and **actors**. We've already seen the idea of tasks with our GNU Parallel work. Ray is designed to take similar ideas and bring them into Python as easily as possible.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Starting Ray
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
import ray
ray.init()
```

<!-- #region slideshow={"slide_type": "fragment"} -->
If you were connecting to a cluster, then you would pass additional arguments with connection specifics.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
init() does the following:
* Starts a number of worker processes for executing Python calls in parallel (approximately one per core in our environment)
* Starts a scheduler process for assigning tasks to workers
* Starts a shared memory object store for sharing objects without making copies
* An in-memory database for storing metadata needed to rerun tasks in the even of failure
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
A **task** is the unit of work schedule by Ray and corresponds to a function call.

Ray workers are separate processes as opposed to threads
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Simple parallel example
All we need is a little decoration

**Detour:** <a href="https://realpython.com/primer-on-python-decorators/">For more information on decorators in Python</a>
<!-- #endregion -->

```python slideshow={"slide_type": "skip"}
import time
```

```python slideshow={"slide_type": "fragment"}
@ray.remote
def f(x):
    time.sleep(1)
    return x
```

<!-- #region slideshow={"slide_type": "subslide"} -->
A common pattern when performing distributed computing is that we get a handle to the eventual output immediate. This is because we don't want our main program to stop and wait unless we explicitly need this to be the case. In most settings we are setting up the distributed computing, but we need to decide ourselves how to grab the data. In Ray, we get back futures (https://en.wikipedia.org/wiki/Futures_and_promises).
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Start 4 tasks in parallel.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
result_ids = []
for i in range(4):
    result_ids.append(f.remote(i))
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Wait for the tasks to complete and retrieve the results.
**With at least 4 cores, this will take 1 second.**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
results = ray.get(result_ids)  # [0, 1, 2, 3]
results
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Class exercise
Using Ray, create a simple parallel search for Prime numbers. Take 5-10 minutes to design and test your solution. We'll run each solution for 30 seconds to see who's solution finds the most prime numbers. Here is naive code to test for prime numbers:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2,n):
        if n % i == 0:
            return False
    return True
```

```python slideshow={"slide_type": "fragment"}
is_prime(6),is_prime(7)
```

```python slideshow={"slide_type": "subslide"}
import signal

class TimeoutException(Exception):   # Custom exception class
    pass

def timeout_handler(signum, frame):   # Custom signal handler
    raise TimeoutException
    
# Your solution/code here!
```

```python slideshow={"slide_type": "subslide"}
result_ids = [find.remote(3)]
result_ids
```

```python slideshow={"slide_type": "subslide"}
import itertools
import numpy as np
all_results = set(itertools.chain(*ray.get(result_ids)))
```

```python slideshow={"slide_type": "fragment"}
max(all_results)
```

```python slideshow={"slide_type": "fragment"}
len(all_results)
```

```python slideshow={"slide_type": "subslide"}
result_ids = [find.remote(3),find.remote(19428)]
result_ids
```

```python slideshow={"slide_type": "subslide"}
import itertools
import numpy as np
all_results = set(itertools.chain(*ray.get(result_ids)))
```

```python slideshow={"slide_type": "fragment"}
max(all_results)
```

```python slideshow={"slide_type": "fragment"}
len(all_results)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Details (and order) make a big difference
Consider the problem of adding a large set of integers together. If we structure our aggregation incorrectly, we don't see the benefits of our distributed environment. 
<img src="https://miro.medium.com/max/1400/1*vHz3troEmr4uLns0V8VmdA.jpeg">
* Both executions result in the same, but the one on the right shows how to turn linear into logarithmic!
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Slow approach in action**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
@ray.remote
def add(x, y):
    time.sleep(1)
    return x + y

values = [1, 2, 3, 4, 5, 6, 7, 8]
while len(values) > 1:
    #import pdb; pdb.set_trace()
    values = [add.remote(values[0], values[1])] + values[2:]
result = ray.get(values[0])
result
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**STOP and THINK:** This code produces the execution on the left (figure above). How would you change it to work like the execution on the right?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Fast approach in action**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
values = [1, 2, 3, 4, 5, 6, 7, 8]
while len(values) > 1:
    values = values[2:] + [add.remote(values[0], values[1])] # remove 2 and put at end
result = ray.get(values[0])
result
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Python Library 2: Dask (and Pandas)
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
One of the most widely used structured data processing tools available is a library in Python known as Pandas. We won't cover all of Pandas or Dask in this chapter. We will cover some very common use cases of the two. Hopefully, enough to know **when Pandas+Dask might be appropriate for your problem.**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Pandas in 10 minutes
There are many resources out there for this purpose. I am being inspired by: https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
import pandas as pd
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**pd.Series**
* a list with more power
* elements have an integer 0-based index and a named index
* a series has a name
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
s1 = pd.Series(["A","B","C",3.14],index=["i","ii","iii","iv"],name="first_example")
s1
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**You can access in either a 0-based index way (iloc) or a named index manner (loc)**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
s1.loc["ii"],s1.iloc[1]
```

<!-- #region slideshow={"slide_type": "fragment"} -->
But a lot of other things still work as expected
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
len(s1),s1.shape
```

<!-- #region slideshow={"slide_type": "subslide"} -->
So the main concept (at this moment) is that Pandas provides extended functionality for common things such as lists and dictionaries. We'll see this in more detail as we examine **pd.DataFrame** by loading COVID19 data.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
> The data set contains daily reports of Covid-19 cases and deaths in countries worldwide. The data also shows the country’s population and the number of cases per 100,000 people on a rolling 14 day average.

https://corgis-edu.github.io/corgis/python/covid/
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
import sys
sys.path.insert(0,f'{home}/csc-369-student/data/covid')
import covid
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Let's see how it looks**
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
report = covid.get_report()
report
```

<!-- #region slideshow={"slide_type": "subslide"} -->
That's not great. It's a dictionary that is very machine readable, but not very useful for us to easily ask questions about. Let's throw it into a DataFrame and see what happens.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
report_df = pd.DataFrame(report)
report_df
```

<!-- #region slideshow={"slide_type": "subslide"} -->
That is not bad, but there are still dictionaries inside each cell of this DataFrame. 
* A DataFrame is a table-like data structure. 
* A DataFrame has column names (e.g., report_df.columns) and can be indexed using iloc or loc.
* You can also access columns by name (e.g., report_df['Date'])
Let's try to process one of those columns.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
report_df['Date'] # By name
```

```python slideshow={"slide_type": "subslide"}
type(report_df['Date'])
```

<!-- #region slideshow={"slide_type": "fragment"} -->
So now we can say a DataFrame is made up of a collection of pd.Series objects
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Because each column is a Series that is made up of individual dictionaries, let's throw each column into a DataFrame and see what happens. Notice that I cast this to a generic List object.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
date_df = pd.DataFrame(list(report_df['Date']))
date_df
```

<!-- #region slideshow={"slide_type": "subslide"} -->
This looks much better. We can now see clear what data we have. (Trust me. We are getting to distributed computing). But let's process the ``data`` and ``location``.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
data_df = pd.DataFrame(list(report_df['Data']))
location_df = pd.DataFrame(list(report_df['Location']))
data_df
```

```python slideshow={"slide_type": "subslide"}
location_df
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Finally, we can join all of this together:
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
report_df2 = date_df.join(data_df).join(location_df)
report_df2
```

<!-- #region slideshow={"slide_type": "subslide"} -->
There is a lot more to show about Pandas and related data science tools, but let's consider the following:
* This sample dataset has 50,000+ rows. 
* How large could this dataset be if we approached collecting all of the data in the world? 
* Could that fit into your computer's memory?
* Even if it could fit into your computer's memory, processing it with pure Python would only make use of a single core unless you did some major coding to use the multiprocessing.

**And here comes the Dask library**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Dask DataFrame
* Implements a blocked parallel DataFrame object that mimics a large subset of the Pandas DataFrame API. 
* One Dask DataFrame is comprised of many in-memory pandas DataFrames separated along the index. 
* One operation on a Dask DataFrame triggers many pandas operations on the constituent pandas DataFrames 
* This is done to exploit potential parallelism and memory constraints
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from dask import dataframe as dd 
report_dd2 = dd.from_pandas(report_df2,npartitions=3)
report_dd2
```

```python slideshow={"slide_type": "subslide"}
report_dd2.head()
```

```python slideshow={"slide_type": "subslide"}
report_dd2.tail()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## What do you think the following returns and why?
Remember Ray
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
report_dd2['Rate'].max()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### And just like Ray, we can evaluate and get a result
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
report_dd2['Rate'].max().compute()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**So what is this doing behind the scenes?**

<img src="https://camo.githubusercontent.com/349ed6d3048da7d324ef6fa8b07f66f5072ad2eb/687474703a2f2f6461736b2e7079646174612e6f72672f656e2f6c61746573742f5f696d616765732f6461736b2d646174616672616d652e737667" width=400>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Let's assume our dataset was large enough that finding the mean took some time. Let's also assume we aren't 100% sure what we want to calculate. We need to be able to quickly experiment with a dataset that "can't" fit into memory. This is where Dask can really shine. Think of Dask if:
* You are already using Pandas
* Your data may or may not fit into memory
* You want to make better use of distributed computing (i.e., cores or processors)
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Example: Calculate the mean rate for each country**
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
report_dd2.groupby('Country')['Rate'].mean()
```

```python slideshow={"slide_type": "subslide"}
country_means = report_dd2.groupby('Country')['Rate'].mean().compute()
country_means_df = country_means.sort_values(ascending=False).to_frame()
country_means_df
```

<!-- #region slideshow={"slide_type": "subslide"} -->
I want to merge the continent back into the analysis
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
continent_group = report_dd2.groupby('Country')['Continent'].apply(lambda s: s.iloc[0], meta="").compute()
continent_group_df = continent_group.sort_values(ascending=False).to_frame()
continent_group_df.columns = ["Continent"]
continent_group_df
```

<!-- #region slideshow={"slide_type": "subslide"} -->
* ``apply`` is a function that will apply a function of our specification to each object. It's basically a for loop
* ``meta`` is a Dask parameter that is needed because we are running a custom function. This tells Dask we are planning on returning a string.
* ``lambda`` is a Python anonymous function.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Note: Both ``continent_group_df`` and ``continent_means_df`` have the same index, so we can join them, and then reset the index. The right mindset to get into in Pandas is to think like a database.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
plot_df = country_means_df.join(continent_group_df).reset_index()
plot_df
```

```python slideshow={"slide_type": "skip"}
import altair as alt
g=alt.Chart(plot_df).mark_bar().encode(
    y='Country',
    x='Rate',
    color='Continent',
    row='Continent'
)
```

```python slideshow={"slide_type": "subslide"}
g
```

<!-- #region slideshow={"slide_type": "subslide"} -->
That's a lot and basically a great big mess. What if we want to radically change the algorithm and calculation? This is where a good distributed framework can start to pay off.
* Running something one time in a serial manner is often ok.
* We almost never need to run something a single time. 
* More than not development is going to take 100's of iterations before we are happy with our results. 
* Strategy: prototype on small datasets, and use a flexible and easy to use distributed system
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Wrapping up
There is much more to Ray and Dask than we presented in this chapter. They are both flexible and wonderful additions to the Python language. You can get a long way with these two libraries without leaving the comfort of a language you know. But in both cases, you need to be aware you are executing your programs in a distributed manner and structure them accordingly or you will not see the benefits.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Questions

### 1. What is a decorator and what do we use them in Ray to accomplish?


### 2. Can you call a function that has a Ray decorator in the normal Python way?


### 3. Explain what futures are in Ray and why they are important?


### 4. How would you compare Ray and Dask?


### 5. Does Dask use futures? How do you compare them to Ray futures? How do you tell Dask to execute and produce a result?


**Thank you!**
<!-- #endregion -->

```python

```
