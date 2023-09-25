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
# Chapter 4 - When old models of computing fail

## Hadoop and Spark

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

What is the best dessert in SLO?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>). Because of the limited time, you can use my server.
<!-- #endregion -->

## What's wrong with our language extensions (Ray and Dask)?
They are tied to a specific language, and designed for a specific purpose. They are not intended for every distributed computing task. Well. Nothing is created to cover every use case, but many engineers and data scientists would consider Ray and Dask not sufficient for enterprise big data analytics needs. 

<!-- #region slideshow={"slide_type": "slide"} -->
## What's wrong with our previous approaches?
* Apache Hadoop is a framework for distributed computation and storage of very large data sets on computer clusters
* Hadoop began as a project to implement Google’s MapReduce programming model
* Hadoop has seen widespread adoption by many companies including Facebook, Yahoo!, Adobe, Cisco, eBay, Netflix, and Datadog
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Consider our needs:
* We want a programming model that can process data many times the size that can be processed by a single computer
* This level of coordination and abstraction can be difficult to achieve for a general purpose computing language such a Python. 
* Hadoop brings a paradigm shift in the following sense:
    * Simplifying our programming constructs (i.e., limit us to Map Reduce)
    * Providing the architecture to support the computing we need
<!-- #endregion -->

## Hadoop Ecosystem
Software suite that provides various services to solve the big data problems.

Four major elements of Hadoop:
* HDFS
* MapReduce
* YARN
* Hadoop Common


<img src="https://media.geeksforgeeks.org/wp-content/cdn-uploads/HadoopEcosystem-min.png">


## History of Hadoop
Core software co-founders are Doug Cutting and Mike Cafarella.

The name Hadoop is from Doug Cutting son’s toy elephant.

Their work was based on two papers: The first paper on the Google File System was published in 2003.  The second paper published in 2004 described Google's implementation of MapReduce at scale (map/reduce is a programming concept that predated Google's paper).

In January 2006, MapReduce development started and consisted of around 6000 lines of code and around 5000 lines of code for HDFS.

In April 2006 Hadoop 0.1.0 was released. 


### The shortest history of the hype and reality

Top tier companies could afford the significant high end skill aquisition to make Hadoop and analytics beneficial. Mid-tier companies had more of a challenge. With talent in short supply, Hadoop data lakes became “data swamps” full of stale and unused data. These data lake were typically an on-premise deployment. As companies migrated their assets to the cloud, they typically found alternatives to both the Hadoop storage layer (HDFS) and the Hadoop processing engine.


The architecture of Hadoop remains important for distributed systems, and the Hadoop ecosystem is still used across the world today.

<!-- #region slideshow={"slide_type": "subslide"} -->
### Hadoop Distributed File System
* underlying file system of a Hadoop cluster
* designed with hardware failure in mind
* built for large datasets, with a default block size of 128 MB
* optimized for sequential operations
* rack-aware
* cross-platform and supports heterogeneous clusters
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
* Data in a Hadoop cluster is broken down into smaller units (called blocks) 
* Blocks are distributed throughout the cluster
* Each block is duplicated twice (for a total of three copies) 
* Two replicas stored on two nodes in a rack somewhere else in the cluster
* Highly available and fault-tolerant
* HDFS will automatically re-replicate it elsewhere in the cluster
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://imgix.datadoghq.com/img/blog/hadoop-architecture-overview/hadoop-architecture-diagram3.png?auto=format&fit=max&w=847">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### NameNode
* this is our leader
* brokers access to files by clients
* operates entirely in memory
* persisting its state to disk
* One single point of failure for a Hadoop cluster (though can be configured to have replica)

Clients communicate directly with DataNodes
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## MapReduce
* Perfect for distributed computing of large data
* three operations: 
    * map an input data set into a collection of <key,value> pairs
    * shuffle the resulting data (transfer data to the reducers)
    * then reduce over all pairs with the same key
<!-- #endregion -->

Map:
* performs sorting and filtering of data and thereby organizing them in the form of group 
* generates a key-value pair based result which is later on processed by the reduce method

Reduce:
* summarization by aggregating the mapped data
* takes the output generated by map as input and combines those tuples into smaller set of tuples

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://imgix.datadoghq.com/img/blog/hadoop-architecture-overview/hadoop-architecture-diagram1-3.png?auto=format&fit=max&w=847" width=2000>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### YARN (Yet Another Resource Negotiator)
Consists of three components
* ResourceManager (one per cluster)
* ApplicationMaster (one per application)
* NodeManagers (one per node)
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://imgix.datadoghq.com/img/blog/hadoop-architecture-overview/hadoop-architecture-diagram8.png?auto=format&fit=max&w=847">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### ResourceManager
* Rack-aware leader node in YARN. 
* Takes inventory of available resources
* Runs Scheduler.


<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### ApplicationMaster
* Each application running on Hadoop has its own dedicated ApplicationMaster instance. 
* Each application’s ApplicationMaster periodically sends heartbeat messages to the ResourceManager, as well as requests for additional resources, if needed. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Yarn Flow
1. Client program submits the MapReduce application to the ResourceManager, along with information to launch the application-specific ApplicationMaster.
2. ResourceManager negotiates a container for the ApplicationMaster and launches the ApplicationMaster.
3. ApplicationMaster boots and registers with the ResourceManager, allowing the original calling client to interface directly with the ApplicationMaster.
4. ApplicationMaster negotiates resources (resource containers) for client application.
5. ApplicationMaster gives the container launch specification to the NodeManager, which launches a container for the application.
6. During execution, client polls ApplicationMaster for application status and progress.
7. Upon completion, ApplicationMaster deregisters with the ResourceManager and shuts down, returning its containers to the resource pool.

<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Let's code some Hadoop

Well. Don't forget that cloud native ecosystems are on the rise now. Even Cloudera which was synonymous with Hadoop years ago has broken away from HDFS and their recent offerings are now new solutions architected from scratch.

We won't cover all of the emerging and established technologies. We will focus on one that was inspired by Hadoop, and definitely is on the rise and is well established.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Introduction to Spark
* Built around speed (in-memory enabled)
* ETL (extract, transform, load)
* Interactive queries (SQL)
* Advanced analytics (e.g., machine learning)
* Streaming over large datasets in a wide range of data stores (e.g., HDFS, Cassandra, HBase, S3)
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### RDDs
* Resilient Distributed Dataset (RDD)
* Fault tolerant like hadoop
* Allows parallel operations upon itself
* RDDs can be created from Hadoop InputFormats (such as HDFS files) OR
* by transforming other RDDs OR
* by loading text files OR
* by parallizing a list of data
* and more...
<!-- #endregion -->

There are other important portions of Spark that we will not cover in this chapter. They include Spark SQL, MLLib, Streaming, and GraphX.

<!-- #region slideshow={"slide_type": "subslide"} -->
### Spark SQL
* Designed for processing structured and semi-structured data
* Provides a DataFrame API for data manipulations. 
* DataFrame is conceptually similar to a table in relational database.
* Represents a distributed collection of data organized into named columns. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### MLLib
<a href="https://spark.apache.org/mllib/">https://spark.apache.org/mllib/</a>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Streaming
<a href="https://spark.apache.org/docs/latest/streaming-programming-guide.html">https://spark.apache.org/docs/latest/streaming-programming-guide.html</a>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### GraphX
<a href="https://spark.apache.org/graphx/">https://spark.apache.org/graphx/</a>
<!-- #endregion -->

## Questions


### 1. What are the main responsibilities of the NameNode?


#### Your solution here


### 2. In general terms (i.e., don't get into Spark and language details), what is the output of the map stage?


#### Your solution here


### 3. Has Spark replaced Hadoop? Should it replace Hadoop?


#### Your solution here


## Spark and MapReduce


We've seen with Ray that you call ray.init() to connect to a Ray cluster. Spark works in a similar manner, and so does Dask. I have pyspark ready for you on the server, and we need to either connect to an existing Spark master node. Below we will connect to a local master and retrieve a spark context. This is our way to run and communicate with Spark. 

```python
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

For our first example, we will load data manually into a Spark RDD and then use the collect function to return each element in the rdd one at a time.

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

What if you wanted to perform a transformation on each element, such as making the string lowercase?

```python
for element in rdd.map(lambda x: x.lower()).collect():
    print(element)
```

**Stop and think:** Why would we want to go to any of this trouble?


For starters, we can easily apply operations to extremely large datasets using this fundamental pattern.


We will now split the data up using the space separator.

```python
for element in rdd.map(lambda x: x.lower().split(" ")).collect():
    print(element)
```

What if you wanted to flatten out the individual arrays into a single array? We switch to ``flatMap``.

```python
for element in rdd.flatMap(lambda x: x.lower().split(" ")).collect():
    print(element)
```

A big change is that each word is now its own element. This will be imporant for our first exercise.


**Exercise 1:** Finish the Python code below and use ``flatMap``, ``map``, and a new function ``reduceByKey`` that applies a function to all elements with the same key.

```python
counts = rdd.flatMap(lambda x: x.split(' '))
# Your solution here
output = counts.collect()
output
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Conclusion
* Hadoop and Spark make up two pillars of the modern data engineering ecosystem. 
* We won't need to implement these (already done). We do need to understand them and use them.
* We will work our way through these technologies.
<!-- #endregion -->

## References:
* Source: https://www.datadoghq.com/blog/hadoop-architecture-overview/
