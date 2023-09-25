# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,md,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.8.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false
# # Chapter 5 - What was wrong with Hadoop (take 2)?
#
# ## Spark RDDs and complex multi-pass algorithms
#
# Paul E. Anderson

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Ice Breaker
#
# Who is the most famous celebrity you've ever met or seen?

# + [markdown] slideshow={"slide_type": "subslide"}
# While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>. Because of the limited time, you can use my server.

# + [markdown] slideshow={"slide_type": "slide"}
# ## Hadoop's Greatness
# * Hadoop really shined for batch processing
# * It quickly leads to frustration once users need to do more such as complex, multipass algorithms, interactive processing and queires, and real-time stream processing
#
# So what did the community do?

# + [markdown] slideshow={"slide_type": "subslide"}
# We create more technology:
# * Pregel, Drill, Presto, Storm, Impala, ...
#
# These have a lot of advantages, but the more systems to tune, manage, and deploy the more the complexity increases. 
#
# But on top of the complexity, data exchange between technologies is the dominant cost in most applications.

# + [markdown] slideshow={"slide_type": "subslide"}
# So we need a generic and efficient infrastructure solution. We've talked about one popular alternative, Spark, which is now an Apache top level project.

# + [markdown] slideshow={"slide_type": "subslide"}
# ### But what's wrong with Hadoop?
# <img src="https://qph.fs.quoracdn.net/main-qimg-dddb2a8c5f004e3c1b981a10f62221df" width=800>

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Why is this possible now?
# * RAM is getting much cheaper
# * Commodity machines can have GBs of RAM (your own laptop!)

# + [markdown] slideshow={"slide_type": "slide"}
# ## Spark Architecture
# <img src="https://intellipaat.com/mediaFiles/2017/02/Spark-Arch.jpg">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Spark Context
# * Get the current status of spark application
# * Set configurations
# * Access various services
# * Cancel a job
# * Cancel a stage
# * Closure cleaning
# * Register Spark-Listener
# * Programmable Dynamic allocation
# * **Access persistent RDD**

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://abhishekbaranwal10.files.wordpress.com/2018/09/introduction-to-apache-spark-20-12-638.jpg">

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://image.slidesharecdn.com/6-150108024847-conversion-gate02/95/scala-and-spark-10-638.jpg?cb=1420685361" width=1000>

# + [markdown] slideshow={"slide_type": "slide"}
# ## Spark Programming Core API
# * High-level coding built in high level language (e.g., Scala, Python)
# * Spark is written in Scala making Scala the default language
# * Code is compiled to distributed parallel operations
# * Two main abstractions in the Core API
#     * RDDs: Resilient Distributed Datasets
#     * Parallel Operations

# + [markdown] slideshow={"slide_type": "subslide"}
# ### RDDs
# * Collection of objects that act as a unit
# * Stored in main memory or on disk
# * Parallel operations are built on top of them
# * **Have fault tolerance without replication**
# * **Fault tolerance is lineage based**

# + [markdown] slideshow={"slide_type": "subslide"}
# Two big but easy to blow by details:
# * RDDs are read-only
#     * Wait... what how can we do anything?
# * Distributed in main memory or disk
#     * Automatically decided

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://i1.wp.com/sparkbyexamples.com/wp-content/uploads/2019/11/rdd-transformation-work-count.png?resize=697%2C429&ssl=1" width=1000>

# + [markdown] slideshow={"slide_type": "subslide"}
# ## RDD Advantage
# Hadoop MapReduce was able to harness the computational power of a cluster but not distributed memory
#
# RDDs allow in-memory storage and transfer of data to disk

# + [markdown] slideshow={"slide_type": "subslide"}
# ### RDDs versus Distributed Shared Memory
# <img src="https://miro.medium.com/max/660/1*MiaofzEizdQ9LjWuKXDLrg.png">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### So how can we create RDDs?
# 1. Loading from external dataset (we've done this :)
# 2. Created from another RDD

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://avinash333.files.wordpress.com/2019/09/rdd5.png?w=960">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Operations on RDDs
# Transformation operations are similar to the map-side of Hadoop and **no** execution is triggered for these operations.
#
# Action operations such as ``collect()`` are similar to the reduce-side of Hadoop. When called execution is triggered. 
#
# This is a delayed execution model.

# + [markdown] slideshow={"slide_type": "subslide"}
# Examples of transformation operations to operate on one RDD and generate a new RDD:
# * map
# * filter
# * join
#
# Lazy evaluation
#
# Input RDD is not changed (remember read-only)

# + [markdown] slideshow={"slide_type": "subslide"} hideCode=false
# Examples of action operations that operate on existing RDDs producing a result:
# * count
# * collect
# * reduce
# * save
#
# Results are either returned to driver program or stored in file systems.

# + [markdown] slideshow={"slide_type": "subslide"}
# Some examples to look at: <a href="https://spark.apache.org/examples.html">https://spark.apache.org/examples.html</a>

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://miro.medium.com/max/1160/1*xGrIK4GU1PRZ49AMPTc-0w.png">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Lazy Evaluation
# * Results are not physically computed right away (remember futures)
# * Metadata from transformations is recorded
# * Transformations are only performed when an action is invoked

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Lineage graph
# <img src="https://2.bp.blogspot.com/-7z9pTkw0EBs/Wn4ocfDTmII/AAAAAAAAEHM/yobSB13veOwwoIR2loy0p2wm-bF6miEqgCLcBGAs/s1600/RDDLineage.JPG">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Spark Partitions
# <img src="https://miro.medium.com/max/1296/1*gDz_AuuB-q0ux9Pl9CrvHA.png">

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Narrow dependencies
# 1-1 relationship between child and parent partitions
#
# Examples: **filter and map**
#
# Relatively cheap to execute
#
# **No data movement is needed as input and output stays in same partition**

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Wide dependency
# M-1 or M-M between child-parent partitions
#
# Examples: **join and grouping**
#
# More expensive
#
# Input from more than one partition is required
#
# Data shuffling is needed

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Narrow versus wide dependencies
# <img src="https://miro.medium.com/max/1266/0*kAw8hogu1oZPy9QU.png">
# -

# ## Questions

# ### 1. What are some of the responsibilities of the spark context?
#
#
# #### Your solution here

# ### 2. How does Spark achieve fault tolerance?
#
#
# #### Your solution here

# ### 3. What is the difference between RDDs and distributed shared memory?
#
#
# #### Your solution here

# ### 4. What is lazy evaluation in Spark?
#
#
# #### Your solution here

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Wrap-up
# As is always the case, there is a lot more to the core Spark API. For example, we haven't discussed the scheduler in detail. But I believe we now have a better feel for what drives computations on Spark. We understand some of the differences between operations in Spark. And finally, we understand why Spark RDDs are preferred over Hadoop in many applications. 
