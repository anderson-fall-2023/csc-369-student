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

# + [markdown] slideshow={"slide_type": "slide"}
# # Lab 5 - Spark Lab 2
#
# ## Map/Reduce
#
# In this lab you will implement matrix multiplication using Spark with two different approaches.
# -

# ### The usual imports

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2


# Put all your solutions into Lab1_helper.py as this script which is autograded
import Lab5_helper
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
# -

# ### Set up your Spark context

# +
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 1:**
# Using the Spark functions (cartesian, map, collect), and the numpy function (np.dot or a loop of your own), compute the matrix multiplication of A_RDD and B_RDD.
# -

A1_RDD = sc.parallelize(Lab5_helper.A1)
B1_RDD = sc.parallelize(Lab5_helper.B1)

result = Lab5_helper.exercise_1(A1_RDD,B1_RDD)
result

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 2:** Implement matrix multiplication using the following alternative format:
#
# 'row number', 'column number', 'value'
#
# For this exercise, you cannot use loops or np.dot. It should be Spark centric using cartesian, join, map, add, reduceByKey, and/or collect. 
# -

A2_RDD = sc.parallelize(Lab5_helper.A2)
B2_RDD = sc.parallelize(Lab5_helper.B2)

result = Lab5_helper.exercise_2(A2_RDD,B2_RDD)
result

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 3:** Implement matrix multiplication using the following alternative format that assumes missing rows have a value of 0 (i.e., sparse matrices):
#
# 'row number', 'column number', 'value'
#
# For this exercise, you cannot use loops or np.dot. It should be Spark centric using join, map, add, reduceByKey, and/or collect. 
# -

A3_RDD = sc.parallelize(Lab5_helper.A3)
B3_RDD = sc.parallelize(Lab5_helper.B3)

result = Lab5_helper.exercise_3(A3_RDD,B3_RDD)
result

# + slideshow={"slide_type": "skip"} tags=[]
# Don't forget to push!
# -

