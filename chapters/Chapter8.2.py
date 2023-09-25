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
# # Chapter 8.2 - MongoDB 
#
# Paul E. Anderson

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Ice Breaker
#
# Best burget in town?

# + [markdown] slideshow={"slide_type": "slide"}
# ## Reference Guide of Commands
#
# https://docs.mongodb.com/manual/reference/command/
#
# I don't think it is particularly useful to lecture about commands that you would ultimately look up regardless. Instead, the above link is very useful to a quick lookup of different commands and the arguments they take. We will learn about MongoDB by using it to solve a few problems.

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2


import pandas as pd
import numpy as np

# + slideshow={"slide_type": "skip"}
from pymongo import MongoClient
client = MongoClient()

db = client["csc-369"]

col = db["daily"]
# -

# **Exercise 1:** Use find_one to find a record with an object ID equal to 60392e3656264fee961ca817. As always, put your solution in Lab6_helper.

# +
from bson.objectid import ObjectId

def exercise_1(col,record_id):
    result = None
    # Your solution here
    return result

record = exercise_1(col,'60392e3656264fee961ca817')
record


# -

# **Exercise 2:** Use count_documents to count the number of records/documents that have ``state`` equal to 'CA'. 

# +
def exercise_2(col,state):
    result = None
    # Your solution here
    return result

record = exercise_2(col,'CA')
record


# -

# **Exercise 3:** Write a function that returns all of the documents that have a date less than ``d``. Sort the documents by the date, and convert the result to a list.

# + tags=[]
def exercise_3(col,d):
    result = None
    # partial solution
    # airline_delay2.groupBy(?).pivot(?).agg(avg(?))
    # Your solution here
    return result

d = 20200315 # YYYY-MM-DD
record = exercise_3(col,d)
record[:3]
# -

# **Exercise 4:** Write a function that returns the total number of positive cases and the number of new cases
# in New York state on April 1.

# +
from bson.code import Code

def exercise_4(col):
    result = None
    # Your solution here
    return result

record = exercise_4(col)
record


# -

# **Exercise 5:** Write a function that returns how many deaths were in the state of New Jersey on the earliest day when the total cumulative number of deaths exceeded 500 (i.e., ``death`` column).
#
# > .sort(), in pymongo, takes key and direction as parameters.
# > So if you want to sort by, let's say, id then you should .sort("_id", 1)

# +
def exercise_5(col):
    result = None
    # Your solution here
    return result

record = exercise_5(col)
record


# -

# **Exercise 6:** Write a function using ``aggregate``. The function reports the count and the cumulative increase in positive cases (when there were positive cases) within the date range (inclusive). Do not include missing days or values (i.e., positive cases > 0). I used \$match, \$group, and \$and within aggregate. The columns I used are date, state, and positiveIncrease.
#
# <a href="https://docs.mongodb.com/manual/aggregation/#std-label-aggregation-framework">Documentation</a>

# +
def process_exercise_6(result):
    process_result = {}
    for record in result:
        process_result[record['_id']['state']] = record['sum']/record['count']
    return process_result

def exercise_6(col,date1,date2):
    result = None
    # partial solution
    # Your solution here
    return result

result = list(exercise_6(col,20200401,20200402))
import pprint
pprint.pprint((result))

record = process_exercise_6(result)
record
# -

record['AZ'],record['AL']


# **Exercise 7:** Repeat exercise 6, but instead of using aggregate you must use map-reduce.

# +
def process_exercise_7(result):
    process_result = {}
    for record in result:
        state,identifier = record['_id'].split(": ")
        value = record['value']
        if state not in process_result:
            process_result[state] = 1.
        if identifier == "sum":
            process_result[state] *= value
        elif identifier == "count":
            process_result[state] *= 1/value
    return process_result

def exercise_7(col,date1,date2):
    result = None
    # partial solution
    # Your solution here
    return result

result = list(exercise_7(col,20200401,20200402).find())
import pprint
pprint.pprint((result))

record = process_exercise_7(result)
record
# -

record['AZ'],record['AL']


