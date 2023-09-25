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
# Chapter 8.1 - Why NoSQL 

## MongoDB

Paul E. Anderson
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Ice Breaker

Detroit style pizza versus new york versus chicago?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
## Overview
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## NoSQL
> NoSQL databases (aka "not only SQL") are non tabular, and store data differently than relational tables. NoSQL databases come in a variety of types based on their data model. The main types are document, key-value, wide-column, and graph. They provide flexible schemas and scale easily with large amounts of data and high user loads. Source: https://www.mongodb.com/nosql-explained
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## What is NoSQL?
<img src="https://www.kdnuggets.com/wp-content/uploads/sql-nosql-dbs.jpg" width=700>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Pros and Cons
<img src="https://www.clariontech.com/hs-fs/hubfs/SQL-NOSQL.png?width=813&name=SQL-NOSQL.png" width=700>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### What performance considerations? Why is this desired in some applications?
<img src="https://www.guru99.com/images/1/101818_0537_NoSQLTutori2.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://miro.medium.com/max/5418/1*73e3UUYS_SsBYZfLvdOcfQ.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## What is a key-value database?
First point, a key-value is a map:
<img src="https://www.onlinemath4all.com/images/identifyingfunctionsfrommapping3.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Put another way:
<img src="https://www.educative.io/api/edpresso/shot/6707099755085824/image/5783885981941760">
<!-- #endregion -->

## CAP Theorem

Consistency - all nodes should see the same data at the same time

Availability - node failures do not prevent surviving nodes from continuing to operate

Partition tolerance - the system continues to function despite arbitrary message loss


When a network partition failure happens, it must be decided whether to
* cancel the operation and thus decrease the availability but ensure consistency or to
* proceed with the operation and thus provide availability but risk inconsistency.

Thus, if there is a network partition, one has to choose between consistency and availability. 

Eric Brewer argues that the often-used "two out of three" concept can be somewhat misleading because system designers only need to sacrifice consistency or availability in the presence of partitions, but that in many systems partitions are rare.

In distributed systems, there is a real chance of networking failures, so partition tolerance has to be accepted (i.e., there is a network partition), so now we have to choose between consitency and availability.


Note that consistency as defined in the CAP theorem is quite different from the consistency guaranteed in ACID database transactions.

ACID addresses an individual node's data consistency

CAP addresses cluster-wide data consistency

<!-- #region slideshow={"slide_type": "subslide"} -->
## What are the tradeoffs?
<img src="https://cdn.educba.com/academy/wp-content/uploads/2020/01/CAP-Theorem-last.jpg">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### What is partition tolerance?
> A partition is a communications break within a distributed system—a lost or temporarily delayed connection between two nodes. Partition tolerance means that the cluster must continue to work despite any number of communication breakdowns between nodes in the system.

Source: https://www.ibm.com/cloud/learn/cap-theorem#:~:text=request%2C%20without%20exception.-,Partition%20tolerance,between%20nodes%20in%20the%20system.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## What is distributed for performance gains?
<img src="https://i2.wp.com/www.kenwalger.com/blog/wp-content/uploads/2017/06/ShardingExample.png?resize=600%2C366">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://devops.com/wp-content/uploads/2017/02/cap-theorem.jpg" width=600>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
## Don't forget about the 8 fallacies...
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="http://www.mypassionfor.net/wp-content/uploads/2020/03/8-fallacies-of-distributed-computing-1024x714.png">
<!-- #endregion -->

## Introduction to pymongo

We will be using pymongo for our mongo labs. This lab focuses on queries and aggregation in MongoDB.

I have inserted ``data/daily.json`` into the database in the collection called ``daily`` in a database called ``csc-369``. You may gain access to it using the following commands:

```python
from pymongo import MongoClient
client = MongoClient()
```

```python
db = client["csc-369"]

col = db["daily"]
```

You can take a look at one of the records using

```python
import pprint
pprint.pprint(col.find_one())
```

## Information about the data
The collection contains information about COVID-19
infections in the United States. The data comes from the COVID Tracking
Project web site, specifically, from this URL:

https://covidtracking.com/api

We will be using the JSON version of the daily US States data, available
directly at this endpoint:

https://covidtracking.com/api/states/daily

For the sake of reproducibility, we will be using a data file Dr. Dekhtyar downloaded
on April 5, that includes all available data from the beginning of the tracking (March 3, 2020) through April 5, 2020. 

The data file is available for download from the course web site.
The COVID Tracking project Website describes the format of each JSON
object in the collection as follows:
* state - State or territory postal code abbreviation.
* positive - Total cumulative positive test results.
* positiveIncrease - Increase from the day before.
* negative - Total cumulative negative test results.
* negativeIncrease - Increase from the day before.
* pending - Tests that have been submitted to a lab but no results have
been reported yet.
* totalTestResults - Calculated value (positive + negative) of total test
results.
* totalTestResultsIncrease - Increase from the day before.
* hospitalized - Total cumulative number of people hospitalized.
* hospitalizedIncrease - Increase from the day before.
* death - Total cumulative number of people that have died.
* deathIncrease - Increase from the day before.
* dateChecked - ISO 8601 date of the time we saved visited their website
* total - DEPRECATED Will be removed in the future. (positive + negative + pending). Pending has been an unstable value and should not count in any totals.

In addition to these attributes, the JSON objects will contain the following
attributes (explained elsewhere in the API documentation):
* date - date for which the data is provided in the YYYYMMDD format
(note: JSON treats this value as a number - make sure you parse
correctly).
* fips - Federal Information Processing Standard state code
* hash - the hash code of the record
* hospitalizedCurrently - number of people currently hospitalized
* hospitalizedCumulative - appears to be the new name for the hospitalized attribute
* inIcuCurrently - number of people currently in the ICU
* inIcuCumulative - total cumulative number of people who required ICU hospitalization
* onVentilatorCurrently - number of people currently on the ventilator
* onVentilatorCumulative - total cumulative number of people who at some point were on ventilator
* recovered - total cumulative number of people who recovered from COVID-19

Note: ”DEPRECATED” attribute means an attribute that can be found
in some of the earlier JSON records, that that is not found in the most
recent ones.


**Stop and think:** What is the below function doing? How is this different or the same from SQL?

```python
col.find_one({'date':20200401,'state':'CA'})
```

<!-- #region slideshow={"slide_type": "subslide"} -->
## Wrap-up
We have introduced NoSQL databases and discussed their overall features, pros, cons, design considerations, and functionality.
<!-- #endregion -->
