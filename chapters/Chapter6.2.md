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
# Chapter 6.2 - Spark SQL continued

For this section we will analyze a very large dataset from the Department of Transportation. I have already converted the data to the parquet format discussed in class, but if you don't believe me that it has benefits, let's check out some stats:
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Here is the original data as I downloaded it without any modification other than I unzipped it.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
!du -sh /disk/airline-data
```

<!-- #region slideshow={"slide_type": "fragment"} -->
Let's take a look at the data after I processed it.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
!du -sh /disk/airline-data-processed
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Well I don't know about you, but that seems amazing :) Here is the original compressed file size. It is important to realize that while the .tar.gz file is "small" at 4.7 GB, we can't access it with Spark or any other program without uncompressing it. But we can do that with the parquet files!
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
!du -sh /disk/airline-data.2003-2018.tar.gz
```

<!-- #region slideshow={"slide_type": "subslide"} -->
If you are curious how I did this, please check out Setup_Chapter6.ipynb. No need to run this or edit it or even look at it, but it's there.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
%load_ext autoreload
%autoreload 2
```

```python slideshow={"slide_type": "subslide"}
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext
```

<!-- #region slideshow={"slide_type": "slide"} -->
## Exploratory Data Analysis
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
# Our main data source
on_time_df = spark.read.parquet('file:///disk/airline-data-processed/airline-data.parquet')
on_time_df.show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
That is a bit brutal to look at... Consider examining like:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
on_time_df.columns
```

```python slideshow={"slide_type": "subslide"}
# The first row
on_time_df.first()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
What if you want to average AirTime?
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from pyspark.sql.functions import avg, col

on_time_df.select('AirTime').agg(
    avg(col('AirTime'))
).show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
So we need navigate a fine line where I don't throw the entire Spark SQL API at you, but there are some functions above that should be discussed. The first is select which you can use to get a subset of the columns. This is important for memory usage. Load only what you need :). The next few are agg which is short for aggregate. Then there is col which selects the column and then avg which of course is average. If you know sql, you can also rely on SQL to work the magic.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
on_time_df.select('AirTime').createOrReplaceTempView("AirTimeView") # create a temporary view so we can query our data

sqlDF = spark.sql("SELECT avg(AirTime) FROM AirTimeView").show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
I don't know about you, but since I already know SQL or at least some SQL, I'm very excited that I can use that. For this topic in general, please use what makes sense to you to accomplish the job.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
What if I wanted average air time per month?
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
on_time_df.select('AirTime','Month').createOrReplaceTempView("AirTimeView") # create a temporary view so we can query our data

sqlDF = spark.sql("SELECT Month, avg(AirTime) FROM AirTimeView group by Month").show()
```

```python slideshow={"slide_type": "subslide"}
on_time_df.select('AirTime','Month').groupBy(
    'Month'
).agg(
    avg(col('AirTime'))
).show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Pretty nice right? You can see why companies might really value engineers who can bring data processing skills with them.

Let's now read in some data that helps us map Carrier name to AirlineName.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
airlines = spark.read.parquet('file:///disk/airline-data/DOT_airline_codes_table')
```

```python slideshow={"slide_type": "fragment"}
airlines.show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
What if we want to apply a user defined function? Here is an example where a new function is defined that combines Year and Month into a string. I also use the sample function to illustrate how to get a random subset of the data. Finally, I show an important function called ``cache``. It is important because we may want to reuse a result. Cache tells Spark that we want to reuse something so please try to keep it cached for us. Finally, I show how you can use orderBy to sort the data.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def getYearMonthStr(year, month):
    return '%d-%02d'%(year,month)

udfGetYearMonthStr = udf(getYearMonthStr, StringType())

example1 = on_time_df.select('Year','Month').withColumn(
    'YearMonth', udfGetYearMonthStr('Year','Month')).sample(0.000001).cache()

example1.show()
```

```python slideshow={"slide_type": "subslide"}
example1.orderBy('YearMonth').show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Finally, there are a number of things to make the world go round, such as renaming a column:
```python
df.withColumnRenamed("dob","DateOfBirth").printSchema()
```
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def getYearMonthStr(year, month):
    return '%d-%02d'%(year,month)

udfGetYearMonthStr = udf(getYearMonthStr, StringType())
```

**Exercise 1:** Create a dataframe that contains the average delay for each airline for each month of each year (i.e., group by carrier, year, and month):
* Columns: Carrier, average_delay, YearMonth
* Carrier must be one of the following: 'AA','WN','DL','UA','MQ','EV','AS','VX'
* Must be ordered by YearMonth, Carrier
* The column to aggregate is ArrDelay

```python slideshow={"slide_type": "subslide"}
def exercise_1(on_time_df):
    result = None
    # Your solution here
    return result
```

```python slideshow={"slide_type": "subslide"}
airline_delay = exercise_1(on_time_df)
airline_delay.show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 2:** Now add a column with the airline name (i.e., use a join). Here is an example from the Spark documentation. Please order your result by YearMonth and Carrier.

```python
# To create DataFrame using SparkSession
people = spark.read.parquet("...")
department = spark.read.parquet("...")

people.filter(people.age > 30).join(department, people.deptId == department.id) \
  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})
```
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def exercise_2(airline_delay,airlines):
    result = None
    # Your solution here
    return result
```

```python slideshow={"slide_type": "subslide"}
airline_delay2 = exercise_2(airline_delay,airlines)
airline_delay2.show()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
If you did everything correctly, you are now rewarded with a nice graph :)
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
import numpy as np

airline_delay_pd = airline_delay2.toPandas()

import altair as alt

alt.Chart(airline_delay_pd).mark_line().encode(
    x='YearMonth',
    y='average_delay',
    color='AirlineName'
)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 3:** Let's assume you believe that the delays experienced by some airlines are correlated. The cause is a different story as we all know correlation does not equal causation. But correlation is often what we can easily calculate, so let's do it on a month by month basis. The first step is of course to get the data in the correct format. We would like each airline to have it's own column because we can easily compute the correlation between columns. Each row in this new dataframe should be a YearMonth.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def exercise_3(airline_delay2):
    result = None
    # partial solution
    # airline_delay2.groupBy(?).pivot(?).agg(avg(?))
    # Your solution here
    return result
```

```python slideshow={"slide_type": "subslide"}
data_for_corr = exercise_3(airline_delay2)

# The data is now small enough to handle, so let's get it into pandas and calculate the correlation and filling
# in missing values with the mean of the column

df = data_for_corr.toPandas().set_index('YearMonth')
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
imp_mean.fit(df)

df_imputed_nan = pd.DataFrame(imp_mean.transform(df),columns=df.columns,index=df.index)
df_imputed_nan
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Now let's take a look at the correlations
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
df_imputed_nan.corr()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Stop and think:** What stands out to you? Let me clean it up and sort it for you.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
corrs = df_imputed_nan.corr()
corrs.values[np.tril_indices(len(corrs))] = np.NaN 
corrs.stack().sort_values(ascending=False)
```

```python
# Don't forget to push with ./submit.sh
```

```python

```
