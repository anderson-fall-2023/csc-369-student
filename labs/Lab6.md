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
# Lab 6

For this lab we will analyze a large dataset from the Department of Transportation. I have already converted the data to the parquet format discussed in class, but if you don't believe me that it has benefits, let's check out some stats:
<!-- #endregion -->

Here is the original data as I downloaded it without any modification other than I unzipped it.

```python
!du -sh /disk/airline-data
```

Let's take a look at the data after I processed it.

```python
!du -sh /disk/airline-data-processed
```

Well I don't know about you, but that seems amazing :) Here is the original compressed file size. It is important to realize while the .tar.gz file is "small" at 4.7 GB, we can't access it with Spark or any other program without uncompressing it. But we can do that with the parquet files!

```python
!du -sh /disk/airline-data.2003-2018.tar.gz
```

```python slideshow={"slide_type": "skip"}
%load_ext autoreload
%autoreload 2
```

```python slideshow={"slide_type": "skip"}
# make sure your run the cell above before running this
import Lab6_helper
```

```python slideshow={"slide_type": "skip"}
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext
```

## Exploratory Data Analysis

```python
# Our main data source
on_time_df = spark.read.parquet('file:///disk/airline-data-processed/airline-data.parquet')
on_time_df.show()
```

That is a bit brutal to look at... Consider examining like:

```python
on_time_df.columns
```

```python
# The first row
on_time_df.first()
```

```python
airlines = spark.read.parquet('file:///disk/airline-data/DOT_airline_codes_table')
```

```python
airlines.show()
```

**Exercise 1:** Create a dataframe that contains the average delay for each airline for each month of each year (i.e., group by carrier, year, and month):
* Columns: Carrier, average_delay, YearMonth
* Carrier must be one of the following: 'AA','WN','DL','UA','MQ','EV','AS','VX'
* Must be ordered by YearMonth, Carrier
* The column to aggregate is ArrDelay

```python
airline_delay = Lab6_helper.exercise_1(on_time_df,spark)
airline_delay.show()
```

<!-- #region -->
**Exercise 2:** Now add a column with the airline name (i.e., use a join). Here is an example from the Spark documentation. Please order your result by YearMonth and Carrier.

```python
# To create DataFrame using SparkSession
people = spark.read.parquet("...")
department = spark.read.parquet("...")

people.filter(people.age > 30).join(department, people.deptId == department.id) \
  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})
```
<!-- #endregion -->

```python
airline_delay2 = Lab6_helper.exercise_2(airline_delay,airlines,spark)
airline_delay2.show()
```

If you did everything correctly, you are now rewarded with a nice graph :)

```python
import numpy as np

airline_delay_pd = airline_delay2.toPandas()

import altair as alt

alt.Chart(airline_delay_pd).mark_line().encode(
    x='YearMonth',
    y='average_delay',
    color='AirlineName'
)
```

**Exercise 3:** Let's assume you believe that the delays experienced by some airlines are correlated. The cause is a different story as we all know correlation does not equal causation. But correlation is often what we can easily calculate, so let's do it on a month by month basis. The first step is of course to get the data in the correct format. We would like each airline to have it's own column because we can easily compute the correlation between columns. Each row in this new dataframe should be a YearMonth.

```python
data_for_corr = Lab6_helper.exercise_3(airline_delay2,spark)

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

Now let's take a look at the correlations

```python
df_imputed_nan.corr()
```

Anything stand out to you? Let me clean it up and sort it for you.

```python
corrs = df_imputed_nan.corr()
corrs.values[np.tril_indices(len(corrs))] = np.NaN 
corrs.stack().sort_values(ascending=False)
```

```python
# Good job!
# Don't forget to push with ./submit.sh
```

```python

```
