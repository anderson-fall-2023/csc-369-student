import sys
import os
sys.path.append(".")

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab6.joblib")

from pyspark.sql import SparkSession

#from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()

sc = spark.sparkContext

# Import the student solutions
import Lab6_helper

on_time_df = spark.read.parquet('file:///disk/airline-data-processed/airline-data.parquet')
airlines = spark.read.parquet('file:///disk/airline-data/DOT_airline_codes_table')

def run_exercise_1():
    airline_delay = Lab6_helper.exercise_1(on_time_df,spark).head(10)
    return airline_delay

def run_exercise_2():
    airline_delay = Lab6_helper.exercise_1(on_time_df,spark)
    airline_delay2 = Lab6_helper.exercise_2(airline_delay,airlines,spark)        

    return airline_delay2.head(10)

def run_exercise_3():
    airline_delay = Lab6_helper.exercise_1(on_time_df,spark)
    airline_delay2 = Lab6_helper.exercise_2(airline_delay,airlines,spark)
    data_for_corr = Lab6_helper.exercise_3(airline_delay2,spark).toPandas()        

    return data_for_corr

def test_exercise_1():
    assert set(answers['exercise_1']) == set(run_exercise_1())

def test_exercise_2():
    assert set(answers['exercise_2']) == set(run_exercise_2())

def test_exercise_3():
    assert answers['exercise_3'].equals(run_exercise_3())
