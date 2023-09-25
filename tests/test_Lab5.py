import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab5_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab5.joblib")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

from operator import add

def run_exercise_1(func):
    A = Lab5_helper.A1
    B = Lab5_helper.B1

    A_RDD = sc.parallelize(A)
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def run_exercise_2(func):
    A = Lab5_helper.A2
    B = Lab5_helper.B2

    A_RDD = sc.parallelize(A)
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def run_exercise_3(func):
    A = Lab5_helper.A3
    B = Lab5_helper.B3
    
    A_RDD = sc.parallelize(A)
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def test_exercise_1():
    assert set(answers['exercise_1']) == set(run_exercise_1(Lab5_helper.exercise_1))

def test_exercise_2():
    assert set(answers['exercise_2']) == set(run_exercise_2(Lab5_helper.exercise_2))

def test_exercise_3():
    assert set(answers['exercise_3']) == set(run_exercise_3(Lab5_helper.exercise_3))
