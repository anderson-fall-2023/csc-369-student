import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab4_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab4.joblib")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

from operator import add

def run_exercise_1(func):
    A = [('A[1,:]',[1, 2, 3]),('A[2,:]',[4, 5,6])]
    A_RDD = sc.parallelize(A)
    B = [('B[:,1]',[7,9,11]),('B[:,2]',[8,10,12])]
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def run_exercise_2(func):
    A = [['A',1,1,1],
     ['A',1,2,0],
     ['A',2,1,3],
     ['A',2,2,4],
     ['A',3,1,0],
     ['A',3,2,6],
     ['A',4,1,7],
     ['A',4,2,8]
    ]
    A_RDD = sc.parallelize(A)

    B = [['B',1,1,7],
         ['B',1,2,8],
         ['B',1,3,9],
         ['B',2,1,0],
         ['B',2,2,11],
         ['B',2,3,0]
        ]
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def run_exercise_3(func):
    A = [['A',1,1,1],
         ['A',2,1,3],
         ['A',2,2,4],
         ['A',3,2,6],
         ['A',4,1,7],
         ['A',4,2,8]
        ]
    A_RDD = sc.parallelize(A)

    B = [['B',1,1,7],
         ['B',1,2,8],
         ['B',1,3,9],
         ['B',2,2,11]
        ]
    B_RDD = sc.parallelize(B)
    return func(A_RDD,B_RDD)

def test_exercise_1():
    assert set(answers['exercise_1']) == set(run_exercise_1(Lab4_helper.exercise_1))

def test_exercise_2():
    assert set(answers['exercise_2']) == set(run_exercise_2(Lab4_helper.exercise_2))

def test_exercise_3():
    assert set(answers['exercise_3']) == set(run_exercise_3(Lab4_helper.exercise_3))
