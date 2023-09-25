import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab3_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab3.joblib")

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

from operator import add

def run_exercise_1(func):
    lines = sc.textFile("file:/home/csc-369-student/data/gutenberg/group1/11-0.txt") # read the file into the cluster
    
    counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(func)
    output = counts.collect()
    return set(output)

def run_exercise_2(func):
    lines = func(sc,"file:/home/csc-369-student/data/gutenberg/group1")
    
    counts = lines.flatMap(lambda x: x.split(' ')) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)
    output = counts.collect()
    return set(output)

def run_exercise_3(func):
    book_word_counts = func(sc,"file:/home/csc-369-student/data/gutenberg/group1")
    entries = []
    for book,word_counts in book_word_counts:
        temp = counts2tuple(word_counts)
        entries.append((book,temp))
    return set(entries)

def counts2tuple(counts):
    lines = []
    for key in sorted(list(counts.keys())):
        lines.append((key,counts[key]))
    return tuple(lines)

def test_exercise_1():
    assert answers['exercise_1'] == run_exercise_1(Lab3_helper.exercise_1_add)

def test_exercise_2():
    assert answers['exercise_2'] == run_exercise_2(Lab3_helper.exercise_2_load_rdd_all_books)

def test_exercise_3():
    assert answers['exercise_3'] == run_exercise_3(Lab3_helper.exercise_3_book_word_counts)
