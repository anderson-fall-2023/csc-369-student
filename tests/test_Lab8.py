import sys
import os
sys.path.append(".")

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab8.joblib")

# Import the student solutions
import Lab8_helper as helper

from pymongo import MongoClient
client = MongoClient()

db = client["csc-369"]

col = db["daily"]

def run_exercise_1():
    return helper.exercise_1(col,'60392e3656264fee961ca817')

def run_exercise_3():
    d = 20200315 # YYYY-MM-DD
    return helper.exercise_3(col,d)

def run_exercise_2():
    return helper.exercise_2(col,'CA')

def test_exercise_1():
    assert answers['exercise_1'] == run_exercise_1()

def test_exercise_2():
    assert answers['exercise_2'] == run_exercise_2()

def test_exercise_3():
    assert answers['exercise_3'] == run_exercise_3()

def test_exercise_4():
    assert answers['exercise_4'] == helper.exercise_4(col)

def test_exercise_5():
    assert answers['exercise_5'] == helper.exercise_5(col)

def test_exercise_6():
    result = list(helper.exercise_6(col,20200401,20200402))
    assert answers['exercise_6'] == helper.process_exercise_6(result)

def test_exercise_7():
    result = list(helper.exercise_7(col,20200401,20200402).find())
    assert answers['exercise_7'] == helper.process_exercise_7(result)
