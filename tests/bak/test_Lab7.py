import sys
import os
sys.path.append(".")

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab7.joblib")

# Import the student solutions
import Lab7_helper as helper

from pymongo import MongoClient
client = MongoClient()

db = client["csc-369"]

col = db["daily"]

def test_exercise_1():
    assert answers['exercise_1'] == helper.exercise_1(col)

def test_exercise_2():
    assert answers['exercise_2'] == helper.exercise_2(col)

def test_exercise_3():
    result = list(helper.exercise_3(col,20200401,20200402))
    assert answers['exercise_3'] == helper.process_exercise_3(result)

def test_exercise_4():
    result = list(helper.exercise_4(col,20200401,20200402).find())
    assert answers['exercise_4'] == helper.process_exercise_4(result)
