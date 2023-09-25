import sys
import os
sys.path.append(".")

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab6.joblib")

# Import the student solutions
import Lab6_helper as helper

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
