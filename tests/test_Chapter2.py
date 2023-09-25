import sys
import os
sys.path.append(".")

# Import the student solutions
import Chapter2_helper


from os import path
book_files = []
for book in open("../data/gutenberg/order.txt").read().split("\n"):
    if path.isfile(f'../data/gutenberg/{book}-0.txt'):
        book_files.append(f'../data/gutenberg/{book}-0.txt')

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib
answers = joblib.load(str(DIR)+"/answers_Chapter2.joblib")

def test_exercise_1():
    assert Chapter2_helper.count_words(book_files) == answers['exercise_1']