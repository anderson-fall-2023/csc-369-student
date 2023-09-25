import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab2_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib
answers = joblib.load(str(DIR)+"/answers_Lab2.joblib")

import os
from pathlib import Path
home = str(Path.home())

from os import path
book_files = []
for book in open(f"{DIR}/../data/gutenberg/order.txt").read().split("\n"):
    if path.isfile(f'{DIR}/../data/gutenberg/{book}-0.txt'):
        book_files.append(f'{DIR}/../data/gutenberg/{book}-0.txt')

def fix_lines(lines):
    new_lines = []
    for line in lines:
        book = line[0].split("/")[-1]
        new_lines.append((book,line[1]))
    return new_lines

def test_exercise_1():
    assert Lab2_helper.read_line_at_pos(book_files[0],100) == answers['exercise_1']

def test_exercise_2():
    index = Lab2_helper.inverted_index(book_files[0])
    assert set(index['things']) == set(answers['exercise_2'])

def test_exercise_3():
    index = Lab2_helper.merged_inverted_index(book_files)
    assert set(index.keys()) == answers['exercise_3']

def test_exercise_4():
    index = Lab2_helper.merged_inverted_index(book_files)    
    lines = fix_lines(Lab2_helper.get_lines(index,'things'))
    assert set(lines) == set(fix_lines(answers['exercise_4']))

def test_exercise_5():
    index = Lab2_helper.merge(data_dir=f"{DIR}/../data")
    lines = fix_lines(Lab2_helper.get_lines(index,'things'))
    assert set(lines) == set(fix_lines(answers['exercise_5']))
