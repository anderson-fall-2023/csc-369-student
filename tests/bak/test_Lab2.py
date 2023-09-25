import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab2_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab2.joblib")

import ray

import glob
def get_book_files(input_dir):
    return glob.glob(f"{input_dir}/*.txt")

group1 = get_book_files(f"{DIR}/../data/gutenberg/group1")
group2 = get_book_files(f"{DIR}/../data/gutenberg/group2")
group3 = get_book_files(f"{DIR}/../data/gutenberg/group3")

def fix_index(index,keys):
    new_index = {}
    for key in keys:
        new_index[key]={}
        for book in index[key]:
            book2 = book.split("/")[-1]
            new_index[key][book2] = index[key][book]
    return new_index

def index2set(index):
    lines = []
    for key in index.keys():
        for book in index[key]:
            lines.append((key,book,tuple(sorted(index[key][book]))))
    return set(lines)

def counts2set(counts):
    lines = []
    for key in counts.keys():
        lines.append((key,counts[key]))
    return set(lines)

def test_exercise_1():
    ray.init(ignore_reinit_error=True)
    student = index2set(fix_index(Lab2_helper.merge([group1,group2,group3]),answers['exercise_1_keys']))
    ray.shutdown()
    assert student == index2set(answers['exercise_1'])

def test_exercise_2():
    student = counts2set(Lab2_helper.count_words(group1[0]))
    assert student == counts2set(answers['exercise_2'])

def test_exercise_3():
    ray.init(ignore_reinit_error=True)
    student = Lab2_helper.merge_count_words([group1,group2,group3])
    ray.shutdown()
    assert counts2set(student) == counts2set(answers['exercise_3'])
