import sys
import os
sys.path.append(".")

# Import the student solutions
import Lab3_helper

import pathlib
DIR=pathlib.Path(__file__).parent.absolute()

import joblib 
answers = joblib.load(str(DIR)+"/answers_Lab3.joblib")

import ray

group1 = Lab3_helper.get_book_files(f"{DIR}/../data/gutenberg/group1")
group2 = Lab3_helper.get_book_files(f"{DIR}/../data/gutenberg/group2")
group3 = Lab3_helper.get_book_files(f"{DIR}/../data/gutenberg/group3")

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
    student = index2set(Lab3_helper.fix_index(Lab3_helper.merge([group1,group2,group3]),answers['exercise_1_keys']))
    ray.shutdown()
    assert student == index2set(answers['exercise_1'])

def test_exercise_2():
    student = counts2set(Lab3_helper.count_words(group1[0]))
    assert student == counts2set(answers['exercise_2'])

def test_exercise_3():
    ray.init(ignore_reinit_error=True)
    student = Lab3_helper.merge_count_words([group1,group2,group3])
    ray.shutdown()
    assert counts2set(student) == counts2set(answers['exercise_3'])
