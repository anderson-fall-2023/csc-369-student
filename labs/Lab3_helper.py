import glob
import os
import ray
import json

def get_book_files(input_dir):
    return glob.glob(f"{input_dir}/*.txt")

def fix_index(index,keys):
    new_index = {}
    for key in keys:
        new_index[key]={}
        for book in index[key]:
            book2 = book.split("/")[-1]
            new_index[key][book2] = index[key][book]
    return new_index

def read_line_at_pos(book, pos):
    with open(book,encoding="utf-8") as f:
        # YOUR SOLUTION HERE
        return f.readline()

# Read in the file once and build a list of line offsets
def inverted_index(book):
    index = {}
    # YOUR SOLUTION HERE
    # Check out https://stackoverflow.com/a/40546814/9864659 for inspiration using seek and tell
    return index

def merged_inverted_index(book_files):
    index = {}
    for book in book_files:
        book_index = inverted_index(book)
        # YOUR SOLUTION HERE
        pass
    return index

def get_lines(index,word):
    lines = []
    for book in index[word]:
        # YOUR SOLUTION HERE
        pass
    return lines

def merge(groups):
    index = {}
    result_ids = []
    # YOUR SOLUTION HERE
    for group_index in ray.get(result_ids):
        # YOUR SOLUTION HERE
        pass
    return index

def count_words(book):
    counts = {}
    with open(book,encoding="utf-8") as f:
        for line in f.readlines():
            pass
    return counts


def merge_count_words(groups):
    counts = {}
    result_ids = []
    # YOUR SOLUTION HERE
    return counts
