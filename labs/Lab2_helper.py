import json

import os
from pathlib import Path
home = str(Path.home())

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

def merge(data_dir=f"{home}/csc-369-student/data"):
    index = {}
    # r = os.system('parallel command from above')
    if r == 0:
        for file in ["group1.json","group2.json","group3.json"]:
            # YOUR SOLUTION HERE
            pass
        os.system("rm group1.json group2.json group3.json")
    return index