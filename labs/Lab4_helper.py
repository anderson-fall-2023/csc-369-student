data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]

def word_counts(rdd):
    counts = None
    return counts

def word_freq(rdd):
    output = []
    # Your solution here
    return output

def load_rdd_all_books(sc,dir):
    lines = None
    return lines

# This is a helper function you can use
def count_words(content):
    counts = {}
    for line in content.split("\n"):
        words = line.split(" ")
        for word in words:
            if word not in counts:
                counts[word] = 0
            counts[word] += 1
    return counts

def book_word_counts(sc,dir):
    res = None
    return res

def lower_case_word_freq(output): # output is the output of word_freq function
    res = None
    return res