import sys
import glob
import json

input_dir = sys.argv[1]
book_files = glob.glob(f"{input_dir}/*.txt")

# Read in the file once and build a list of line offsets
def inverted_index(book):
    file = open(book)
    index = {}
    # YOUR SOLUTION HERE
    # Check out https://stackoverflow.com/a/40546814/9864659 for inspiration using seek and tell
    return index

def merged_inverted_index(book_files):
    index = {}
    for book in book_files:
        book_index = inverted_index(book)
        # YOUR SOLUTION HERE
    return index


# hard coded index.
index = json.loads('{"Monasteries": {"../data/gutenberg/50040-0.txt": [0, 539], "../data/gutenberg/4300-0.txt": [165555]}, "Rule": {"../data/gutenberg/50040-0.txt": [0, 539, 1176, 7394, 23739, 41161, 61317, 76452, 76598, 93148, 106478, 106768, 106912, 107259, 111009, 111879, 120919, 125334, 130876, 130953, 131627, 131930], "../data/gutenberg/1400-0.txt": [545322], "../data/gutenberg/4300-0.txt": [241964, 1412047], "../data/gutenberg/3600-0.txt": [1256456], "../data/gutenberg/147-0.txt": [20549], "../data/gutenberg/45-0.txt": [378016], "../data/gutenberg/6130-0.txt": [132996, 139438, 659523]}}')
#index = merged_inverted_index(book_files) # add this line in once you are ready


print(json.dumps(index))
