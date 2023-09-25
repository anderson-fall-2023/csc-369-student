---
jupyter:
  jupytext:
    encoding: '# -*- coding: utf-8 -*-'
    formats: ipynb,md,py
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region slideshow={"slide_type": "slide"} -->
# Lab 2 - Creating an inverted index

Overview of inverted indexes: <a href="https://en.wikipedia.org/wiki/Inverted_index">https://en.wikipedia.org/wiki/Inverted_index</a>

In this lab you will create an inverted index for the Gutenberg books. What I want you to do is create a single index that you can quickly return all the lines from all the books that contain a specific word. We will be using the basic and naive split functionality from the chapter (i.e., don't worry about punctuation, etc). Those are details that are not necessary for our exploration into distributed computing. We will use GNU Parallel to distributed our solution.

This lab will focus on distributing the workload across multiple cores/processors. We will bring lab 1 and lab 2 together in lab 3 and use gluster and parallel together.
<!-- #endregion -->

```python slideshow={"slide_type": "skip"}
%load_ext autoreload
%autoreload 2


# Put all your solutions into Lab1_helper.py as this script which is autograded
import Lab2_helper
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
```

<!-- #region slideshow={"slide_type": "subslide"} -->
### Read in the book files for testing purposes
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
from os import path
book_files = []
for book in open(f"{home}/csc-369-student/data/gutenberg/order.txt").read().split("\n"):
    if path.isfile(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt'):
        book_files.append(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt')
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 1:** Create a function that returns a line that is read after seeking to ``pos`` in ``book``.

Hint: You'll need to open a file object and the call seek. Calling readline will then work as expected.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
line = Lab2_helper.read_line_at_pos(book_files[0],100)
display(line)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Notice that readline reads from the current position until the end of the line.** For the inverted index, you'll want to make sure to record only the positions that get you to the beginning of the line.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
display(Lab2_helper.read_line_at_pos(book_files[0],95))
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 2:** Create a function that returns a Python dictionary representing the inverted index. The dictionary should contain an offset that puts the file point at the beginning of the line. I used ``.split()`` without any arguments.

Hint: I used the ``tell`` function to return the correct offset.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
index = Lab2_helper.inverted_index(book_files[0])
display(index['things'])
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 3:** Write a function that reads all of inverted indices into a single inverted index in the format shown below.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
index = Lab2_helper.merged_inverted_index(book_files)
display(pd.Series(index.keys()))
```

```python jupyter={"outputs_hidden": true}
index['things']
```

```python
pd.Series(index['things'])
```

```python slideshow={"slide_type": "subslide"}
pd.Series(index['things'])
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 4:** Write a function that returns all of the lines from all of the books that contain a word. Duplicate lines are correct if the line has more than one occurence of the word. Format shown below.
<!-- #endregion -->

```python
lines = Lab2_helper.get_lines(index,'things')
lines[:10]
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 5:**

Write a Python script that we can execute using Parallel in the following manner. 

I have hard coded an example script that will return the incorrect answer, but it will run. 

Your job is to remove the hard coded answer and insert the correct solution that will produce the correct answer. I have supplied the directory structure, and the parallel commands. You do need to write code that merges the groups back together.
<!-- #endregion -->

**Here are the three groups.** Each directory has about 25 books. We could distribute these to different machines in a cluster, but you get the idea without that step.

```python
!ls -d {home}/csc-369-student/data/gutenberg/group*
```

```python
!ls {home}/csc-369-student/data/gutenberg/group1
```

```python
!ls {home}/csc-369-student/data/gutenberg/group2
```

```python
!ls {home}/csc-369-student/data/gutenberg/group3
```

**Running a single directory:** You can run a single directory with the following command and store the results to a file.

```python slideshow={"slide_type": "subslide"}
!python Lab2_exercise5.py {home}/csc-369-student/data/gutenberg/group1 > group1.json
```

We can easily read these back into Python by relying on the JSON format. While more strict than Python dictionaries. They are very similar for our purposes (<a href="https://www.json.org/json-en.html">https://www.json.org/json-en.html</a>). 

```python
import json
group1_results = json.load(open("group1.json"))
pd.Series(group1_results['things'])
```

**You can run the files in parallel using**

```python
!ls {home}/csc-369-student/data/gutenberg/group1
```

```python
# !parallel "python Lab2_exercise5.py" ... # Come and see me to get the ... You'll have to try to come up with it first
```

```python
index = Lab2_helper.merge()
# You've done it!
```

```python slideshow={"slide_type": "subslide"}
pd.Series(index['things'])
```

This solution should match your solution above that was single thread, but now you are a rockstar distributed computing wizard who could process thousands of books on a cluster with nothing other than simple Python and GNU parallel.

```python slideshow={"slide_type": "skip"}
# Don't forget to push!
```
```python
!rm *.json
```

```python

```
