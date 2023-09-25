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
# Lab 3 - Ray and Dask

## Creating an inverted index and Word Counting

Please review Lab 2 before proceeding. Part of this lab is creating an inverted index, but using Ray instead of Parallel. We'll then move onto the more complicated word counting example.
<!-- #endregion -->

```python slideshow={"slide_type": "skip"}
import ray
ray.init(ignore_reinit_error=True)
```

```python slideshow={"slide_type": "skip"}
%load_ext autoreload
%autoreload 2


# Put all your solutions into Lab1_helper.py as this script which is autograded
import Lab3_helper
    
import os
from pathlib import Path
home = str(Path.home())

import pandas as pd
```

## Inverted Index

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 1:**

You have already written most of what you need to use Ray to construct distributed inverted indices. Here I want you to modify Lab3_helper.py to use Ray and return the final inverted index. I'm supplying the code that divides your books into three sets.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
group1 = Lab3_helper.get_book_files(f"{home}/csc-369-student/data/gutenberg/group1")
group2 = Lab3_helper.get_book_files(f"{home}/csc-369-student/data/gutenberg/group2")
group3 = Lab3_helper.get_book_files(f"{home}/csc-369-student/data/gutenberg/group3")
```

```python slideshow={"slide_type": "subslide"}
index = Lab3_helper.merge([group1,group2,group3])
```

```python
index['Education']
```

```python slideshow={"slide_type": "subslide"}
index['Education']
```

```python
# clean up memory to help us all co-exist on the same machine
index = None
import gc
gc.collect()
```

## Word Counting
Now consider a different problem of common interest. Suppose we have a large corpus (fancy word common in natural language processing) and we want to calculate the number of times a word appears. We could try to hack our inverted index, but let's insert the requirement that this must be a clean implementation. In other words, I'll be manually reviewing your design and making you redo the assignment if it isn't "clean". 

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 2:**

Write a function that counts the words in a book. Output format shown below. You do not have to worry about punctuation and capitalization. In other words, please stick to simple f.readlines() and line.split(" "). Do not strip anything out.
<!-- #endregion -->

```python
counts = Lab3_helper.count_words(group1[0])
```

```python tags=[] jupyter={"outputs_hidden": true}
counts
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 3**

Now let's distribute this using Ray. Please implement a function that parallelizes the word counting and subsequent merges.
<!-- #endregion -->

```python
merged_counts = Lab3_helper.merge_count_words([group1,group2,group3])
```

```python
merged_counts
```

```python
merged_counts['things']
```

```python slideshow={"slide_type": "skip"} tags=[]
# Don't forget to push!
```
```python

```
