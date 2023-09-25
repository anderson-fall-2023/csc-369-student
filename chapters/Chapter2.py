# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,md,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.8.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] slideshow={"slide_type": "slide"} hideCode=false hidePrompt=false
# # Chapter 2
#
# ## Distributed computing at the command line
#
# Paul E. Anderson

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Ice Breaker
#
# What's is the best pizza place in SLO?

# + [markdown] slideshow={"slide_type": "subslide"}
# While this text can be viewed as PDF, it is most useful to have a Jupyter environment. I have an environment ready for each of you, but you can get your own local environment going in several ways. One popular way is with Anaconda (<a href="https://www.anaconda.com/">https://www.anaconda.com/</a>. Because of the limited time, you can use my server.

# + slideshow={"slide_type": "skip"}
# %load_ext autoreload
# %autoreload 2


# Put all your solutions into Lab1_helper.py as this script which is autograded
import Chapter2_helper 

from pathlib import Path
home = str(Path.home()) # all other paths are relative to this path. 
# This is not relevant to most people because I recommended you use my server, but
# change home to where you are storing everything. Again. Not recommended.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Command Line
#
# In Chapter 1, I included several videos where you had to follow along as I entered commands on the command line. In this chapter we are going to increase our mastery of the command line and distributed computing at the same time. 
#
# The command line is very agile. It has a read-eval-print loop that lends itself to interactive exploration as well as automation. It has been a mainstay in the toolkit of modern computer science, and it will remain so for a long time to come.
#
# The command line augments and amplifies technology we are already using. We'll use Spark, Hadoop, MongoDB, and other programs in this class. The command line is important across these technologies. 
#
# The command line is scalable which is very important for distributed computing. It allows us to automate many tasks and scale them in a distributed manner.
#
# The command line is ubiquitous. 95% of the worlds supercomputers use Unix/Linux and are accessible by the command line. 

# + [markdown] slideshow={"slide_type": "subslide"}
# ### How are distributed systems different?
# While we can abstract away some of elements of distributed computing, we are going to study approaches for:
# 1. How to store data on multiple systems?
# 2. How to handle updates and fix (or handle) inconsistencies?
# 3. How do we assemble the full answer?

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Practical Considerations
# Most real world examples that need distributed computing need distributed computing because they would otherwise (and may still) require a long time to run. This isn't practical for learning. More importantly to remember, even in the real world we test on small subsets of data before scaling up. All of the examples throughout are scaled down representations of a real problem that may require distributed computing depending on time and resources.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Parallel Processing Logs
# Consider the cybersecurity task of examining web server logs. Specifically, read/skim this article:
# <a href="https://resources.infosecinstitute.com/topic/log-analysis-web-attacks-beginners-guide/">Log Analysis</a>.
#
# Once you have read the article, consider the problem of running Scalp on a single log. Probably not an issue. Consider running it on a very large log (web server logs can get very very big). 
#
# Because they can get big they are often archived routinely. So now you have a problem of looking through many different log files. We may want to run Scalp on the logs over a long period of time. We may want to run it with different parameters. We may make mistakes and need to run it again quickly. You are starting to get my point I think.
#
# Would this problem need distributed computing. Discuss in your groups and bring the answer back in a few minutes.

# + [markdown] slideshow={"slide_type": "subslide"}
# **It boils down to these questions for us over and over again (some of these overlap):**
# * Do I need a more efficient (i.e., faster) approach?
# * Can my task be broken down into components that may be analyzed and then combined?
# * Is it worth the effort? 
# * Can you just wait for this answer?
# * Can you estimate how long a non-distributed approach will take?

# + [markdown] slideshow={"slide_type": "slide"}
# ## Detour: Linux and Bash
#
# While there are many tutorials and introduction to Bash, I like this one: https://ubuntu.com/tutorials/command-line-for-beginners. You may do almost the entire tutorial directly in this notebook. There are several ways to run Bash within Jupyter. Here are some examples.

# + [markdown] slideshow={"slide_type": "slide"}
# ## Example Data: Project Gutenberg

# + [markdown] slideshow={"slide_type": "subslide"}
# Consider another example. What if you were interested in looking for patterns in the top 100 books last year? Your first idea is to compare the word frequencies in the top 25 books to the next 25 books. 
# * You have a level of programming skill that makes writing a Python program to look through a single book (text file) within range. 
# * You are familiar with Python dictionaries and can sequentially process a book. 
#
# Let's design such a program together!

# + [markdown] slideshow={"slide_type": "subslide"}
# There is a list of books downloaded from Project Gutenberg in the data folder
# * Website has over 60,000 books. 
# * I downloaded the most popular books on 1/12/2021 
# * The order in which they are ranked is in order.txt. 
#
# Let's see a few Bash commands to take a look at the books.

# + slideshow={"slide_type": "subslide"}
# !ls {home}/csc-369-student/data/gutenberg

# + slideshow={"slide_type": "subslide"}
# !ls -l {home}/csc-369-student/data/gutenberg | wc -l

# + slideshow={"slide_type": "subslide"}
# !head {home}/csc-369-student/data/gutenberg/order.txt

# + [markdown] slideshow={"slide_type": "subslide"}
# **Python code to create a list of files in the ranked order**

# + slideshow={"slide_type": "fragment"}
from os import path
book_files = []
for book in open(f"{home}/csc-369-student/data/gutenberg/order.txt").read().split("\n"):
    if path.isfile(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt'):
        book_files.append(f'{home}/csc-369-student/data/gutenberg/{book}-0.txt')
book_files[:10]

# + [markdown] slideshow={"slide_type": "subslide"}
# ### What is our top book?

# + slideshow={"slide_type": "fragment"}
# !head {book_files[0]}

# + [markdown] slideshow={"slide_type": "subslide"}
# ### What is our second book?

# + slideshow={"slide_type": "fragment"}
# !head {book_files[1]}

# + [markdown] slideshow={"slide_type": "subslide"}
# **Exercise 1:** Write a function that counts the number of times a word appears in a book. In other words, we want to know the frequencies of the words in each book. One way to store this is in a dictionary for each book. Do not worry about punctuation or capitalization. This is just an exercise.

# + slideshow={"slide_type": "subslide"}
book_word_freq = Chapter2_helper.count_words(book_files)
import pandas as pd
pd.Series(book_word_freq)

# + [markdown] slideshow={"slide_type": "fragment"}
# **If you want to time it, there is a magic command for that.**

# + slideshow={"slide_type": "fragment"}
# %%timeit -n 1
book_word_freq = Chapter2_helper.count_words(book_files)

# + [markdown] slideshow={"slide_type": "slide"}
# ## Motivating Example: Inverted Index
#
# Information retrieval (IR) is a field concerned with the structure, analysis, organization, storage, searching, and retrieval of information.
#
# IR is the science of searching for documents, information within documents, and metadata about documents. Documents as used here describe an abstract piece of information that includes databases and the World Wide Web (WWW).

# + [markdown] slideshow={"slide_type": "subslide"}
# **Stop and think:** Take a moment and a piece of paper. Sketch out how you think the backend and the frontend process/architecture of search engine is constructed. Very high level of course. 

# + [markdown] slideshow={"slide_type": "subslide"}
# Image commented out by default.
#
# <!--<img src="https://www.researchgate.net/profile/Yoshiki-Mikami/publication/224197661/figure/fig1/AS:341147890274356@1458347403868/Architecture-of-Search-Engine.png" width=400>-->

# + [markdown] slideshow={"slide_type": "subslide"}
# We will not dig into all of the aspects shown in that image. 
#
# We will focus on one important algorithm: the creation of the inverted index. 
#
# We will implement this algorithm and apply it to produce an inverted index of our Project Gutenberg Dataset.

# + [markdown] slideshow={"slide_type": "subslide"}
# <img src="https://hdscorp--c.na74.content.force.com/servlet/servlet.ImageServer?id=0151J000004MM9EQAW&oid=00Do0000000IJig">

# + [markdown] slideshow={"slide_type": "subslide"}
# **Stop and think:** How could it help someone trying to retrieve information from Project Gutenberg?

# + [markdown] slideshow={"slide_type": "fragment"}
# It provides a quick and efficient data structure to find the location of words/terms in our dataset. We could imagine a situation where an author or scholar would want to look up all occurences of a word, and it is reasonable to assume that they would benefit from viewing the nearby text containing the word/term.

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Implementation thoughts
#
# We will write algorithms throughout this course using different technology. 
#
# Our first approach to creating an inverted index using distributed computing will rely on minimal technology. Specifically, we will rely on our knowledge of Python and the command line.
#
# Later in the class, we will repeat this example using Hadoop and Spark.

# + [markdown] slideshow={"slide_type": "subslide"}
# Before you stop drop and code, please consider the following questions:
#
# 1. What part of computing the inverted index could be distributed? Think in terms of subtasks that can run in parallel.
#
# 2. Once you have the results of these subtasks (i.e., the parallel processing finishes), how do you combine the results into a single inverted index?

# + [markdown] slideshow={"slide_type": "slide"}
# ## GNU Parallel
# One of my favorite command line finds of all time: <a href="https://www.gnu.org/software/parallel/">https://www.gnu.org/software/parallel/</a>. This flexible program provides an easy to use way of running a command in *parrallel*. We will illustrate with a silly example, but it illustrates the point of *parallel*. 

# + [markdown] slideshow={"slide_type": "subslide"}
# Let's say you want to list the contents of three directors: dir1, dir2, and dir3. You can do this with:
#
# ```bash
# $ ls dir1
#
# $ ls dir2
#
# $ ls dir3
# ```

# + [markdown] slideshow={"slide_type": "subslide"}
# Using parallel you can do:
#
# ```bash
# parallel ls {} ::: dir1 dir2 dir3
# ```
#
# This command can be brocken down into the list after ::: that is split and inserted into {}. Each of these commands are excuted in parallel now. 
#
# **Stop and think:** Do you know which directory will be listed first?

# + [markdown] slideshow={"slide_type": "subslide"}
# Consider creating an inverted index for our small example of <100 books. 
#
# Technically, our small example wouldn't classify as something that needs distributed computing, but consider computing the architecture of a search engine we described previously. Creating an inverted index for the WWW is an application that needs distributed computing. 
#
# In both scenarios, the algorithms and processes are conceptually similar.

# + [markdown] slideshow={"slide_type": "subslide"}
# We will be leaving the implementation of an inverted index for Project Gutenberg to a lab exercise.
#
# For this lecture, we will discuss and demonstrate *parallel* by constructing a distributed computing version of our word/term frequency algorithm.

# + [markdown] slideshow={"slide_type": "subslide"}
# The **strength** of the GNU parallel project is it seamlessly takes something written (e.g., program, script) and executes it in parallel.
#
# The burden of constructing a merged/final output is not covered by parallel and must be done by you. More sophisticated technologies such as Spark provide built-in support for this task.
#
# Still, we all have many useful programs not optimized for distributed computing that nevertheless could benefit from distributed computing.

# + [markdown] slideshow={"slide_type": "slide"}
# ## Case study: Term Frequency

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Running parallel
#
# We will break down a more sophisticated *parallel* command and answer the following:
# * Identify the pipes. What are they doing?
# * What is the structure of the find command?
# * What happened to the ::: in the parallel command
# * What does the -v mean in the egrep command?

# + slideshow={"slide_type": "subslide"}
# !find ~/csc-369-student/data/gutenberg -name "*.txt" | egrep -v order.txt | parallel echo {}

# + [markdown] slideshow={"slide_type": "subslide"}
# Often I use this pattern where echo is the last command. This helps me debug before I even get started. To me programming is about debugging more than anything. The better I am at debugging, the better programmer. 
#
# Next we will use a script ``Chapter2_count_words_book.py`` and *parallel* to perform a distributed computation of counting words in parallel.

# + slideshow={"slide_type": "subslide"}
# %%timeit -n 1
# !find ~/csc-369-student/data/gutenberg -name "*.txt" | egrep -v order.txt | parallel python Chapter2_count_words_book.py

# + [markdown] slideshow={"slide_type": "subslide"}
# ### Results
# * We got a speedup even though we had to start Python multiple times (our results may vary on a shared environment). 
# * There is always overhead when moving towards distributed computing. 
# * We aren't going to do the actual comparison of top 25 to next 25. 
# * Well... why not. We are almost there. This material is definitely not part of this class though.

# + slideshow={"slide_type": "subslide"}
import pandas as pd
import altair as alt

book_word_freq = Chapter2_helper.count_words(book_files)

top_books = book_files[:25]
next_books = book_files[25:50]

top_df = pd.DataFrame(columns=["book","word","freq","group"]).set_index(["book","word"])
next_df = pd.DataFrame(columns=["book","word","freq","group"]).set_index(["book","word"])

# Normalize the counts for each book
for book in top_books:
    data = pd.Series(book_word_freq[book])
    data = (data/data.sum()).to_frame().reset_index()
    data.columns=["word","freq"]
    data["book"] = book
    data["group"] = "top"
    top_df = top_df.append(data.set_index(["book","word"]))
    
for book in next_books:
    data = pd.Series(book_word_freq[book])
    data = (data/data.sum()).to_frame().reset_index()
    data.columns=["word","freq"]
    data["book"] = book
    data["group"] = "bottom"
    next_df = next_df.append(data.set_index(["book","word"]))

# + slideshow={"slide_type": "subslide"}
next_df = next_df.reset_index()
top_df = top_df.reset_index()

# + slideshow={"slide_type": "subslide"}
plot_df = top_df.append(next_df)
plot_df

# + slideshow={"slide_type": "subslide"}
pivot_df = plot_df.groupby(['word','group']).mean().reset_index().pivot_table(index='group',columns='word').T

# + slideshow={"slide_type": "subslide"}
top_k = 30
top_words = [v[1] for v in (pivot_df["bottom"] - pivot_df["top"]).abs().sort_values(ascending=False)[:top_k].index]
top_words

# + slideshow={"slide_type": "subslide"}
alt.Chart(plot_df.set_index('word').loc[top_words].reset_index()).mark_bar().encode(
    x='group',
    y='freq',
    column='word',
    color='group'
)

# + [markdown] slideshow={"slide_type": "subslide"}
# **Anyways...** I feel like we've gotten that out of our system. In reality, we need to perform a lot more data cleaning and data organization before such an analysis will yield what we want.
#
# Now back to distributed computing.

# + [markdown] slideshow={"slide_type": "slide"}
# ## Wrapping up our warmup
# There is so much more to Parallel then we can discuss here. 
#
# For example, you can use Parallel to execute commands on multiple nodes: <a href="https://www.gnu.org/software/parallel/parallel_tutorial.html#Remote-execution">https://www.gnu.org/software/parallel/parallel_tutorial.html#Remote-execution</a>. 
#
# Parallel is one of the most useful distributed computing tools at your disposal. If you have a command line program that would benefit from running in a distributed fashion. Do NOT rewrite it until you have considered running it this way.

# + [markdown] slideshow={"slide_type": "subslide"}
# ## Questions
# 1. Describe an instance where you would reach for GNU parallel instead of more advanced and integrated solutions.
#
#
# 2. When solving a distributed computing problem, what doesn't GNU parallel do for you? What are going to have to implement in the lab?
#
#
#
#

# + slideshow={"slide_type": "skip"}
# Don't forget to push!
