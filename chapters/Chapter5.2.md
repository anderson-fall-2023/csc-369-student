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
# Chapter 5.2 - Matrix Multiplication in Spark using RDDs
<!-- #endregion -->

```python slideshow={"slide_type": "slide"}
%load_ext autoreload
%autoreload 2
```

```python slideshow={"slide_type": "subslide"}
from pyspark import SparkConf
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
```

<!-- #region slideshow={"slide_type": "slide"} -->
### Matrix Multiplication Reviewed
* Critical to a large number of tasks from graphics and cryptography to graph algorithms and machine learning.
* Computationally intensive. A naive sequential matrix multiplication algorithm has complexity of O(n^3). 
* Algorithms with lower computational complexity exist, but they are not always faster in practice.
* Good candidate for distributed processing
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
* Every matrix cell is computed using a separate, independent from other cells computation. The computation consumes O(n) input (one matrix row and one matrix column).
* Good candidate for being expressed as a MapReduce computation.
* For a refresher on matrix muliplication. <a href="https://www.youtube.com/watch?v=kuixY2bCc_0&ab_channel=MathMeeting">Here is one such video.</a>
* <a href="https://en.wikipedia.org/wiki/Matrix_multiplication#Definition">Formal definition</a>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/18/Matrix_multiplication_qtl1.svg/2560px-Matrix_multiplication_qtl1.svg.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "slide"} -->
### Why Spark for matrix multiplication? 
If you've ever tried to perform matrix multiplication and you've run out of memory, then you know one of the reasons we might want to use Spark. In general, it is faster to work with a library such as numpy when the matrices are reasonable in size. We would only see the performance benefits of a Spark approach at scale.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
### Creating our input

Creating the input for testing purposes is easy. In practice, we would be reading from files or a database. Please review the documentation on <a href="https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections">parallelized collections</a>.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Let $A$ be a matrix of size $m \times n$ and $B$ be a matrix of size $n \times s$. Then our goal is to create a matrix $R$ of size $m \times s$. 
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
Let's start with a concrete example that is represented in what seems like a reasonable way. In general, we use two dimensional arrays to represent lists. Things like:
```python
[[1,2,3],[4,5,6]]
```
We will do that here, but we will write each row as a key,value pair such as:
```python
[('A[1,:]',[1,2,3]),
 ('A[2,:]',[4,5,6])]
```
We'll switch to different formats later for reasons that you will notice while doing this first exercise. If you haven't seen ``A[1,:]`` it means this is the first row and all the columns of the A matrix. Below is how we create the RDDs.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
A = [('A[1,:]',[1, 2, 3]),('A[2,:]',[4, 5,6])]
A_RDD = sc.parallelize(A)

B = [('B[:,1]',[7,9,11]),('B[:,2]',[8,10,12])]
B_RDD = sc.parallelize(B)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
We can convert these into numpy arrays easily.
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
import numpy as np
A_mat = np.array(A_RDD.map(lambda v: v[1]).collect())
A_mat
```

```python slideshow={"slide_type": "fragment"}
B_mat = np.array(B_RDD.map(lambda v: v[1]).collect())
B_mat
```

<!-- #region slideshow={"slide_type": "slide"} -->
Let's ask numpy to do our multiplication for us. **Error below is on purpose**. The dot product between two vectors:
<img src="https://wikimedia.org/api/rest_v1/media/math/render/svg/5bd0b488ad92250b4e7c2f8ac92f700f8aefddd5">
So numpy will calculate the dot product of two vectors each time an entry (circle in image below) is needed:
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/Matrix_multiplication_diagram_2.svg/440px-Matrix_multiplication_diagram_2.svg.png">
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
np.dot(A_mat,B_mat)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
We have already transposed B in our example to make our map reduce task easier. The ``.dot`` function assumes we have not done the transpose. So in order for numpy to do the multiplication for us, we need to transpose the second matrix (note the .T).
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
np.dot(A_mat,B_mat.T)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Let's pick apart how we got the value 64. This is the dot product of row 1 of A and column 2 of B (Hint: when we discuss matrix we start counting at 1). Our goal then is to compute $n \times s$ values: one for each pair (i, k) of rows from matrix A and columns from matrix B.

To do this we'll need to join the two RDDs together. 

**Stop and think:** Why is the following empty?
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
A_RDD.join(B_RDD).collect()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Your solution here**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "fragment"} -->
Here is the output of the cartesian product:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
A_RDD.cartesian(B_RDD).collect()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Stop and think:** Can you calculate entry R[2,1]? Take a moment and do so on paper.

For reference, here is the numpy calculated answers:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
R = np.dot(A_mat,B_mat.T)
R[2-1,1-1] # -1 is to transform mathematical notation to 0-based indexing
```

<!-- #region slideshow={"slide_type": "subslide"} -->
$$
(4, 5, 6) \cdot (7, 9, 11) = 4*7+5*9+6*11 = 139
$$
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
We will now get Spark to calculate the matrix multiplication for us.
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
# These functions are needed because we used a string above 
# In the second implementation of matrix muliplication, we don't need to do this
def get_row(s):
    return int(s.split(",")[0].split("[")[-1])

def get_col(s):
    return int(s.split(",")[1].split("]")[0])
```

```python slideshow={"slide_type": "subslide"}
A2 = A_RDD.map(lambda kv: (get_row(kv[0]),kv[1]))
A2.collect()
```

```python slideshow={"slide_type": "fragment"}
B2 = B_RDD.map(lambda kv: (get_col(kv[0]),kv[1]))
B2.collect()
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 1:**
Using what I have defined above (A_RDD, B_RDD, A2, B2), the Spark functions (cartesian, map, collect), and the numpy function (np.dot or a loop of your own), compute the matrix multiplication of A_RDD and B_RDD. Here is a reminder of what cartesian results in:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
A2.cartesian(B2).collect()
```

```python slideshow={"slide_type": "subslide"}
def exercise_1(A_RDD,B_RDD):
    result = None
    A2 = A_RDD.map(lambda kv: (get_row(kv[0]),kv[1]))
    B2 = B_RDD.map(lambda kv: (get_col(kv[0]),kv[1]))

    # Your solution here
    return result

result = exercise_1(A_RDD,B_RDD)
result
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**We did it!** Matrix multiplication using map reduce.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "fragment"} -->
If you want to put it back in the same format, the keys provide that information:
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
R_mat = np.zeros((2,2))
for row_col,value in result:
    row,col = row_col
    R_mat[row-1,col-1] = value
R_mat
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 2:** Implement matrix multiplication using the following alternative format:

'row number', 'column number', 'value'

For this exercise, you cannot use loops or np.dot. It should be Spark centric using cartesian, join, map, add, reduceByKey, and/or collect. 
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
A = [[1,1,1],
     [1,2,0],
     [2,1,3],
     [2,2,4],
     [3,1,0],
     [3,2,6],
     [4,1,7],
     [4,2,8]
    ]
A_RDD = sc.parallelize(A)

B = [[1,1,7],
     [1,2,8],
     [1,3,9],
     [2,1,0],
     [2,2,11],
     [2,3,0]
    ]
B_RDD = sc.parallelize(B)
```

<!-- #region slideshow={"slide_type": "subslide"} -->
We will now convert this to numpy arrays and perform the multiplication:
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
A_mat = np.zeros((4,2))
for row,col,val in A:
    A_mat[row-1,col-1]=val
A_mat

B_mat = np.zeros((2,3))
for row,col,val in B:
    B_mat[row-1,col-1]=val
A_mat,B_mat
```

```python slideshow={"slide_type": "subslide"}
R = np.dot(A_mat,B_mat)
R
```

<!-- #region slideshow={"slide_type": "subslide"} -->
Before coding, consider the following diagram and ask yourself the following when considering how to calculate the yellow circle:

**How would you join/match the correct elements (rows and columns) of A and B together to compute, R[1,2]?**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/e/eb/Matrix_multiplication_diagram_2.svg/440px-Matrix_multiplication_diagram_2.svg.png">
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Your answer here**
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Stop and think:** Can you write Spark code that joins the correct elements from A_RDD and B_RDD?
<!-- #endregion -->

```python slideshow={"slide_type": "fragment"}
AB=None
# Your solution here
AB
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**Now what?** Can we just reduce by key perform multiplication and then do a summation?
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "fragment"} -->
... This is a leading question. If you try this, we'll be heading down an incorrect path. Instead, is there something we can map the keys to a key that helps us out? My real question to you is:
**Pick a line at random, and what should be the key?**
<pre>
(1, ([3, 1, 0], [1, 2, 8]))
</pre>
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Your solution here**

Once you do it manually, then do it using Spark...
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
AB=None
AB
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**We can now put it all together and solve the problem!**
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
def exercise_2(A_RDD,B_RDD):
    result = None
    # Your solution here
    return result

result = exercise_2(A_RDD,B_RDD)
result
```

```python slideshow={"slide_type": "subslide"}
result_mat = np.zeros((4,3))
for row_col,val in result:
    row,col = row_col
    result_mat[row-1,col-1] = val
result_mat
```

```python slideshow={"slide_type": "subslide"}
R == result_mat
```

<!-- #region slideshow={"slide_type": "subslide"} -->
This second format is very useful and common when representing sparse matrices. In this format, we remove/exclude any entry where a value in the matrix is 0. This occurs twice in A and twice in B. This results in a significant memory savings when a lot of the entries are in fact 0.
<!-- #endregion -->

<!-- #region slideshow={"slide_type": "subslide"} -->
**Exercise 3:** Implement matrix multiplication using the following alternative format that assumes missing rows have a value of 0 (i.e., sparse matrices):

'row number', 'column number', 'value'

For this exercise, you cannot use loops or np.dot. It should be Spark centric using join, map, add, reduceByKey, and/or collect. 
<!-- #endregion -->

```python slideshow={"slide_type": "subslide"}
A = [[1,1,1],
#     [1,2,0],
     [2,1,3],
     [2,2,4],
#     [3,1,0],
     [3,2,6],
     [4,1,7],
     [4,2,8]
    ]
A_RDD = sc.parallelize(A)

B = [[1,1,7],
     [1,2,8],
     [1,3,9],
#     [2,1,0],
     [2,2,11],
#     [2,3,0]
    ]
B_RDD = sc.parallelize(B)
```

```python slideshow={"slide_type": "subslide"}
def exercise_3(A_RDD,B_RDD):
    result = None
    # Your solution here
    return result

result = exercise_3(A_RDD,B_RDD)
result
```

```python slideshow={"slide_type": "subslide"}
result_mat = np.zeros((4,3))
for row_col,val in result:
    row,col = row_col
    result_mat[row-1,col-1] = val
result_mat
```

<!-- #region slideshow={"slide_type": "subslide"} -->
**We didn't have to change a thing!**
<!-- #endregion -->
