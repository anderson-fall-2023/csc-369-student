from operator import add
import numpy as np

A1 = [('A[1,:]',[1, 2, 3]),('A[2,:]',[4, 5,6])]
B1 = [('B[:,1]',[7,9,11]),('B[:,2]',[8,10,12])]

A2 = [[1,1,1],
     [1,2,0],
     [2,1,3],
     [2,2,4],
     [3,1,0],
     [3,2,6],
     [4,1,7],
     [4,2,8]
    ]

B2 = [[1,1,7],
     [1,2,8],
     [1,3,9],
     [2,1,0],
     [2,2,11],
     [2,3,0]
    ]

A3 = [[1,1,1],
#     [1,2,0],
     [2,1,3],
     [2,2,4],
#     [3,1,0],
     [3,2,6],
     [4,1,7],
     [4,2,8]
    ]

B3 = [[1,1,7],
     [1,2,8],
     [1,3,9],
#     [2,1,0],
     [2,2,11],
#     [2,3,0]
    ]

# These functions needed because we used a string above (don't worry, later I don't do this).
def get_row(s):
    return int(s.split(",")[0].split("[")[-1])

def get_col(s):
    return int(s.split(",")[1].split("]")[0])

def exercise_1(A_RDD,B_RDD):
    result = None
    A2 = A_RDD.map(lambda kv: (get_row(kv[0]),kv[1]))
    B2 = B_RDD.map(lambda kv: (get_col(kv[0]),kv[1]))
    

    # Your solution here
    return result

def exercise_2(A_RDD,B_RDD):
    result = None
    # Your solution here
    return result

def exercise_3(A_RDD,B_RDD):
    result = None
    # Your solution here
    return result
