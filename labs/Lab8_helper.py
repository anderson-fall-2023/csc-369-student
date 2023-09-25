from bson.objectid import ObjectId

from bson.code import Code

from bson.objectid import ObjectId

def exercise_1(col,record_id):
    result = None
    # Your solution here
    return result

def exercise_2(col,state):
    result = None
    # Your solution here
    return result

def exercise_3(col,d):
    result = None
    # partial solution
    # airline_delay2.groupBy(?).pivot(?).agg(avg(?))
    # Your solution here
    return result

def exercise_4(col):
    result = None
    # Your solution here
    return result

def exercise_5(col):
    result = None
    # Your solution here
    return result

def process_exercise_6(result):
    process_result = {}
    for record in result:
        process_result[record['_id']['state']] = record['sum']/record['count']
    return process_result

def exercise_6(col,date1,date2):
    result = None
    # partial solution
    # Your solution here
    return result

def process_exercise_7(result):
    process_result = {}
    for record in result:
        state,identifier = record['_id'].split(": ")
        value = record['value']
        if state not in process_result:
            process_result[state] = 1.
        if identifier == "sum":
            process_result[state] *= value
        elif identifier == "count":
            process_result[state] *= 1/value
    return process_result

def exercise_7(col,date1,date2):
    result = None
    # partial solution
    #map_func = Code(
    #    """Insert Javascript code here"""
    #)
    #reduce_func = Code(
    #    """Insert Javascript code here"""
    #)   
    #result = col.map_reduce(map_func, reduce_func, "myresults")  
    # Your solution here
    return result


