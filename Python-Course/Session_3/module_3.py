# import time
from typing import List
import time
Matrix = List[List[int]]


def task_1(exp):
    def power(x):
        return x ** exp
    return power
    
    
def task_2(*args, **kwags):
    for value in args:
        print(value)
    for value in kwags.values():
        print(value)
task_2(1, 2, 3, moment=4, cap="arkadiy")


def helper(func):
    def wrapper(*args, **kwargs):
        print("Hi, friend! What's your name?")
        result = func(*args, **kwargs)
        print ("See you soon!")
        return result
    return wrapper

@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")


def timer(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        run_time = end - start
        print(f"Finished {func.__name__} in {run_time:.4f} secs")
        return result
    return wrapper

@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])


def task_5(matrix: Matrix) -> Matrix:
    return list(map(list, zip(*matrix)))


def task_6(queue: str):
    count = 0 
    for char in queue:
        if char == '(':
            count +=1
        elif char == ')':
            count -=1
        if count < 0:
            return False
    return count == 0

print(task_6("((()))"))
            
