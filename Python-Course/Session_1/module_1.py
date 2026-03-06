from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    result= set()
    for num in array:
        dif=target-num
        if dif in result:
            return[dif,num]
        result.add(num)
    return[]

def task_2(number: int) -> int:
    reversed=0
    negative = number < 0

    number = abs(number)

    while number > 0:
        digit = number % 10
        reversed = reversed * 10 + digit
        number = number // 10

    if negative:
        reversed=-reversed
    return reversed

def task_3(array: List[int]) -> int:
    for i in range(len(array)):
        index = abs(array[i])-1
        if array[index]<0:
            return abs(array[i])
        array[index]=-array[index]
    return -1

def task_4(string: str) -> int:
    """
    Write your code below
    """
    pass


def task_5(array: List[int]) -> int:
    """
    Write your code below
    """
    pass
