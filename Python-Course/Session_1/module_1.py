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
    roman_dict = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    total = 0
    length = len(string)
    for i in range(length):
        current_value = roman_dict[string[i]]
        if i < length - 1 and current_value < roman_dict[string[i + 1]]:
            total -= current_value
        else:
            total += current_value

    return total


def task_5(array: List[int]) -> int:
    smallest = float('inf')
    for num in array:
        if num<smallest:
            smallest =num
    return smallest 

