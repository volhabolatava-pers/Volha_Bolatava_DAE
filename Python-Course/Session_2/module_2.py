# from collections import defaultdict as dd
# from itertools import product
from typing import Any, Dict, List, Tuple
from itertools import product


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    new_dict=dict()

    for k in data_1:
        if k in data_2:
            new_dict[k]= data_1[k]+data_2[k]
        else:
             new_dict[k]= data_1[k]

    for k in data_2:
        if k in data_1:
            continue 
        else: 
            new_dict[k] = data_2[k]
    
    return new_dict


def task_2():
    a =[]
    for i in range(1,16):
        a.append(i)
    result= {x: x**2 for x in a}
    return result



def task_3(data: Dict[Any, List[str]]):
    lists = list(data.values())
    combinations = [''.join(p) for p in product(*lists)]

    return combinations



def task_4(data: Dict[str, int]):
    sorted_keys = sorted(data,key = lambda k: data[k],reverse=True)
    return sorted_keys[:3]


def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    result = {}

    for k, v in data:
        if k in result:
            result[k].append(v)
        else:
            result[k]=[v]
        
    return result

def task_6(data: List[Any]):
    pass


def task_7(words: [List[str]]) -> str:
    pass


def task_8(haystack: str, needle: str) -> int:
    pass
