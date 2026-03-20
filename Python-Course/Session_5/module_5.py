# from collections import Counter
import os
from pathlib import Path
# from random import choice
from random import seed
from typing import List, Union
import random
import re
from collections import Counter
import requests
from requests.exceptions import RequestException

# import requests
# from requests.exceptions import ConnectionError
# from gensim.utils import simple_preprocess


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"


def task_1():

    random.seed(1)
    
    with open(PATH_TO_NAMES, 'r',encoding="utf-8") as f:
        names = [line.strip().lower() for line in f if line.strip()]

    with open(PATH_TO_SURNAMES, 'r',encoding="utf-8") as f:
        surnames = [line.strip().lower() for line in f if line.strip()]

    names.sort()

    full_names = [f"{name} {random.choice(surnames)}" for name in names]

    with open (PATH_TO_OUTPUT, "w",encoding="utf-8") as f:
        f.write("\n".join(full_names) + "\n")



def task_2(top_k: int):
    
    with open(PATH_TO_TEXT, 'r',encoding="utf-8") as f:
        text = f.read().lower()

    with open(PATH_TO_STOP_WORDS, 'r',encoding="utf-8") as f:
        stop_words ={ line.strip().lower() for line in f if line.strip()}

    words = re.findall(r'\b[a-z]+\b', text)

    filtered_words = [word for word in words if word not in stop_words]

    word_counts = Counter(filtered_words)

    return word_counts.most_common(top_k)
    


def task_3(url: str):
   
    try:

        response = requests.get(url)
        response.raise_for_status()
        return response
     
    except RequestException as e:
        raise RequestException(str(e))


def task_4(data: List[Union[int, str, float]]):

    result = 0

    for i in data:
        try:
            result += i
        except TypeError as e:
            result += float(i)
    return result



def task_5():

    try:

        var1_str, var2_str = input().split()
        var1 = float(var1_str)
        var2 = float(var2_str)

        if var2 == 0:
            print("Can't divide by zero")
        else:
            print(var1 / var2)

    except ValueError: 
        print("Entered value is wrong")
        

     


