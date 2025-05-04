
def load_dict(example_dict:dict[str,str])->None:
    for n,(key,value) in enumerate(example_dict.items()):
        print(n, key,value)


sample_dict = {"fname":"Arnab",
               "lname":"Dutta",
               "language":"Python"}

load_dict(sample_dict)

def load_list(sample_list:list[str])->None:
    for n, items in enumerate(sample_list):
        print(n, items)


sample_list=["Arnab","Dutta","Python"]

load_list(sample_list)

def check_return_even_items(list_items:list[int])->list[int]:
    """check for even items in a list by its index and return the even items"""
    empty_list=[]
    for n,val in enumerate(list_items, start=1): # index will start from 1 instead of 0
        if n%2==0:
            empty_list.append(val)
    return empty_list

list_items = [1,2,3,4,5,6,7]
print(check_return_even_items(list_items))