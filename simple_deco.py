from datetime import datetime

def simple_deco(func):
    def wrapper(*args, **kwargs):
        dttm = datetime.now()
        print(dttm)
        result = func(*args, **kwargs)
        print(datetime.now())
        return result
    return wrapper

@simple_deco
def print_hello(value):
    res = f"Hello - {value}"
    print("function executed\n", res)
    return res

print_hello('hi')


print()
@simple_deco
def print_name(value):
    res = f"Name - {value}"
    print("function executed\n", res)
    return res

print(print_name("Arun"))

# def func_aaa(val):
#     print('value is ' + val)


# aaa = func_aaa

# aaa('aaa')
