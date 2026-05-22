# # def func(val1, val2, val3, **args, **kwargs):
# #     print(val1, val2, val3)


# # func("aaa", "bbb", "ccc")
# # func(val3 = "aaa", val2 = "bbb", val1 = "ccc")

# list1 = [1, 2, 3]
# print(*list1)
# print(list1)

# dict1 = {"a1": "aaa", "a2": "bbb"}
# print(dict1)
# #print(str(**dict1)) -- this doesn't work because its trying to print(a1 = "aaa", ...) 
# # which is not proper syntax print function

# dict2 = {"b1": "ccc", "b2": "ddd", **dict1}
# print(dict2)

# dict3 = {**dict1, **dict2}
# print(dict3)

# str1 = "aaa = {a1}"
# print(str1.format(**dict3))
# print(str1.format(a1 = "bbb"))
# print(f"""a1 = "bbb" """)

# tp1 = (1, 2, 3, 2, 1)
# print(type(tp1), tp1)

# st1 = {1, 2, 3, 2, 1}
# print(type(st1), st1)

# ls1 = [1, 2, 3, 2, 1]
# print(type(ls1), ls1)

# dc1 = {"a": 1, "a": 2}
# print(type(dc1), dc1)

def func1(var2, var1 = None, *args, num, num3 = 4, **kwargs):
    print(type(var1), var1)
    print(type(var2), var2)
    print(type(args), args)
    print(type(num), num)
    print(type(num3), num3)
    print(type(kwargs), kwargs)

func1(1, 2, "c", num = 6, num2 = 7)
print()
func1(2, num = 8)

list1 = [1, 2, 3, 5]
dict1 = {"a": "aaa", "b": "BBB", "aaa": "1"}

def func(var1, *args, aaa = None, **kwargs):
    print("printing args")
    print(args)
    count1 = 0
    for item in args:
        count1 += 1
        print(type(item), item, count1)

    print("printing kwargs")
    print(kwargs)
    count2 = 0
    for key, val in kwargs.items():
        count2 += 1
        print(key, val, count2)

# func(list1, dict1)
# func(*list1, **dict1)

print(*list1)
# * is both a packing and unpacking operator. 
# Most common use in packing functions inputs into tuples
start, *mid, last = [1, 2, 3, 4, 5]
print(start)
print(mid)
print(last)