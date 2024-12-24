def list_datatype():
    list1 = ["a", "b", "c"]
    print(list1, type(list1))
    list2 = [1,2,3]
    print(list2)
    print(list1+list2)
    list1.extend(list2)
    print(list1)
    list1.pop(2)
    for x in list1:
        print(x)
    print(len(list1))
    print(list1[0])
    print(list1[-1])

    for x in list1:
        if isinstance(x, int):
            print(x*10)
        else:
            pass
    
    print(x*10 if isinstance(x,int) else 0)
    print([x*10 for x in list1])

def tuple_datatype():
    tuple1 = (1,2,3)
    tuple2 = ("a","b","c")

    for x in tuple1:
        print(x)
    
    for i in range(len(tuple1)):
        print(i, tuple1[i])
        i += 1

    print(tuple2.index("a"))
    print(tuple1[2])
    y = tuple1+tuple2
    print(type(y))

    print(tuple1, tuple2)

def dictionary_datatype():
    dict1 = {"first":"Mary","last":"Smith","pay":100000}
    dict2 = {"first":"Sam","last":"West","pay":90000}
    # print(type(dict1))
    # print(dict1, dict2)
    # dict1.update(dict2)
    # print(dict1)
    ndict = [dict1,dict2]
    # for x in ndict:
    #     print(type(x))
    print(type(ndict))
    for x,y in dict1.items():
        print(x,y)

def json_datatype():
    import datetime
    import json
    with open("data.json", "r") as data:
        # print(data.read())
        x = json.load(data)
        print(x)
        print(datetime.date.today())
        # print(json.dumps(x,indent=2))
    
    # open_file = open("data.json","r")
    # print(open_file.read())
    # open_file.close()
    

def str_datatype():
    s = "aaa.bbb.ccc"
    x,y,z = s.split('.')
    print(x,y,z)

def return_value():
    return "a", "b"


if __name__ == "__main__":
    #list_datatype()
    #tuple_datatype()
    # dictionary_datatype()
    # list1 = ["a", "b", "c"]
    # list2 = [1,2,3]
    # print([*list1,*list2])
    # _,b = return_value()
    # print(b)
    json_datatype()
    import datetime
    import json
    jjj = {"first":"Mary",
           "last":"Smith",
           "pay":100000,
           "dt": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    print(f"\nDict: {jjj}\n")
    print(f"\nJson: {json.dumps(jjj)}\n")



    print("\n")
    print("Val")
    print(jjj)


    aaa = "Arun"
    print("val {}, {}, {}".format(aaa, aaa, aaa))
    print(f"\n Val {aaa}\t vals={aaa}\n")

    lll = ["a","r","u","n"]
    l = "".join(lll)
    print(l)

    ttt = ("a","r","u","n")
    t = "".join(ttt)
    print(t)

    sss = {"a","r","u","n"}
    s = "".join(sss)
    print(s)
