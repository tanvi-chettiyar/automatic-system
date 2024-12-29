from list_class import ListDatatype

def list_function(): 
    list_obj_1 = ListDatatype()
    list_obj_2 = ListDatatype(["a", "b", "c"])
    list_obj_3 = ListDatatype(["b"], ["c"])
    print(list_obj_1.merge_list_0())
    print(list_obj_2.merge_list_1())
    print(list_obj_3.merge_list_2())
    print(list_obj_3.check_list("A",["a","b","B"]))
    print(list_obj_3.create_str_from_list({"1","a","4", "AAA"}))
    print(list_obj_2.create_str_inheritence())


if __name__ == "__main__":
    list_function()
    # var1= None
    # if not var1:
    #     print("none")
    # else: 
    #     print("not none")
else:
    print(__name__)