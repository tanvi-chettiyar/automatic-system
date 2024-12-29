from typing import List, Optional
from string_class import StringDatatype

class ListDatatype(StringDatatype):
    """
    This class is for List Datatypes
    Parameters:
    :param list1 Optional List, Default = None
    :param list2 Optional List, Default = None
    """

    def __init__(self,list1: List, list2: List):
        self.list1 = list1 
        self.list2 = list2 
        # super().__init__(list1)     #always calls the first parent inheritence class in this case is StringDatatype
        #StringDatatype.__init__(list1) to call a parent class explicitly by name if there are multiple parent classes
    
    def merge_list_0(self):
        print("merge list no arguments")
        if self.list1 is None:
            self.list1 = []
        if self.list2 is None:
            self.list2 = []
        return [*self.list1, *self.list2]

    def merge_list_1(self,list2 = None):
        print("merge list 1 arguments")
        if list2:
            list2 = list2
        else:
            list2 = self.list2
        return self.list1 + list2
   
    def merge_list_2(self, list1 = None, list2 = None):
        print("merge list 2 arguments")
        # print(list1, list2)
        # print(self.list1, self.list2)
        list1 = list1 if list1 else self.list1
        list2 = list2 if list2 else self.list2
        # print(list1, list2)
        list1.extend(list2)
        return list1
    
    def check_list(self,search_str: str, list1 = None):
        print("check in list")
        if search_str.lower() in list1:
            return True
        else:
            return False
    
    def join_list(self, lista: List):
        print("join a list")
        join_list = ''
        for x in lista:
            if isinstance(x, str):
                join_list = join_list + x
            else:
                join_list = join_list + str(x)
        return join_list
    
    def create_str_from_list(self, lista: List[str]):
        print("create a string")
        return "".join(lista)

    def create_str_inheritence(self) -> str:
        """
        This Function is inherited from String Datatypes
        Parameters are initialized with the parent class
        Parameters:
        None
        """
        print("create list from inheritence")
        return self.create_str()    
