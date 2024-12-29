from typing import Optional
from list_class import ListDatatype
from string_class import StringDatatype

class TupleDatatype:
    def __init__(self, tuple1: Optional[tuple] = None , tuple2: Optional[tuple]= None):
        self.tuple1 = tuple1
        self.tuple2 = tuple2
    
    def merge_tuple_0(self):
        print("merge tuple no arguments")
        if self.tuple1 is None:
            self.tuple1 = []
        if self.tuple2 is None:
            self.tuple2 = []
        return {*self.tuple1, *self.tuple2}

    def merge_tuple_1(self,tuple2 = None):
        print("merge 1 tuple argument")
        tuple2 = tuple2 if tuple2 else self.tuple2
        return self.tuple1 + tuple2
    
    def merge_tuple_2(self, tuple1 = None, tuple2 = None):
        print("merge 2 tuple arguemnts")
        tuple1 = tuple1 if tuple1 else self.tuple1
        tuple2 = tuple2 if tuple2 else self.tuple2
        return tuple1 + tuple2
    
    def check_tuple(self,search_str: str, tuple1 = None):
        print("check string in tuple")
        if search_str.lower() in tuple1:
            return True
        else:
            return False