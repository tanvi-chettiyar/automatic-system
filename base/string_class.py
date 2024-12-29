from typing import Optional
from os.path import basename

class StringDatatype:
    def __init__(self, var1: str, var2: str):
        self.var1 = var1
        self.var2 = var2

    def create_str(self):
        print(f"{self.__class__} \t {basename(__name__)}")
        return "".join(self.var1)
    
    def join_str_0(self):
        return self.var1 + self.var2
    
    def join_str_1(self, var2: Optional[str] = None):
        var2 = var2 if var2 else self.var2
        return self.var1 + var2
    
    def join_str_2(self, var1: Optional[str] = None, var2: Optional[str] = None):
        var1 = var1 if var1 else self.var1
        var2 = var2 if var2 else self.var2
        return var1 + var2