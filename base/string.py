from os.path import basename

class StringDatatype:
    def __init__(self, var1):
        self.var1 = var1

    def create_str(self):
        print(f"{self.__class__} \t {basename(__name__)}")
        return "".join(self.var1)