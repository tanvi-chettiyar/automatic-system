class DictionaryDatatype:
    def __init__(self, dict1: dict, dict2: dict):
        self.dict1 = dict1
        self.dict2 = dict2
    
    def merge_dictionary(self):
        print("merge two dictionaries into a new dictionary")
        return {**self.dict1, **self.dict2}
    
    def update_dictionary(self, dict1: dict):
        print("update a dictionary with a new dictionary")
        self.dict1.update(dict1)
        return self.dict1
    
    