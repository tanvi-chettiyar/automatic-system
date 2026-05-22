from pydantic import BaseModel
from typing import Optional, Dict, Any


class Sale(BaseModel):
    # def __init__(self, name: str, amount: float):
    #     self.name = name
    #     self.amount = amount
    #     self.email = None
    name: str
    amount: float
    email: Optional[str] = None

    # def print_obj(self):
    #     print(self.name, self.amount, self.email)

class Department(BaseModel):
    # def __init__(self, name: str, type: str):
    #     self.name = name
    #     self.type = type
    name: str
    type: str

    
def get_email(obj: Sale):
    email = obj.name + "@email.com"
    print(email)
    obj.email = email


def main():
    dict1: Dict[str, Any] = {"name": "aaa", "amount": 84.50}

    obj1 = Sale(**dict1)
    # obj1.print_obj()

    # get_email(obj1)
    print(obj1.name, obj1.amount, obj1.email)
    print(type(obj1))

if __name__ == "__main__":
    main()