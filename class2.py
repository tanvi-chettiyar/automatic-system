from class_data import Sale, Department
from typing import TypeVar, Type
from pydantic import BaseModel

## Look into TypeVar, Type and TypeAlias

PydanticClass = TypeVar("PydanticClass", bound=BaseModel)


def dynamic_class(cls_arg: Type[PydanticClass], *args, **kwargs):
    obj1 = cls_arg(**kwargs)
    print(type(obj1), obj1)

dynamic_class(Sale, name = "aaa", amount = 85.40, email = "aaa@email.com")
dynamic_class(Department, name = "aaa", type = "IT")