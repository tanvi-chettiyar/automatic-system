from pydantic import BaseModel, field_validator, computed_field
from datetime import datetime


class Student(BaseModel):
    name: str
    dob: str

    @field_validator('dob')
    @classmethod
    def validate_dob(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("dob must be in YYYY-MM-DD format")
        return v

    @computed_field
    @property
    def age(self) -> int:
        birth_date = datetime.strptime(self.dob, "%Y-%m-%d")
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

    def __str__(self):
        return f"{self.name}, {self.dob}, {self.age}"


obj1 = Student(name="Arun", dob="1990-09-02")
print(obj1)
print(obj1.model_dump())
#  model_dump is a class method of BaseModel

# def aaa() -> tuple[int]:
#     return 1, 2

# print(type(aaa()))
