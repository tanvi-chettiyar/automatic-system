from dataclasses import dataclass

@dataclass
class Student:
    name: str
    dob: str

    @property
    def age(self):
        from datetime import datetime
        birth_date = datetime.strptime(self.dob, "%Y-%m-%d")
        today = datetime.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    
    def __str__(self):
        return f"{self.name}, {self.dob}, {self.age}"
    
obj1 = Student(name = "Arun", dob = "1990-09-02")
print(obj1)



def aaa() -> tuple[int]:
    return 1, 2

print(type(aaa()))