from typing import Any

class StudentDetails:
    def __init__(self, name: str, age: int, grade: int, phone: str, email: str = None):
        self.name = name
        self.age = age
        self.grade = grade
        self.phone = phone
        self.email = self.set_email(email)
    
    def set_email(self, email = None) -> str:
        if not email:
            email = self.name.lower() + "@gmail.com"
        return email
    
    def set_grade(self):
        grade_map: dict[Any, str] = {
            1: "PK",
            2: "PK",
            3: "PK",
            5: "K",
            29: "too old",
            "default": "no age provided"
        }
        return grade_map.get(self.age, grade_map["default"])
    
    def __str__(self):
        return f"Student name, age and email: {self.name}, {self.age}, {self.email}"
    
    def __repr__(self):
        return f"Student name, age and email: {self.name}, {self.age}, {self.email}"

student1 = StudentDetails("Arun", 5, 0, "7036408352", "AAA")
student2 = StudentDetails("Tanvi", 30, 12, "8135175719")
print(student2.set_grade())