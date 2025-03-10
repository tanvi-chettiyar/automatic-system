from datetime import date
from typing import List

@staticmethod
def print_current_date():
    #print(dir(date))
    pass

current_year: int = 2024
##Classes -  a blueprint for an instance of a class
class Employee:
    
    #any variable outside the init method is a class variable which stays constant for any instance
    num_of_emps: int = 0
    raise_amount: float = 1.04
    
    #instance is automatically the first argument is conventionally labeled self
    def __init__(self, first: str, last: str, pay: int):
        #self.____ is a instance variable
        self.first = first
        self.last = last
        self.pay = pay

        #Can use either class variable (Employee.varname) or instance variable (self.varname) but for this variable ...
        # we won't need the flexibility to change the variable for an instance so class variable makes more sense
        Employee.num_of_emps += 1
        print(Employee.raise_amount, self.pay, current_year)
        print_current_date()
        
    #syntax for a regular method (function associated with a class) which always takes in the instance argument as input
    def fullname(self):
        return "{} {}".format(self.first, self.last)
    
    def apply_raise(self):
        #we can change variable for an instance with self.varname but if we use Employee.varname then we can't
        #both methods work but have different flexibilities
        self.pay = int(self.pay * self.raise_amount)
    
    #syntax for class method which always takes the class argument as first input and conventionally labeled cls
    @classmethod
    def set_raise_amount(cls, amount: float):
        cls.raise_amount = amount
    
    #class methods as alternative constructors using the convention of naming the method with from
    @classmethod
    def from_string(cls, emp_str: str):
        first, last, pay = emp_str.split('-')
        return cls(first,last,pay)

    #syntax for static methods which doesn't take class or instance arguments (cls or self)
    #regular functions that are added to class because they are related but are independent of the class
    @staticmethod
    def is_workday(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True    


    ## Magic/Dunder Methods
    def __repr__(self):
        return "Employee('{}', '{}', {})".format(self.first, self.last, self.pay)
    # used to be unambiguous to help developers debug scripts
    
    def __str__(self):
        return '{} - {}'.format(self.fullname, self.email)
    # used for readability 
    
    # def __add__(self, other):
    #     return self.pay + other.pay
    
    # def __len__(self):
    #     return len(self.fullname())
    
    print("Employee")
    

    ## decorator
    @property
    def email(self):
        return '{}.{}@company.com'.format(self.first, self.last)
    
    @property
    def fullname_property(self):
        return "{} {}". format(self.first, self.last)
    
    @fullname_property.setter
    def fullname(self, name: str):
        first, last = name.split(" ")   #you can split the name with a different splitter like - or _
        self.first = first
        self.last = last
    
    @fullname_property.deleter
    def fullname(self):
        self.first = None
        self.last = None
    

    
##SubClasses 
# inheritance is a mechanism that allows you to create a subclass that inherits attributes and methods from an ... 
# existing parent class 
# we can make changes to Parent class without directly changing the Parent class

class Developer(Employee):
    raise_amount1: float = 1.10

    def __init__ (self, first: str, last: str, pay: int, prog_lang: str):
        #super function is used to give access to methods and attributes of parent class
        super().__init__(first,last,pay)
        #Employee.__init__(self,first,last,pay)
        # is also an option but it's easier to use the super method espcecially when having multiple inheritances
        self.prog_lang = prog_lang
    
    print("Developer")

class Manager(Employee):

    def __init__(self, first: str, last: str, pay: int, employees: List =None):
        super().__init__(first, last, pay)
        if employees is None:
            self.employees = []

        else:
            self.employees = employees
    
    def add_emp(self, emp):
        if emp not in self.employees:
            self.employees.append(emp)
    
    def remove_emp(self, emp):
        if emp in self.employees:
            self.employees.remove(emp)
    
    def print_emp(self):
        for emp in self.employees:
            print(">>", emp.fullname())

    print("Manager")


class Mistake:
    print("Mistake")

if __name__ == "__main__":
    print("***")
    print(__name__)
    print("***")
    print(__file__)
    print_current_date()

else:
    print("***")
    print(__name__)
    print("***")

