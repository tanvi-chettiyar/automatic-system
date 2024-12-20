#Object Oriented Programming

##Classes -  a blueprint for an instance of a class
class Employee:
    
    #any variable outside the init method is a class variable which stays constant for any instance
    num_of_emps = 0
    raise_amount = 1.04
    
    #instance is automatically the first argument is conventionally labeled self
    def __init__(self, first, last, pay):
        #self.____ is a instance variable
        self.first = first
        self.last = last
        self.pay = pay
        self.email = first + '.' + last + "@company.com"

        #Can use either class variable (Employee.varname) or instance variable (self.varname) but for this variable ...
        # we won't need the flexibility to change the variable for an instance so class variable makes more sense
        Employee.num_of_emps += 1
        
    #syntax for a regular method (function associated with a class) which always takes in the instance argument as input
    def fullname(self):
        return "{} {}".format(self.first, self.last)
    
    def apply_raise(self):
        #we can change variable for an instance with self.varname but if we use Employee.varname then we can't
        #both methods work but have different flexibilities
        self.pay = int(self.pay * self.raise_amount)
    
    #syntax for class method which always takes the class argument as first input and conventionally labeled cls
    @classmethod
    def set_raise_amount(cls, amount):
        cls.raise_amount = amount
    
    #class methods as alternative constructors using the convention of naming the method with from
    @classmethod
    def from_string(cls, emp_str):
        first, last, pay = emp_str.split('-')
        return cls(first,last,pay)

    #syntax for static methods which doesn't take class or instance arguments (cls or self)
    #regular functions that are added to class because they are related but are independent of the class
    @staticmethod
    def is_workday(day):
        if day.weekday() == 5 or day.weekday() == 6:
            return False
        return True    


#self input is not needed when class is called because the output is passed as self
emp_1 = Employee("John", "Smith", 50000)
emp_2 = Employee("Jane", "Smith", 60000)

#fullname is a method so empty parentheses are necessary
emp1fullname = emp_1.fullname()
#another option to use an method using class instead in which case the instance is required as input
emp2fullname = Employee.fullname(emp_2)

#we can change the class variable outside the class function and we can change the class variable for a single instance...
#without changing the class variable as a whole by creating an attribute for the instance
Employee.raise_amount = 1.05
emp_1.raise_amount = 1.05

#class method to change class variables demonstrated in previous section but this method cannot change only instance ...
#variable and not the class variable like shown in previous section
Employee.set_raise_amount(1.05)

#using class methods as alternative constructors 
emp_str_1 = 'John-Doe-70000'
emp_str_2 = 'Jane-Doe-40000'

new_emp_1 = Employee.from_string(emp_str_1)
new_emp_2 = Employee.from_string(emp_str_2)

#using static method
import datetime
my_date = datetime.date(2024, 4, 8)
WorkDay = Employee.is_workday(my_date)



##SubClasses 
# inheritance is a mechanism that allows you to create a subclass that inherits attributes and methods from an ... 
# existing parent class 
# we can make changes to Parent class without directly changing the Parent class

class Developer(Employee):
    raise_amount1 = 1.10

    def __init__ (self, first, last, pay, prog_lang):
        #super function is used to give access to methods and attributes of parent class
        super().__init__(first,last,pay)
        #Employee.__init__(self,first,last,pay)
        # is also an option but it's easier to use the super method espcecially when having multiple inheritances
        self.prog_lang = prog_lang

class Manager(Employee):

    def __init__(self, first, last, pay, employees=None):
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


#Use help(Developer) or help(Manager) to see the hierarchy of the inheritance of the class

dev_1 = Developer("Steve", "Martins", 60000, "Python")
dev_2 = Developer("Sarah", "Collins", 50000, "Java")

mgr_1 = Manager("Sue", "Grant", 90000, [dev_1])
mgr_1.add_emp(dev_2)
mgr_1.remove_emp(dev_1)
mgr_1.print_emp()


isinstance(mgr_1,Manager)               #returns True 
isinstance(mgr_1,Developer)             #returns False

issubclass(Manager,Employee)            #returns True
issubclass(Manager,Developer)           #returns False