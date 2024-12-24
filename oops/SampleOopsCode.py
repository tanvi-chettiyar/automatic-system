#Object Oriented Programming
import sys , os
sys.path.insert(0,os.path.abspath(__file__))

from oops.oops_example import (
    Employee,
    Developer,
    Manager
)


def main():

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

    #using magic/dunder methods
    # both methods work, the second command is automatically rewriting the command into the first command
    print(emp_1.__repr__())
    #print(repr(emp.1))
    print(emp_1.__str__())
    #print(str(emp.1))

    #print uses __add__ method to add; if __add__ wasn't coded into the class this print command will return an error
    print(emp_1 + emp_2)

    #like before, if the method is not coded into the class, this command will return an error
    len_emp_1 = len(emp_1)




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


if __name__ == "__main__":
    #main()
    print(__name__)
    # print(__file__)
    # print(os.path.basename(__file__))
    # print(os.path.dirname(__file__))
    
    emp_3 = Employee("Mary", "Smith", 100000)
    print(emp_3)
    from datetime import date
    print (date.today())
    