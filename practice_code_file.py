# input_file = open('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', 'r')
# output_file = open('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_untitled.txt', 'w')
# for contents in input_file.readlines():
#     output_file.write(contents)
# input_file.close()
# output_file.close()

# import pandas

# pandas_dataframe = pandas.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', sep= ',')
# pandas_dataframe.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_df.txt', sep= '|', header= True, index = False, mode= 'w')

# import duckdb

# duckdb_dataframe = duckdb.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', sep= ',')
# duckdb_dataframe.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_duckDb.txt', sep= '|', header= True)

# import polars

# polars_dataframe = polars.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', separator= ',')
# polars_dataframe.write_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_polars.txt', separator= '|', include_header= True)

# import dask.dataframe as daskdf

# dask_df = daskdf.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt')
# dask_df.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_dask.txt', sep= '|', index= False, header= True)

# import pyarrow.csv as pycsv

# pyarrow_dataframe = pycsv.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt')
# pycsv.write_csv(pyarrow_dataframe, '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_pyarrow.txt', write_options = pycsv.WriteOptions(delimiter = '|'))

# import sys, os
# sys.path.append(os.path.abspath('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/base'))
# from base.list_class import ListDatatype
# from base.string_class import StringDatatype
# from base.dictionary_class import DictionaryDatatype
# from base.tuple_class import TupleDatatype


# def CheckDataType(var1, var2):
#     if type(var1) == type(var2):
#         if type(var1) == list:
#             list_vars = ListDatatype(var1, var2)
#             return list_vars.merge_list_0()
#         elif type(var1) == str:
#             str_vars = StringDatatype(var1, var2)
#             return str_vars.join_str_0()
#         elif type(var1) == tuple:
#             tuple_vars = TupleDatatype(var1, var2)
#             return tuple_vars.merge_tuple_2()
#         elif type(var1) == dict:
#             dict_vars = DictionaryDatatype(var1, var2)
#             return dict_vars.merge_dictionary()
#     else:
#         return "Variables are of different types. Please input variables of same type to join"
    

# if __name__ == "__main__":
#     var1 = {"a": "0", "b": "2"}
#     var2 = "MMM"
#     print(CheckDataType(var1, var2))

# from datetime import date

# @staticmethod
# def today_date():
#     today = date.today()

# class School:
#     def __init__(self, first, last, occupation):
#         self.first = first
#         self.last = last
#         self.occupation = occupation
    
#     @property
#     def email(self):
#         return f"{self.first}.{self.last}@school.edu"
    
#     @staticmethod
#     def is_school_day(today):
#         if today.weekday() == 5 or today.weekday() == 6:
#             return False
#         else:
#             return True
    
#     @classmethod
#     def from_str(cls, string: str):
#         first, last, occupation = string.split('-')
#         return cls(first, last, occupation)

#     @property
#     def fullname(self):
#         return '{} {}'.format(self.first, self.last)
    
#     @fullname.setter
#     def full_name(self, name):
#         last, first = name.split(", ")
#         self.first = first
#         self.last = last
    
#     @fullname.deleter
#     def full_name(self):
#         self.first = None
#         self.last = None
    
#     def __str__(self):
#         return f"{self.last}, {self.first} - ({self.occupation})\n{self.email}"
    

# class Student(School):
#     def __init__(self, first, last, occupation, year, grade):
#         super().__init__(first, last, occupation)
#         self.year = year
#         self.grade = grade
    
#     def __str__(self):
#         return f"{self.last}, {self.first} - ({self.occupation} in Grade {self.year})\n{self.email}"
    

# class Teacher(School):
#     raise_amount = 1.05

#     def __init__(self, first, last, occupation, pay):
#         super().__init__(first, last, occupation)
#         self.pay = pay

#     @classmethod
#     def set_raise_amount(cls, raise_amount):
#         cls.raise_amount = raise_amount

#     def apply_raise(self):
#         raise_pay = int(self.pay * self.raise_amount)
#         return raise_pay
    
#     def __str__(self):
#         return f"{self.last}, {self.first} - ({self.occupation})\n{self.email}"


# if __name__ == "__main__":

#     personA = School('Dean', 'Winchester', 'Student')
#     personB = School('Sam', 'Wesson', 'Student')
#     personC = School('Bobby', 'Michael', 'Teacher')
#     person1 = Student('Dean', 'Winchester', 'Student', '11', 'Fail')
#     person2 = Student('Sam', 'Wesson', 'Student', '9', 'Pass')
#     person3 = Teacher('Bobby', 'Michael', 'Teacher', 40000)

#     print(person3.apply_raise())

#     import argparse

#     parser = argparse.ArgumentParser()
#     parser.add_argument('UserInput', nargs= 1, type= str, help= 'a string with  the following format: first name-last name-occupation')
#     arguments = parser.parse_args()

#     person4 = School.from_str(arguments.UserInput[0])
#     print(person4.__str__())


# import psycopg2
# import pandas
# from postgres_connector import PostgresConnector
# import argparse

# def postgresconnection():
#     connection = psycopg2.connect(database='postgres', user='tanvi_rajkumar', password='', host='localhost', port='5432')
#     cur = connection.cursor()
#     cur.execute("select * from public.samlist")
#     print(cur.fetchall())

#     pdf = pandas.read_sql('select * from public.samlist', con= connection, index_col= None)
#     print(pdf.head())

#     cur.close()
#     connection.close()

# if __name__ == "__main__":
#     postgresconnection()

#     parser2 = argparse.ArgumentParser()
#     parser2.add_argument("DatabaseInfo", nargs=5, type= str, help= "5 strings with the following format: 'databaseName' 'user' 'password' 'host' 'port' ")
#     arguments2 = parser2.parse_args()
    

#     with PostgresConnector(database=arguments2.DatabaseInfo[0], user=arguments2.DatabaseInfo[1], password=arguments2.DatabaseInfo[2], host=arguments2.DatabaseInfo[3], port=arguments2.DatabaseInfo[4]) as conn:
#         conn.execute("select * from public.samlist")
#         print(conn.fetchall())

    




