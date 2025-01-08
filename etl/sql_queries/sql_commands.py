import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from typing import Dict


command_dict: Dict[str, str] = {"create_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql", 
                                    "copy_from_file": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql", 
                                    "external_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql",
                                    "load_data": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"}
    

def get_sql( command: str) -> str:
    """
    This method checks which sql operation to be performed. 
    """

    if command == 'create_table':
        sql_filepath = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql" 
    elif command == 'copy_from_file':
        sql_filepath = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql"
    elif command == 'external_table':
        sql_filepath = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql"
    elif command == "load_data":
        sql_filepath = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"
    else:
        raise ValueError("Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")
    return sql_filepath

def _raise_error() -> None:
    """
    This method checks whether the command inputted is one of the list of commands accounted for in the previous method.
    """
    # Single Underscore in Function name means the method is to be used internally within the class and shouldn't be called outside.
    # It's called a private method. Python won't enforce anything but developers will understand the convention
    
    # Double Underscore in Function name triggers name mangling in Python to make accidental or intentional overriding in subclasses less likely 
    # by changing the name internally to include the class name as prefix (it's still possible to access this for the determined but is considered protected method)
    
    raise ValueError("2. Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")

def get_sql_with_dict(command: str) -> str:
    """
    This method also checks which sql command to be performed with the use of a dictionary. 
    """
    if command_dict.get(command):
        return command_dict.get(command)
    else:
        return _raise_error()
    