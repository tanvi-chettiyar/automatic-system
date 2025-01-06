import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from typing import Dict

class SQLCommands():
    command_dict: Dict[str, str] = {"create_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql", 
                                    "copy_from_file": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql", 
                                    "external_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql",
                                    "load_data": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"}
    


    def __init__(self, command):
        self.command = command
    
    def get_sql(self, command: str) -> str:
        """
        This method checks which sql operation to be performed. 
        """

        if command == 'create_table':
            sql_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql" 
        elif command == 'copy_from_file':
            sql_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql"
        elif command == 'external_table':
            sql_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql"
        elif command == "load_data":
            sql_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"
        else:
            raise ValueError("Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")
        return sql_file

    def _raise_error(self) -> None:
        """
        This method checks whether the command inputted is one of the list of commands accounted for in the previous method.
        """
        # Single Underscore in Function name means the method is to be used internally within the class and shouldn't be called outside.
        # It's called a private method. Python won't enforce anything but developers will understand the convention
        
        raise ValueError("Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")
    
    def get_sql_with_dict(self, command: str) -> None:
        """
        This method also checks which sql command to be performed with the use of a dictionary. 
        """
        if self.command_dict.get(command):
            return self.command_dict.get(command, self._raise_error())
        else:
            raise ValueError("1. Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")