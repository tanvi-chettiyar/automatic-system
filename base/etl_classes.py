import os, sys
from typing import Dict


sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from postgres_connector import PostgresConnector

class UsingCopyExpert(PostgresConnector):
    command_dict: Dict[str, str] = {"create_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql", 
                                    "copy_from_file": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql", 
                                    "external_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql",
                                    "load_data": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"}
    
    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        super(UsingCopyExpert, self).__init__(database, user, password, host, port)
        
    

    def _get_sql(self, command: str) -> str:
        """
        This function checks which sql operation to be performed. 
        Single Underscore in Function name means the method is to be used internally within the class and shouldn't be called outside.
        It's called a private method. Python won't enforce anything but developers will understand the convention
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
        raise ValueError("Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")
    
    def _get_sql_with_dict(self, command: str) -> None:
        if self.command_dict.get(command):
            return self.command_dict.get(command, self._raise_error())
        else:
            raise ValueError("1. Invalid value passed. Acceptable values: create_table, copy_from_table, external_table, and load_data")

    
    def execute(self, command: str, filepath: str) -> None:
        db_conn, db_cur = PostgresConnector.getConnection(self)
        # print(self._get_sql_with_dict(command.lower())) #TRC
        sql_file = self._get_sql(command.lower())
        print(sql_file)

        if not os.path.exists(sql_file):
            raise FileNotFoundError('File not found')
        
        with open(sql_file, 'r') as sql:
            sql_query = sql.read()
            # db_cur.execute(sql_query)
            # db_conn.commit()
            #auto commit not working. #TRC

        with open(filepath, 'r') as data_file:
            db_cur.copy_expert(sql = sql_query, file = data_file) #pass the file object not the file.read()
            db_conn.commit()


postgres_parameters: Dict[str, str] = {"database": 'postgres', 
                       "user":'tanvi_rajkumar', 
                       "password": '', 
                       "host": 'localhost', 
                       "port": "5432",
                       "filepath": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt"}
# UsingCopyExpert(**postgres_parameters).execute("Create_table")