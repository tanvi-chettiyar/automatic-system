import os, sys
from typing import Dict


sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base.postgres_connector import PostgresConnector
from base.sql_commands_class import SQLCommands

class UsingCopyExpert(PostgresConnector, SQLCommands):
    command_dict: Dict[str, str] = {"create_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/create_table.sql", 
                                    "copy_from_file": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/copy_to.sql", 
                                    "external_table": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/external_table.sql",
                                    "load_data": "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/sql_queries/load_data.sql"}
    
    def __init__(self, database: str, user: str, password: str, host: str, port: str, command: str):
        super(UsingCopyExpert, self).__init__(database, user, password, host, port)
        super(PostgresConnector, self).__init__(command)

        #super(class to avoid, self).__init__(inputs to be initialized) 
        
        #*** HOWEVER, try to avoid multiple inheritences ***
        # if not possible the following is a cleaner way to make sure all parent classes are initialized
        
        #PostgresConnector.__init__(database, user, password, host, port)
        #SQLCommands.__init__(commands)

    
    def execute(self, command: str, filepath: str) -> None:
        db_conn, db_cur = PostgresConnector.getConnection(self)
        # print(SQLCommands.get_sql_with_dict(command.lower())) #TRC
        sql_file = SQLCommands.get_sql(command.lower())
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