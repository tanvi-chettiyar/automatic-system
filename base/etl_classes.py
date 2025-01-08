## ETL means Extract, Transform and Load

import os, sys
from typing import Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base.postgres_connector import PostgresConnector
from etl.sql_queries import sql_commands

class UsingCopyExpert(PostgresConnector):
    def __init__(self, database, user, password, host, port):
        super().__init__(database, user, password, host, port)
        
        # *** Try to avoid multiple inheritences. If not avoidable, use the following syntax ***

        #super(ClasstoAvoid, self).__init__(inputs_to_be_initialized)
        #super(UsingCopyExpert, self).__init__(database, user, password, host, port) 

        # *** Another way to make sure all parent classes are initialized: ***

        #PostgresConnector.__init__(database, user, password, host, port)
        #(SecondInheritence).__init__(other_inputs)

    
    def execute(self, command: str, filepath: str) -> None:
        db_conn, db_cur = PostgresConnector.getConnection(self)
        # sql_file1 = sql_commands.get_sql_with_dict(command.lower()) #TRC
        sql_file2 = sql_commands.get_sql(command.lower())
        print(sql_file2)

        if not os.path.exists(sql_file2):
            raise FileNotFoundError('File not found')
        
        with open(sql_file2, 'r') as sql:
            sql_query = sql.read()
            db_cur.execute(sql_query)
            db_conn.commit()
            #auto commit not working. #TRC

        with open(filepath, 'r') as data_file:
            db_cur.copy_expert(sql = sql_query, file = data_file) #pass the file object not the file.read()
            db_conn.commit()
        
        db_cur.close()
        db_conn.close()


postgres_parameters: Dict[str, str] = {"database": 'postgres', 
                       "user":'tanvi_rajkumar', 
                       "password": '', 
                       "host": 'localhost', 
                       "port": "5432"}
data_filepath =  "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt"

# UsingCopyExpert(**postgres_parameters).execute("Create_table")

class UsingNativePython(PostgresConnector):
    def __init__(self, database, user, password, host, port):
        super().__init__(database, user, password, host, port)
    
    def execute(self, command: str, data_filepath: str, table_name: str) -> None:
        with PostgresConnector(self) as (db_conn, db_cur):
            sql_filepath = sql_commands.get_sql_with_dict(command.lower())

            if not os.path.exists(sql_filepath):
                raise FileNotFoundError(f'File not found in {os.path.dirname(sql_filepath)}')
            
            with open(sql_filepath, 'r') as sql_script:
                sql_query = sql_script.readlines()
                db_cur.execute(sql_query)

            with open(data_filepath, 'r') as input_data:
                cols = input_data.read()
                if cols.find("\n") > -1:
                    cols = cols.replace('\n', '')
                data = input_data.readlines()
                for x in data:
                    if x.find('\n') > -1:
                        x = x.replace('\n', '')
                    query = f"""Insert into {table_name} ({cols}) values ('{x.replace(",", "','")}');"""
                    db_cur.execute(query)
                    db_conn.commit()



