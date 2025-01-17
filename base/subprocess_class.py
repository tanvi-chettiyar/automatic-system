from subprocess import run
from typing import List

class UsingSubprocess:
    def __init__(self, database: str, user: str, filepath: str, tablenames: List[str]):
        self.database = database
        self.user = user
        self.filepath = filepath
        self.tablenames = tablenames

    def execute(self):
        run(f'psql -d {self.database} -U {self.user}')
        run(f"""create foreign table if not exists {self.tablenames[0]} (id INTEGER, name TEXT, subject TEXT, grade INTEGER)
                       server file_server 
                       options(filename '{self.filepath}', format 'csv', delimiter ',', header 'true');""")
        run('commit')
        run('echo $?')
        run(f"insert into {self.tablenames[1]} select * from {self.tablenames[0]}")
        run('commit')
        run('echo $?')
        run(f"drop foreign table if exists {self.tablenames[0]}")
        run("\q")