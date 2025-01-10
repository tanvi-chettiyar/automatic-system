import subprocess
from typing import List

class UsingSubprocess:
    def __init__(self, database: str, user: str, filepath: str, tablenames: List[str]):
        self.database = database
        self.user = user
        self.filepath = filepath
        self.tablenames = tablenames

    def execute(self):
        subprocess.run(f'psql -d {self.database} -U {self.user}')
        subprocess.run(f"""create foreign table if not exists {self.tablenames[0]} (id INTEGER, name TEXT, subject TEXT, grade INTEGER)
                       server file_server 
                       options(filename '{self.filepath}', format 'csv', delimiter ',', header 'true');""")
        subprocess.run('commit')
        subprocess.run('echo $?')
        subprocess.run(f"insert into {self.tablenames[1]} select * from {self.tablenames[0]}")
        subprocess.run('commit')
        subprocess.run('echo $?')
        subprocess.run(f"drop foreign table if exists {self.tablenames[0]}")
        subprocess.run("\q")