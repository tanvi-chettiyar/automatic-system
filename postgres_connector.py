import psycopg2 as pg

class PostgresConnector:
    def __init__(self, database, user, password, host, port):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        print(f'inside class {self.database} {self.user} {self.password} {self.host} {self.port}')
        print("class initialized")

    def getConnection(self):
        self.conn = pg.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        print("executed git connection and cursor")
        self.cur = self.conn.cursor()
        return self.conn, self.cur
    
    def closeConnection(self):
        print("executed close connection")
        self.conn.close()
        self.cur.close()