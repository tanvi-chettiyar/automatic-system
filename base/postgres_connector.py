import psycopg2 as pg

class PostgresConnector:
    def __init__(self, database: str, user: str, password: str, host: str, port:str):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        print(f'inside class {self.database} {self.user} {self.password} {self.host} {self.port}')
        print("class initialized")

    def __enter__(self):
        self.conn = pg.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        self.conn.autocommit = True
        print("Automatically Create Connection and Cursor")
        return self.conn, self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Automatically Close Connection and Cursor")
        self.conn.close()

    def getConnection(self):
        self.conn = pg.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        print("executed get connection and cursor")
        self.cur = self.conn.cursor()
        return self.conn, self.cur
    
    def closeConnection(self):
        print("executed close connection")
        self.conn.close()
        self.cur.close()


# class PostgresConnection():
#     def __init__(self, database: str, user: str, password: str, host: str, port: str):
#         self.database = database
#         self.user = user
#         self.password = password
#         self.host = host
#         self.port = port
    
#     def __enter__(self):
#         self.connect = pg.connect(database= self.database, user= self.user, password= self.password, host= self.host, port= self.port):
#         return self.connect, self.connect.cursor()
    
#     def __exit__(self):
#         self.connect.close()

#     def open_connection(self):
#         self.connection = pg.connect(database= self.database, user= self.user, password= self.password, host= self.host, port= self.port)
#         self.cursor = self.connection.cursor()
#         return self.connection, self.cursor
    
#     def close_connection(self):
#         self.cursor.close()
#         self.connection()