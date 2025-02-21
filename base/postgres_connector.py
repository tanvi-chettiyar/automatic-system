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


class PostgresConnection():
    def __init__(self, database: str, user: str, password: str, host: str, port: str):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
    
    def __enter__(self):
        self.connect = pg.connect(database = self.database, user = self.user, password = self.password, host = self.host, port = self.port)
        self.connect.autocommit = True
        return self.connect, self.connect.cursor()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.close()

    def open_connection(self):
        self.connection = pg.connect(database= self.database, user= self.user, password= self.password, host= self.host, port= self.port)
        self.cursor = self.connection.cursor()
        return self.connection, self.cursor
    
    def close_connection(self):
        self.cursor.close()
        self.connection()


## created a class names PostgresConnector where class is like an object constructor, or a "blueprint" for creating objects.

	## __init__(val1,val2,val3,etc.) is used to assign values to object properties, or other operations that are necessary ...
    ## to do when the object is called

        ## self.val1 = val1, etc. assign values to object properties so that if the object is called all values ...
        ## are pre-assigned

	## this special function is used when the WITH statement is entered. It's responsible for setting up the context, ...
    ## acquiring resources, and returning the object that the with statement will use.

        ## using object assigned values, create a connection to database
    
    ## this special function is used automatically when the with block is exited, regardless of whether an exception ...
    ## occurred within the block. It takes three arguments: exc_type: (the type of the exception raised, if any), ...
    ## exc_value: (the exception instance, if any) and traceback: (the traceback object, if any.)

        ## using object assigned values, close the connection to database

    ## created a function to make a connection to a database using object assigned values and used a return statement ...
    ## to output the connection and cursor object

    ## created a function to close a connection to a database using object assigned values 
