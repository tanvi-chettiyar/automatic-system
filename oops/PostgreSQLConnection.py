import psycopg2 as pg
import pandas as pd
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base.postgres_connector import PostgresConnector


def postgresconnect():
    conn = pg.connect(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")

    print(type(conn))

    cur = conn.cursor()
    print(type(cur))

    cur.execute("select * from public.sample")
    print(cur.fetchall())

    df = pd.read_sql('select * from public.sample', con=conn)
    print(df.head())

    cur.close()
    conn.close()

if __name__ == "__main__":
    #functional programming
    postgresconnect()
    
    #oops 
    pgobject = PostgresConnector(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")
    print(f'object {pgobject.database} {pgobject.user} {pgobject.password} {pgobject.host} {pgobject.port}')
    pgconn, pgcur = pgobject.getConnection()
    pgcur.execute("select * from public.sample")
    print(pgcur.fetchall())
    pgobject.closeConnection()

    ## second open connection with oops
    # pgobject2 = PostgresConnector(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")
    # pgconn2, pgcur2 = pgobject2.getConnection()
    # pgcur2.execute("select * from public.sample where id = 1")
    # print(pgcur2.fetchall())

    #with clause
    with PostgresConnector(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432") as curr:
        curr.execute("select * from public.sample")
        print(curr.fetchall())

    print("exit")


## import psycopg2 driver using pip install psycopg2 in terminal (if it doesn't work, try pip install psycopg2-binary)
## it is used to connect python to a Postgre SQL database
## import pandas library using method mentioned above

### Functional Programming

##     connection-name = psycopg2.connect(database="name", user="username", password= "[optional password]", ...
##     host="host name", port="port number")
##          the database information can be looked up in DBeaver in edit connection option
##          type function returns the class of the object inputted

##     cursor-name = connection-name.cursor()
##          Cursor class of the psycopg library provide methods to execute the PostgreSQL commands in the database ...
##          using python code which can be created using the cursor command

##     cursor-name.execute("PostgreSQL command")
##          executes Postgre SQL commands in python

##     cursor-name.fetchall() or cursor-name.fetchone() or cursor-name.fetchmany()
##          The fetchall() method retrieves all the rows in the result set of a query and returns them as list of tuples. ...
##          (If we execute this after retrieving few rows, it returns the remaining ones).
##          The fetchone() method fetches the next row in the result of a query and returns it as a tuple.
##          The fetchmany() method is similar to the fetchone() but, it retrieves the next set of rows in the result ...
##          set of a query, instead of a single row.

##     dataframe = pandas.read_sql("PostgreSQL command", con= connection-name)
##          read database data into a python dataframe

##     dataframe.head([integer])
##          prints the first (integer) rows of data, if no integer is given then 5 is the default number

##     cursor-name.close
##          closes cursor object, a must after all queries are done
		
##     connection_name.close
##          closes connection to database, a must after all queries are done otherwise the connection remains open


### OOPS 

## used a class to create a connection to Postgre SQL database

## used a f string to embed object assigned values which are enclosed in {} into a string 

## caught the two returned values from the function

## executed a SQL command in python

## retrieved all rows as a result of a query into a list of tuples and printed them

## used the function to close the connection to the database
