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



