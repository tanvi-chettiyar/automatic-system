import psycopg2 as pg
import pandas as pd
from postgres_connector import PostgresConnector


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
    postgresconnect()
    pgobject = PostgresConnector(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")
    print(f'object {pgobject.database} {pgobject.user} {pgobject.password} {pgobject.host} {pgobject.port}')
    pgconn, pgcur = pgobject.getConnection()
    pgcur.execute("select * from public.sample")
    print(pgcur.fetchall())
    #pgobject.closeConnection()

    pgobject2 = PostgresConnector(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")
    pgconn2, pgcur2 = pgobject2.getConnection()
    pgcur2.execute("select * from public.sample where id = 1")
    print(pgcur2.fetchall())

