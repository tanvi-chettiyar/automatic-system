import psycopg2 as pg
import pandas as pd

conn = pg.connect(database="postgres", user="tanvi_rajkumar", password="", host="localhost", port="5432")

print(conn.status)

cur = conn.cursor()

cur.execute("select * from public.sample")
print(cur.fetchall())

df = pd.read_sql('select * from public.sample', con=conn)
print(df.head())

cur.close()
conn.close()

