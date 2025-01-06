from postgres_connector import PostgresConnector
import pandas
import psycopg2

file_path = '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt'
# input_data = pandas.read_csv(file_path, sep= None, header=1)

db_object = PostgresConnector(database='postgres', user= 'tanvi_rajkumar', password= '', host= 'localhost', port= '5432')
db_conn, db_cur = db_object.getConnection()
db_cur.execute('create table if not exists public.pythonstudent(id varchar, name text, subject varchar, grade smallint);')

query = f"""
COPY public.pythonstudent 
FROM '{file_path}'
DELIMITER ','
CSV HEADER;
"""

truncate_sql = "TRUNCATE TABLE public.pythonstudent;"

db_cur.execute(truncate_sql)

query2 = '''COPY public.pythonstudent 
    FROM stdin WITH CSV HEADER DELIMITER as ',';
    '''
# print(query)
# db_cur.copy_expert(query)

with open(file_path, 'r') as f:
    db_cur.copy_expert(sql=query2, file=f)

    # Commit the transaction
    db_conn.commit()


query3 = """CREATE FOREIGN table if not exists public.ext_student (id INTEGER, name TEXT, subject text, grade INTEGER)
SERVER file_server
OPTIONS (filename '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', format 'csv', delimiter ',', header 'true');
"""

db_cur.execute(query3)

query4 = """truncate table public.pythonstudent;
insert into public.pythonstudent
select * from public.ext_student; """

db_cur.execute(query4)




# for index, rows in input_data.iterrows():
#     print(rows)
#     query = f"insert into public.pythonstudent values ({rows})"
#     db_cur.execute(query)

db_cur.execute('select * from public.ext_student')
print(db_cur.fetchall())
db_cur.close()
db_conn.close()
