import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from base.postgres_connector import PostgresConnector
import psycopg2

file_path = '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt'

# db_object = PostgresConnector(database='postgres', user= 'tanvi_rajkumar', password= '', host= 'localhost', port= '5432')
# db_conn, db_cur = db_object.getConnection()
# db_cur.execute('create table if not exists public.pythonstudent(id varchar, name text, subject varchar, grade smallint);')

# query = f"""
# COPY public.pythonstudent 
# FROM '{file_path}'
# DELIMITER ','
# CSV HEADER;
# """

# truncate_sql = "TRUNCATE TABLE public.pythonstudent;"

# db_cur.execute(truncate_sql)

# query2 = '''COPY public.pythonstudent 
#     FROM stdin WITH CSV HEADER DELIMITER as ',';
#     '''
# # print(query)
# # db_cur.copy_expert(query)

# with open(file_path, 'r') as f:
#     db_cur.copy_expert(sql=query2, file=f)

#     # Commit the transaction
#     db_conn.commit()


# query3 = """CREATE FOREIGN table if not exists public.ext_student (id INTEGER, name TEXT, subject text, grade INTEGER)
# SERVER file_server
# OPTIONS (filename '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt', format 'csv', delimiter ',', header 'true');
# """

# db_cur.execute(query3)

# query4 = """truncate table public.pythonstudent;
# insert into public.pythonstudent
# select * from public.ext_student; """

# db_cur.execute(query4)

# db_cur.execute('select * from public.ext_student')
# print(db_cur.fetchall())
# db_cur.close()
# db_conn.close()


## without using copy expert

with PostgresConnector(database='postgres', user= 'tanvi_rajkumar', password= '', host= 'localhost', port= '5432') as (db_conn2, db_cur2):

    db_cur2.execute(f"create table if not exists public.pythonstudent2 (id varchar, name text, subject varchar, grade smallint);")
    db_cur2.execute(f'truncate table public.pythonstudent2;')

    with open(file_path, 'r') as inputfile:
        cols = inputfile.readline()
        if cols.find('\n') > -1:
            cols = cols.replace('\n', '')
        data = inputfile.readlines()
        for x in data:
            if x.find("\n") > -1:
                x = x.replace('\n', '')
            
            query5 = f"""Insert into public.pythonstudent2 ({cols}) values ('{x.replace(",", "','")}');"""
            db_cur2.execute(query5)
            db_conn2.commit()

        db_cur2.execute('select * from public.pythonstudent2')
        print(db_cur2.fetchall())

