CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER file_server FOREIGN DATA WRAPPER file_fdw;

CREATE FOREIGN table if not exists public.ext_student (id INTEGER, name TEXT, subject text, grade INTEGER)
SERVER file_server
OPTIONS (filename '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt', format 'csv', delimiter ',', header 'true');