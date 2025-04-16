CREATE TABLE public.student (
	id int NULL,
	name text NULL
);

GRANT pg_read_server TO USER postgres


insert into public.student values (1, 'Tanvi') ;
insert into public.student values (2, 'Arun') ;

truncate  public.student;

copy public.pythonstudent1 from '/home/tanvi/airflow/dags/airflow-dags/data/student.csv' delimiter ',' csv header

truncate public.student_new;

insert into public.student_new select * from  public.student;


select * from public.student_new ;


CREATE TABLE public.student_new (
	id int NULL,
	name text NULL
);



select * from public.foreign_student_table;


CREATE EXTENSION IF NOT EXISTS file_fdw; 
CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;

DROP FOREIGN TABLE IF EXISTS public.foreign_student_table ;

CREATE FOREIGN TABLE IF NOT EXISTS public.foreign_student_table (id varchar, name text, subject varchar, grade smallint)
SERVER file_server
OPTIONS (filename '/home/tanvi/airflow/dags/airflow-dags/data/dataset.csv', format 'csv', delimiter ',', header 'true');

--CREATE FOREIGN TABLE IF NOT EXISTS public.foreign_student_table (id varchar, name text, subject varchar, grade smallint)
--SERVER file_server
--OPTIONS (filename '/temp/dataset.csv', format 'csv', delimiter ',', header 'true');


select count(*) from public.pythonstudent2 p  ;

select count(*) from public.pythonstudent1 p  ;



select * 
into public.my_table_3
from public.my_table_2
where false;


select * from public.my_table_3 ;

select user ;

)
commit;




select *, md5(concat("name",subject,grade)) rec_hash 
from public.pythonstudent2 p  ;

select * from public.student where "name" = 'Arun';

alter table public.pythonstudent2 add column rec_hash text;




CREATE OR REPLACE FUNCTION update_record_hash_locations()
RETURNS TRIGGER AS $$
BEGIN
    -- For INSERT and UPDATE, set the update_time to current timestamp
	NEW.rec_hash := md5(concat("name",subject,grade))
    
    -- For DELETE, also set the update_time to current timestamp (optional)
    IF (TG_OP = 'DELETE') THEN
		OLD.rec_hash := md5(concat("name",subject,grade))
        RETURN OLD; -- Return OLD for DELETE operation
    END IF;

    RETURN NEW;  -- Return NEW for INSERT and UPDATE operations
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_record_hash_current()
RETURNS TRIGGER AS $$
BEGIN
    -- For INSERT and UPDATE, set the update_time to current timestamp
	NEW.rec_hash := md5(concat("name",subject,grade))
    
    -- For DELETE, also set the update_time to current timestamp (optional)
    IF (TG_OP = 'DELETE') THEN
		OLD.rec_hash := md5(concat("name",subject,grade))
        RETURN OLD; -- Return OLD for DELETE operation
    END IF;

    RETURN NEW;  -- Return NEW for INSERT and UPDATE operations
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_update_record_hash
BEFORE INSERT OR UPDATE OR DELETE ON prep.hourly
FOR EACH ROW
EXECUTE FUNCTION update_record_hash();


--pl/sql

select table_schema,table_name, --array_agg(column_name)::text, 
concat('md5( concat( ', replace(replace(array_agg(column_name)::text, '{', ''), '}', ''), ' ) )')
from
(
	select distinct table_schema, table_name, column_name , ordinal_position 
	from information_schema."columns" 
	where table_schema in ('stage') and table_name in ('locations')
		and column_name not in ('id')
	order by ordinal_position 
) 
group by table_schema,table_name ;


select md5( concat( city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time ) )
from stage.location l;



select *  
from information_schema."columns" 
where table_schema in ('stage') and table_name in ('locations');



--where table_schema = 'prep' or table_schema = 'target' or table_schema = 'stage' ;



select 1000, cast(1000 as text), 1000::text;

select 'stage' like '%age%' pattern, 'stage' in ('stage') usingin, 'stage' = 'stage' using_equalto;

select 'stage' not like '%age%', 'stage' not in ('stage'), 'stage' != 'stage', 'stage' <> 'stage';

select null <=> 'aa';

--Equality Operators
--=
-->
--<
--<>
-->=
--<=
--!=
--<=> for hive
--
--isnull for postgres

select max(id) from public.my_table_3;

select * from stage.location;



SELECT * FROM stage.location 
WHERE local_time LIKE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '%';

SELECT pg_get_serial_sequence('stage.locations', 'id');

--alter sequence stage.location_id_seq restart with 4;

select md5(concat('aaa'));

-- CTE - Common table Expression 
with table1 as (
	select 1 id, 'Arun' name
	union all
	select 2, 'Tanvi'
	union all
	select 3, 'Kumar'
)
, table2 as (
	select 1 id, 'Arun' name
	union all
	select 3, 'Kumar'
	union all
	select 4, 'Raj'
)
--inner join
--select * from table1
--inner join table2 on ( table1.id = table2.id ); 
--
--select table1.id, table1.name, table2.name  
--from table1
--inner join table2 on ( table1.id = table2.id ); 
--
--equi join
--select * from table1 , table2 
--where table1.id = table2.id;
--
--subquery filter
--select * from table1 where id in ( select id from table2 ) ;
--
--correlated sub-query
--select * from table1 where exists ( select id from table2 where table1.id = table2.id ) ;
--
--
--select table1.id, table1.name, (select name from table2 where table2.id = table1.id ) as table2_name  
--from table1 ; 
--
--select * from table1
--union --all
--select * from table2 ;
--
--Minus / Except
--select * from table1
--except
--select * from table2 ;
--
--select * from table2
--except
--select * from table1
--
--select * from table1 where id not in (select id from table2 ) ;
--
--select * from table2 where id not in (select id from table1 ) ;
--
--select * from table1 where id not in (select id from table2 )
--union all
--select * from table2 where id not in (select id from table1 ) ;
--
--( select * from table1
--except
--select * from table2 )
--union all
--( select * from table2
--except
--select * from table1 );


---self join
with emp as (
	select 1  id, 'TR' name, 0 manager_id
	union all
	select 2, 'Arun',  1 
	union all
	select 3, 'Tanvi', 2
	union all
	select 4, 'Kumar', 2
)
select e1.* , e2.name
from emp e1
left join emp e2 on ( e1.manager_id = e2.id  );



begin transaction;

lock apublic.delete IN EXCLUSIVE mode ;

select * from apublic.delete ;

end transaction;




select * from pg_catalog.pg_locks pl ;

SELECT 
    pg_locks.pid, 
    relname AS locked_table,
    mode,
    granted,
    query,
    age(clock_timestamp(), query_start) AS query_duration,
    usename,
    application_name
FROM pg_locks 
JOIN pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid
JOIN pg_class ON pg_locks.relation = pg_class.oid
WHERE NOT pg_locks.fastpath;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA stage;

select * from stage.location where city_name = '"Piscataway"';

SELECT uuid_generate_v4();

CREATE EXTENSION IF NOT EXISTS "pgcrypto" schema target;

SELECT gen_random_uuid();



