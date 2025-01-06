truncate table public.pythonstudent;
insert into public.pythonstudent
select * from public.ext_student;